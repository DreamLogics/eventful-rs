//! Dedicated-thread backend startup and coordinated shutdown.
use crate::{
    ShardError,
    engine::{self, ContextGuard, ShardEventHandle},
};
use std::{
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

/// Select the executor used to poll the common shard driver.
#[derive(Clone, Copy)]
pub(crate) enum Runtime {
    /// Use the executor-independent futures executor.
    Standard,
    /// Use a current-thread Tokio runtime with I/O and time enabled.
    #[cfg(feature = "tokio")]
    Tokio,
}
/// Serialize concurrent joins and cache the worker exit outcome.
struct JoinState {
    /// Join handle consumed exactly once by the first joining caller.
    thread: Option<thread::JoinHandle<()>>,
    /// Cached thread outcome returned to repeated joiners.
    result: Option<Result<(), String>>,
}
/// Background state retained by the backend and global registry.
struct Shared {
    /// Thread-safe admission handle for this backend.
    handle: ShardEventHandle,
    /// Constructing or worker thread identity used to enforce thread affinity.
    owner: thread::ThreadId,
    /// Protects the one-time thread join and reusable result.
    join: Mutex<JoinState>,
}
impl Shared {
    /// Reject self-joins, request shutdown, and observe the cached worker outcome.
    fn join(&self) -> Result<(), ShardError> {
        if thread::current().id() == self.owner {
            return Err(ShardError::JoinError(
                "cannot join own shard thread".into(),
                None,
            ));
        }
        self.handle.request_shutdown();
        let mut state = self.join.lock().unwrap();
        if state.result.is_none() {
            let result = state
                .thread
                .take()
                .expect("join handle missing")
                .join()
                .map_err(|_| "shard thread panicked".to_owned());
            state.result = Some(result);
        }
        state
            .result
            .clone()
            .unwrap()
            .map_err(|e| ShardError::JoinError(e, None))
    }
}
/// Own a dedicated thread while exposing the common submission handle.
pub(crate) struct Background {
    /// State shared with registry callbacks and asynchronous joiners.
    shared: Arc<Shared>,
}
impl Background {
    /// Start a worker and register its join callback only after successful initialization.
    pub(crate) fn new(name: &str, runtime: Runtime, grace: Duration) -> std::io::Result<Self> {
        let (handle, rx) = ShardEventHandle::channel();
        let worker_handle = handle.clone();
        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
        let worker = thread::Builder::new().name(name.into()).spawn(move || {
            #[cfg(feature = "tokio")]
            let rt = match runtime {
                Runtime::Standard => None,
                Runtime::Tokio => match ::tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(rt) => Some(rt),
                    Err(e) => {
                        let _ = ready_tx.send(Err(e));
                        return;
                    }
                },
            };
            let _ctx = ContextGuard::new(worker_handle.shard_id);
            struct Close(ShardEventHandle);
            impl Drop for Close {
                fn drop(&mut self) {
                    self.0.request_shutdown();
                }
            }
            let _close = Close(worker_handle.clone());
            if ready_tx.send(Ok::<_, std::io::Error>(())).is_err() {
                return;
            }
            let future = engine::drive(rx, worker_handle, grace, None);
            match runtime {
                Runtime::Standard => futures::executor::block_on(future),
                #[cfg(feature = "tokio")]
                Runtime::Tokio => {
                    let local = ::tokio::task::LocalSet::new();
                    rt.as_ref().unwrap().block_on(local.run_until(future));
                    drop(local);
                }
            }
            // Drop the runtime before the context guard (including detached tasks).
            #[cfg(feature = "tokio")]
            drop(rt);
        })?;
        let owner = worker.thread().id();
        match ready_rx.recv() {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                let _ = worker.join();
                return Err(e);
            }
            Err(_) => {
                let _ = worker.join();
                return Err(std::io::Error::other("shard startup panicked"));
            }
        }
        let shared = Arc::new(Shared {
            handle,
            owner,
            join: Mutex::new(JoinState {
                thread: Some(worker),
                result: None,
            }),
        });
        let registered = shared.clone();
        crate::register_shard(shared.handle.shard_id, Box::new(move || registered.join()));
        Ok(Self { shared })
    }
    /// Clone the backend admission handle.
    pub(crate) fn handle(&self) -> ShardEventHandle {
        self.shared.handle.clone()
    }
    /// Read the worker identity for same-thread checks.
    pub(crate) fn owner(&self) -> thread::ThreadId {
        self.shared.owner
    }
    /// Reject self-joins, request shutdown, and observe the cached worker outcome.
    pub(crate) fn join(&self) -> Result<(), ShardError> {
        #[cfg(feature = "tokio")]
        if ::tokio::runtime::Handle::try_current().is_ok() {
            return Err(ShardError::JoinError(
                "use join_async from Tokio".into(),
                None,
            ));
        }
        self.shared.join()
    }
    /// Perform the blocking join on Tokio blocking capacity, rejecting self-joins.
    #[cfg(feature = "tokio")]
    pub(crate) async fn join_async(&self) -> Result<(), ShardError> {
        if thread::current().id() == self.owner() {
            return Err(ShardError::JoinError(
                "cannot join own shard thread".into(),
                None,
            ));
        }
        let shared = self.shared.clone();
        ::tokio::task::spawn_blocking(move || shared.join())
            .await
            .map_err(|e| ShardError::JoinError(e.to_string(), None))?
    }
}
impl Drop for Background {
    fn drop(&mut self) {
        self.shared.handle.request_shutdown();
    }
}
