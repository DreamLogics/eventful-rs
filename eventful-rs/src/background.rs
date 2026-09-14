use crate::{
    ShardError,
    engine::{self, ContextGuard, ShardEventHandle},
};
use std::{
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

#[derive(Clone, Copy)]
pub(crate) enum Runtime {
    Standard,
    #[cfg(feature = "tokio")]
    Tokio,
}
struct JoinState {
    thread: Option<thread::JoinHandle<()>>,
    result: Option<Result<(), String>>,
}
struct Shared {
    handle: ShardEventHandle,
    owner: thread::ThreadId,
    join: Mutex<JoinState>,
}
impl Shared {
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
pub(crate) struct Background {
    shared: Arc<Shared>,
}
impl Background {
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
    pub(crate) fn handle(&self) -> ShardEventHandle {
        self.shared.handle.clone()
    }
    pub(crate) fn owner(&self) -> thread::ThreadId {
        self.shared.owner
    }
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
