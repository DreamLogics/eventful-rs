//! Slint UI-thread adapter. Construct and bind on the UI thread; keep its event
//! loop running while work is pending. Request shutdown before quitting Slint.
use crate::{
    EventLoop, Eventful, HasEvents, ShardError, ShardId, ShardRc,
    engine::{self, ContextGuard},
};
use futures::{FutureExt, channel::oneshot, future::Shared};
use std::{future::Future, time::Duration};
/// Submission handle for this backend.
pub type SlintShardHandle = crate::ShardEventHandle;

/// Adapter driven by an application-owned Slint UI event loop.
pub struct SlintShard {
    /// Thread-safe admission handle for this backend.
    handle: SlintShardHandle,
    /// Identity of this shard.
    shard_id: ShardId,
    /// Constructing or worker thread identity used to enforce thread affinity.
    owner: std::thread::ThreadId,
    /// Cloneable completion notification from the Slint driver.
    done: Shared<oneshot::Receiver<()>>,
}
impl SlintShard {
    /// Return this backend's immutable shard identity.
    pub fn shard_id(&self) -> ShardId {
        self.shard_id
    }
    /// Schedule the driver on the current UI thread.
    ///
    /// # Panics
    /// Panics if the Slint event loop is unavailable.
    pub fn new() -> Self {
        let (handle, rx) = SlintShardHandle::channel();
        let (done_tx, done_rx) = oneshot::channel();
        let shard_id = handle.shard_id;
        let driver_handle = handle.clone();
        let owner = std::thread::current().id();
        // Slint owns polling; the same engine handles queues, wakeups, and panic isolation.
        ::slint::invoke_from_event_loop(move || {
            assert_eq!(
                std::thread::current().id(),
                owner,
                "create SlintShard on the UI thread"
            );
            ::slint::spawn_local(async move {
                {
                    let _ctx = ContextGuard::new(shard_id);
                    engine::drive(rx, driver_handle, Duration::from_secs(5), None).await;
                }
                let _ = done_tx.send(());
            })
            .expect("Slint local executor unavailable");
        })
        .expect("Slint event loop unavailable");
        Self {
            handle,
            shard_id,
            owner,
            done: done_rx.shared(),
        }
    }
    /// Drain the shard before quitting Slint; the UI loop must remain active.
    ///
    /// # Errors
    /// Returns [`ShardError::JoinError`] if the driver ends without signaling completion.
    pub async fn shutdown_async(&self) -> Result<(), ShardError> {
        self.request_shutdown();
        self.done
            .clone()
            .await
            .map_err(|_| ShardError::JoinError("Slint driver ended unexpectedly".into(), None))
    }
    /// Reject new work and enqueue shutdown after previously accepted jobs.
    pub fn request_shutdown(&self) {
        self.handle.request_shutdown();
    }
    /// Submit a construction factory immediately; await its result without blocking.
    ///
    /// # Errors
    /// Returns an error for mismatched affinity, shutdown, cancellation, or a factory panic.
    pub fn bind_async<F, R, T>(
        &self,
        f: F,
    ) -> impl Future<Output = Result<R, crate::InvokeError>> + Send + 'static + use<F, R, T>
    where
        F: FnOnce(&dyn Fn(T) -> ShardRc<T>) -> R + Send + 'static,
        R: Send + 'static,
        T: Eventful + HasEvents<T::EventSetType> + 'static,
    {
        self.handle.bind_async(f)
    }
}
impl Default for SlintShard {
    fn default() -> Self {
        Self::new()
    }
}
impl EventLoop for SlintShard {
    type HandleType = SlintShardHandle;
    fn handle(&self) -> Self::HandleType {
        self.handle.clone()
    }
    fn bind<F, R, T>(&self, f: F) -> R
    where
        F: FnOnce(&dyn Fn(T) -> ShardRc<T>) -> R + Send + 'static,
        R: Send + 'static,
        T: Eventful + HasEvents<T::EventSetType> + 'static,
    {
        self.handle.bind(self.owner, f)
    }
    fn join(&self) -> Result<(), ShardError> {
        Ok(())
    }
}
impl Drop for SlintShard {
    fn drop(&mut self) {
        self.request_shutdown();
    }
}

impl std::fmt::Debug for SlintShard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SlintShard")
            .field("shard_id", &self.shard_id)
            .finish_non_exhaustive()
    }
}
