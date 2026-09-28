//! Dedicated-thread shard using Tokio.
use crate::{EventLoop, Eventful, HasEvents, ShardError, ShardId, ShardRc};
use std::future::Future;
/// Submission handle for this backend.
pub type TokioShardHandle = crate::ShardEventHandle;

/// Dedicated OS thread with a Tokio runtime for timers and I/O.
pub struct TokioShard {
    /// Shared implementation of background startup and joining.
    inner: crate::background::Background,
    /// Identity of this shard.
    shard_id: ShardId,
}
impl TokioShard {
    /// Return this backend's immutable shard identity.
    pub fn shard_id(&self) -> ShardId {
        self.shard_id
    }
    /// Start a dedicated Tokio thread with a five-second shutdown grace period.
    ///
    /// # Panics
    /// Panics if startup fails; use [`Self::try_new`] to handle failure.
    pub fn new(name: &str) -> Self {
        Self::try_new(name, std::time::Duration::from_secs(5)).expect("shard startup failed")
    }
    /// Start a background thread with a custom grace period for draining pending futures.
    ///
    /// # Errors
    /// Returns an I/O error if the worker thread or its runtime cannot start.
    pub fn try_new(name: &str, grace: std::time::Duration) -> std::io::Result<Self> {
        let inner =
            crate::background::Background::new(name, crate::background::Runtime::Tokio, grace)?;
        let shard_id = inner.handle().shard_id;
        Ok(Self { inner, shard_id })
    }
    /// Stop and join from Tokio without blocking a worker; cannot join the current shard.
    ///
    /// # Errors
    /// Returns [`ShardError`] for a self-join or failed worker shutdown.
    ///
    /// # Panics
    /// Panics outside a Tokio runtime.
    #[cfg(feature = "tokio")]
    pub async fn join_async(&self) -> Result<(), ShardError> {
        self.inner.join_async().await
    }
    /// Reject new work and enqueue shutdown after previously accepted jobs.
    pub fn request_shutdown(&self) {
        self.handle().request_shutdown();
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
        self.handle().bind_async(f)
    }
}
impl EventLoop for TokioShard {
    type HandleType = TokioShardHandle;
    fn handle(&self) -> Self::HandleType {
        self.inner.handle()
    }
    fn bind<F, R, T>(&self, f: F) -> R
    where
        F: FnOnce(&dyn Fn(T) -> ShardRc<T>) -> R + Send + 'static,
        R: Send + 'static,
        T: Eventful + HasEvents<T::EventSetType> + 'static,
    {
        self.handle().bind(self.inner.owner(), f)
    }
    fn join(&self) -> Result<(), ShardError> {
        self.inner.join()
    }
}

impl std::fmt::Debug for TokioShard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TokioShard")
            .field("shard_id", &self.shard_id)
            .finish_non_exhaustive()
    }
}
