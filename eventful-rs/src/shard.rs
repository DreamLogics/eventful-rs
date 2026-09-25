//! Dedicated-thread shard using executor-independent Rust futures.
use crate::{EventLoop, Eventful, HasEvents, ShardError, ShardId, ShardRc};
use std::future::Future;
/// Submission handle for this backend.
pub type ShardEventHandle = crate::ShardEventHandle;

/// Dedicated OS thread running executor-independent futures.
pub struct Shard {
    /// Shared implementation of background startup and joining.
    inner: crate::background::Background,
    /// Identity of this shard.
    pub shard_id: ShardId,
}
impl Shard {
    /// Start a dedicated thread with a five-second shutdown grace period.
    /// Panics if the worker cannot start; use [`Self::try_new`] to handle failure.
    pub fn new(name: &str) -> Self {
        Self::try_new(name, std::time::Duration::from_secs(5)).expect("shard startup failed")
    }
    /// Start a background thread with a custom grace period for draining pending futures.
    pub fn try_new(name: &str, grace: std::time::Duration) -> std::io::Result<Self> {
        let inner =
            crate::background::Background::new(name, crate::background::Runtime::Standard, grace)?;
        let shard_id = inner.handle().shard_id;
        Ok(Self { inner, shard_id })
    }
    /// Stop and join from Tokio without blocking a worker; cannot join the current shard.
    #[cfg(feature = "tokio")]
    pub async fn join_async(&self) -> Result<(), ShardError> {
        self.inner.join_async().await
    }
    /// Reject new work and enqueue shutdown after previously accepted jobs.
    pub fn request_shutdown(&self) {
        self.handle().request_shutdown();
    }
    /// Submit a construction factory immediately; await its result without blocking.
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
impl EventLoop for Shard {
    type HandleType = ShardEventHandle;
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
