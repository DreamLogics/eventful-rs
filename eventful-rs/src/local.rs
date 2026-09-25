//! Calling-thread shard using executor-independent Rust futures.
use crate::{EventLoop, Eventful, HasEvents, ShardError, ShardId, ShardRc};
use std::future::Future;
/// Submission handle for this backend.
pub type LocalShardHandle = crate::ShardEventHandle;

/// Single-use event loop driven on its constructing thread.
pub struct LocalShard {
    /// Shared implementation of the calling-thread driver.
    inner: crate::main_loop::MainLoop,
    /// Identity of this shard.
    shard_id: ShardId,
}
impl LocalShard {
    /// Return this backend's immutable shard identity.
    pub fn shard_id(&self) -> ShardId {
        self.shard_id
    }
    /// Create a single-use event loop bound to the current thread.
    pub fn new() -> Self {
        let inner = crate::main_loop::MainLoop::new(crate::background::Runtime::Standard);
        let shard_id = inner.handle.shard_id;
        Self { inner, shard_id }
    }
    /// Run once on the constructing thread. The main future may be non-Send.
    ///
    /// # Panics
    /// Panics on another thread, repeated use, or inside a Tokio runtime.
    /// Panics from the main future propagate to the caller.
    pub fn run_main<F, RF, R>(&self, main: F) -> R
    where
        F: FnOnce() -> RF + 'static,
        RF: Future<Output = R> + 'static,
        R: 'static,
    {
        self.inner.run_main(main)
    }
    /// Drive until shutdown, once only, on the constructing thread and outside Tokio.
    ///
    /// # Panics
    /// Panics on another thread, repeated use, or inside a Tokio runtime.
    pub fn run_event_loop(&self) {
        self.inner.run_event_loop()
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
impl Default for LocalShard {
    fn default() -> Self {
        Self::new()
    }
}
impl EventLoop for LocalShard {
    type HandleType = LocalShardHandle;
    fn handle(&self) -> Self::HandleType {
        self.inner.handle.clone()
    }
    fn bind<F, R, T>(&self, f: F) -> R
    where
        F: FnOnce(&dyn Fn(T) -> ShardRc<T>) -> R + Send + 'static,
        R: Send + 'static,
        T: Eventful + HasEvents<T::EventSetType> + 'static,
    {
        self.handle().bind(self.inner.owner, f)
    }
    fn join(&self) -> Result<(), ShardError> {
        Ok(())
    }
}

impl std::fmt::Debug for LocalShard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalShard")
            .field("shard_id", &self.shard_id)
            .finish_non_exhaustive()
    }
}
