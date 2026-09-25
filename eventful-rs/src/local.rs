//! Calling-thread shard using executor-independent Rust futures.
use crate::{EventLoop, Eventful, HasEvents, ShardError, ShardId, ShardRc};
use std::future::Future;
pub type LocalShardHandle = crate::ShardEventHandle;

pub struct LocalShard {
    inner: crate::main_loop::MainLoop,
    pub shard_id: ShardId,
}
impl LocalShard {
    pub fn new() -> Self {
        let inner = crate::main_loop::MainLoop::new(crate::background::Runtime::Standard);
        let shard_id = inner.handle.shard_id;
        Self { inner, shard_id }
    }
    /// Run once on the constructing thread. The main future may be non-Send.
    pub fn run_main<F, RF, R>(&self, main: F) -> R
    where
        F: FnOnce() -> RF + 'static,
        RF: Future<Output = R> + 'static,
        R: 'static,
    {
        self.inner.run_main(main)
    }
    pub fn run_event_loop(&self) {
        self.inner.run_event_loop()
    }
    pub fn request_shutdown(&self) {
        self.handle().request_shutdown();
    }
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
