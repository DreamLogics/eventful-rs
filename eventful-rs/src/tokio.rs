//! Dedicated-thread shard using Tokio.
use crate::{EventLoop, Eventful, HasEvents, ShardError, ShardId, ShardRc};
use std::future::Future;
pub type TokioShardHandle = crate::ShardEventHandle;

pub struct TokioShard {
    inner: crate::background::Background,
    pub shard_id: ShardId,
}
impl TokioShard {
    pub fn new(name: &str) -> Self {
        Self::try_new(name, std::time::Duration::from_secs(5)).expect("shard startup failed")
    }
    pub fn try_new(name: &str, grace: std::time::Duration) -> std::io::Result<Self> {
        let inner =
            crate::background::Background::new(name, crate::background::Runtime::Tokio, grace)?;
        let shard_id = inner.handle().shard_id;
        Ok(Self { inner, shard_id })
    }
    #[cfg(feature = "tokio")]
    pub async fn join_async(&self) -> Result<(), ShardError> {
        self.inner.join_async().await
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
