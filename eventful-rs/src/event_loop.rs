//! Backend contracts and shard affinity.
use crate::{Eventful, HasEvents, InvokeError, ShardError, ShardHandle, ShardId, ShardRc};

/// Thread-safe submission contract implemented by shard backends.
/// See the [calling methods guide](crate#calling-methods).
/// Built-in backends queue accepted jobs in admission order; async jobs may interleave.
pub trait EventLoopHandle: Clone + Send + Sync + 'static {
    /// Identity of the destination shard.
    fn shard_id(&self) -> ShardId;
    /// Queue a synchronous callback.
    ///
    /// # Panics
    /// Built-in backends panic if admission is closed.
    fn invoke<F>(&self, task: F)
    where
        F: FnOnce() + Send + 'static;
    /// Queue an async callback.
    ///
    /// # Panics
    /// Built-in backends panic if admission is closed.
    fn invoke_async<F>(&self, f: F)
    where
        F: AsyncFnOnce() -> () + Send + 'static;
    /// Queue a callback on a shard-local value; built-in backends skip missing or rejected targets.
    fn invoke_with_handle<T, H, F>(&self, handle: H, f: F)
    where
        T: Eventful + HasEvents<T::EventSetType> + Sized + 'static,
        H: ShardHandle<T>,
        F: FnOnce(&T) + Send + 'static;
    /// Queue an async callback on a shard-local value, allowing interleaving while it awaits.
    fn invoke_with_handle_async<T, H, F>(&self, handle: H, f: F)
    where
        T: Eventful + HasEvents<T::EventSetType> + Sized + 'static,
        H: ShardHandle<T>,
        F: AsyncFnOnce(&T) -> () + Send + 'static;

    /// Submit immediately and observe the result.
    ///
    /// # Panics
    /// Built-in backends panic on delivery failure, including a callback panic.
    fn deferred_invoke<T, H, F, R>(
        &self,
        handle: H,
        task: F,
    ) -> futures::future::BoxFuture<'static, R>
    where
        T: Eventful + HasEvents<T::EventSetType> + Sized + 'static,
        H: ShardHandle<T>,
        F: AsyncFnOnce(&T) -> R + Send + 'static,
        R: Send + 'static;

    /// Submit a callback immediately and report its completion. Backends may
    /// override this to distinguish rejection and missing targets from cancellation.
    ///
    /// # Errors
    /// Returns [`InvokeError`] for failed delivery. The default implementation
    /// distinguishes callback panics and cancellation.
    fn try_deferred_invoke<T, H, F, R>(
        &self,
        handle: H,
        task: F,
    ) -> futures::future::BoxFuture<'static, Result<R, InvokeError>>
    where
        T: Eventful + HasEvents<T::EventSetType> + Sized + 'static,
        H: ShardHandle<T>,
        F: AsyncFnOnce(&T) -> R + Send + 'static,
        R: Send + 'static,
    {
        use futures::FutureExt;
        let (tx, rx) = futures::channel::oneshot::channel();
        self.invoke_with_handle_async(handle, async move |target| {
            let result = std::panic::AssertUnwindSafe(async move { task(target).await })
                .catch_unwind()
                .await
                .map_err(|_| InvokeError::Panicked);
            let _ = tx.send(result);
        });
        Box::pin(async move { rx.await.map_err(|_| InvokeError::Canceled)? })
    }

    /// Submit a future.
    ///
    /// # Panics
    /// Built-in backends panic if admission is closed.
    fn spawn<F>(&self, f: F)
    where
        F: Future + Send + 'static;
}

/// Lifecycle and construction interface shared by runtime backends.
/// See the [`crate::DynamicShard`] construction example.
pub trait EventLoop {
    /// Thread-safe handle used to submit work.
    type HandleType: EventLoopHandle;
    /// Clone the submission handle.
    fn handle(&self) -> Self::HandleType;
    /// Construct eventful values on this shard and return a Send result.
    /// Runs directly on the owner thread; otherwise blocks until the factory finishes.
    ///
    /// # Panics
    /// Built-in backends panic on wrong affinity, stopped shards, factory panics, or cross-thread
    /// blocking from Tokio. Use backend `bind_async` methods in async code.
    fn bind<F, R, T>(&self, f: F) -> R
    where
        F: FnOnce(&dyn Fn(T) -> ShardRc<T>) -> R + Send + 'static,
        R: Send + 'static,
        T: Eventful + HasEvents<T::EventSetType> + Sized + 'static;
    /// Submit a future.
    ///
    /// # Panics
    /// Built-in backends panic if admission is closed.
    fn spawn<F>(&self, f: F)
    where
        F: Future + Send + 'static,
    {
        self.handle().spawn(f);
    }
    /// Stop and join a background shard. Fails on its own thread or inside Tokio.
    /// Calling-thread and Slint backends return immediately; drive their shutdown separately.
    ///
    /// # Errors
    /// Returns [`ShardError`] for self-joining, blocking inside Tokio, or failed shutdown.
    fn join(&self) -> Result<(), ShardError>;
}
