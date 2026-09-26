//! Common admission queue, dispatch, owner-thread storage, and shutdown driver.
mod driver;
pub(crate) use driver::drive;

use crate::{
    EventLoopHandle, Eventful, HasEvents, ShardAffinity, ShardHandle, ShardId, ShardRc,
    ShardRcStore,
};
use futures::{
    FutureExt,
    channel::{mpsc, oneshot},
};
use std::{
    cell::RefCell,
    collections::HashMap,
    fmt,
    future::Future,
    panic::AssertUnwindSafe,
    pin::Pin,
    rc::Rc,
    sync::{Arc, Mutex},
    thread::{self, ThreadId},
};

/// A future confined to the shard thread; it need not implement Send.
pub(crate) type LocalFuture = Pin<Box<dyn Future<Output = ()> + 'static>>;
/// A Send factory producing a local future after crossing the queue.
type Job = Box<dyn FnOnce(Rc<RefCell<ShardRcStore>>) -> LocalFuture + Send + 'static>;
/// Messages serialized through the admission queue.
pub(crate) enum Command {
    /// Construct and start one admitted operation on the owner thread.
    Job(Job),
    /// Drain pending operations after all preceding jobs have started.
    Stop,
}
/// Single owner of the shard admission queue.
pub(crate) type Receiver = mpsc::UnboundedReceiver<Command>;

thread_local! {
    static STORES: RefCell<HashMap<ShardId, Rc<RefCell<ShardRcStore>>>> = RefCell::new(HashMap::new());
}

/// Whether this thread currently owns any shard stores.
pub(crate) fn on_shard_thread() -> bool {
    STORES.with(|s| !s.borrow().is_empty())
}

/// Access or initialize the thread-local store for a shard identity.
pub(crate) fn store(id: ShardId) -> Rc<RefCell<ShardRcStore>> {
    STORES.with(|s| {
        s.borrow_mut()
            .entry(id)
            .or_insert_with(|| Rc::new(RefCell::new(ShardRcStore::new())))
            .clone()
    })
}

/// Removes a shard store on exit, dropping values outside the TLS borrow.
pub(crate) struct ContextGuard(ShardId);
impl ContextGuard {
    /// Enter the shard context on this thread, creating its store if necessary.
    pub(crate) fn new(id: ShardId) -> Self {
        let _ = store(id);
        Self(id)
    }
}
impl Drop for ContextGuard {
    fn drop(&mut self) {
        let removed = STORES.with(|s| s.borrow_mut().remove(&self.0));
        drop(removed); // Never call user destructors under the TLS map borrow.
    }
}

/// A submission or deferred invocation failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum InvokeError {
    /// The shard no longer accepts submissions.
    Closed,
    /// The value or declared affinity belongs to another shard.
    WrongShard,
    /// The target value no longer exists in its shard's store.
    ValueMissing,
    /// The callback or factory unwound. Mutations are not rolled back.
    Panicked,
    /// The driver dropped the operation before returning its result.
    Canceled,
}
impl fmt::Display for InvokeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Closed => "shard is stopping or stopped",
            Self::WrongShard => "handle belongs to another shard",
            Self::ValueMissing => "target value no longer exists",
            Self::Panicked => "callback panicked",
            Self::Canceled => "invocation canceled before returning a result",
        })
    }
}
impl std::error::Error for InvokeError {}

/// Thread-safe submission handle shared by the runtime backends.
/// Submitted closures cross threads; shard-local values and their callback futures do not.
#[derive(Clone)]
pub struct ShardEventHandle {
    /// Stable identity used to validate affinity and look up the owner store.
    pub(crate) shard_id: ShardId,
    /// Admission lock; taking the sender permanently closes new submissions.
    pub(crate) sender: Arc<Mutex<Option<mpsc::UnboundedSender<Command>>>>,
}
impl fmt::Debug for ShardEventHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShardEventHandle")
            .field("shard_id", &self.shard_id)
            .finish_non_exhaustive()
    }
}
impl ShardEventHandle {
    /// Create an open submission handle and its sole receiver.
    pub(crate) fn channel() -> (Self, Receiver) {
        let (tx, rx) = mpsc::unbounded();
        (
            Self {
                shard_id: ShardId::new(),
                sender: Arc::new(Mutex::new(Some(tx))),
            },
            rx,
        )
    }
    /// Serialize admission with shutdown; drop rejected captures outside the lock.
    pub(crate) fn post(&self, job: Job) -> Result<(), InvokeError> {
        let command = Command::Job(job);
        let result = {
            let tx = self.sender.lock().unwrap_or_else(|e| e.into_inner());
            match tx.as_ref() {
                Some(tx) => tx.unbounded_send(command).map_err(|e| e.into_inner()),
                None => Err(command),
            }
        };
        // Drop captured user values only after releasing the admission lock.
        result.map_err(|command| {
            drop(command);
            InvokeError::Closed
        })
    }
    /// Reject new submissions immediately and enqueue shutdown after accepted work.
    pub fn request_shutdown(&self) {
        let tx = self.sender.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(tx) = tx {
            let _ = tx.unbounded_send(Command::Stop);
        }
    }
    /// Whether admission has closed or the driver has stopped.
    pub fn is_closed(&self) -> bool {
        self.sender
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .is_none_or(|s| s.is_closed())
    }
    /// Queue a synchronous callback.
    ///
    /// # Errors
    /// Returns [`InvokeError::Closed`] if admission has stopped.
    pub fn try_invoke<F>(&self, f: F) -> Result<(), InvokeError>
    where
        F: FnOnce() + Send + 'static,
    {
        self.post(Box::new(move |_| {
            Box::pin(async move {
                f();
            })
        }))
    }
    /// Queue an async callback; its future is created and polled on the shard.
    ///
    /// # Errors
    /// Returns [`InvokeError::Closed`] if admission has stopped.
    pub fn try_invoke_async<F>(&self, f: F) -> Result<(), InvokeError>
    where
        F: AsyncFnOnce() -> () + Send + 'static,
    {
        self.post(Box::new(move |_| {
            Box::pin(async move {
                f().await;
            })
        }))
    }
    /// Queue an existing Send future.
    ///
    /// # Errors
    /// Returns [`InvokeError::Closed`] if admission has stopped.
    pub fn try_spawn<F>(&self, f: F) -> Result<(), InvokeError>
    where
        F: Future + Send + 'static,
    {
        self.post(Box::new(move |_| {
            Box::pin(async move {
                f.await;
            })
        }))
    }
    /// Queue a callback on a shard-local value, rejecting closed or mismatched shards.
    /// A target that expires before execution is silently skipped.
    ///
    /// # Errors
    /// Returns [`InvokeError::WrongShard`] for a mismatched handle, or
    /// [`InvokeError::Closed`] if admission has stopped.
    pub fn try_invoke_with_handle<T, H, F>(&self, handle: H, f: F) -> Result<(), InvokeError>
    where
        T: Eventful + HasEvents<T::EventSetType> + 'static,
        H: ShardHandle<T>,
        F: FnOnce(&T) + Send + 'static,
    {
        if handle.shard_id() != self.shard_id {
            return Err(InvokeError::WrongShard);
        }
        self.post(Box::new(move |store| {
            Box::pin(async move {
                let value = {
                    store
                        .borrow()
                        .get::<crate::shard_handle::ShardValue<T>>(handle.id())
                };
                if let Some(value) = value {
                    f(&value);
                }
            })
        }))
    }
    /// Queue an async callback on a shard-local value, rejecting closed or mismatched shards.
    /// A target that expires before execution is silently skipped.
    ///
    /// # Errors
    /// Returns [`InvokeError::WrongShard`] for a mismatched handle, or
    /// [`InvokeError::Closed`] if admission has stopped.
    pub fn try_invoke_with_handle_async<T, H, F>(&self, handle: H, f: F) -> Result<(), InvokeError>
    where
        T: Eventful + HasEvents<T::EventSetType> + 'static,
        H: ShardHandle<T>,
        F: AsyncFnOnce(&T) -> () + Send + 'static,
    {
        if handle.shard_id() != self.shard_id {
            return Err(InvokeError::WrongShard);
        }
        self.post(Box::new(move |store| {
            Box::pin(async move {
                let value = {
                    store
                        .borrow()
                        .get::<crate::shard_handle::ShardValue<T>>(handle.id())
                };
                if let Some(value) = value {
                    f(&value).await;
                }
            })
        }))
    }
    /// Submit immediately and asynchronously receive the result. Dropping the
    /// receiver does not cancel the operation. Weak targets may be missing.
    ///
    /// # Errors
    /// Reports closed shards, mismatched or missing targets, cancellation, and
    /// unwinding callback panics through [`InvokeError`].
    pub fn try_deferred_invoke<T, H, F, R>(
        &self,
        handle: H,
        f: F,
    ) -> impl Future<Output = Result<R, InvokeError>> + Send + 'static + use<T, H, F, R>
    where
        T: Eventful + HasEvents<T::EventSetType> + 'static,
        H: ShardHandle<T>,
        F: AsyncFnOnce(&T) -> R + Send + 'static,
        R: Send + 'static,
    {
        let (tx, rx) = oneshot::channel();
        let posted = if handle.shard_id() != self.shard_id {
            Err(InvokeError::WrongShard)
        } else {
            self.post(Box::new(move |store| {
                Box::pin(async move {
                    let result = AssertUnwindSafe(async move {
                        let value = {
                            store
                                .borrow()
                                .get::<crate::shard_handle::ShardValue<T>>(handle.id())
                        };
                        match value {
                            Some(value) => Ok(f(&value).await),
                            None => Err(InvokeError::ValueMissing),
                        }
                    })
                    .catch_unwind()
                    .await
                    .unwrap_or(Err(InvokeError::Panicked));
                    let _ = tx.send(result);
                })
            }))
        };
        async move {
            posted?;
            rx.await.map_err(|_| InvokeError::Canceled)?
        }
    }
    /// Create an eventful value on its owner thread; usable from any executor.
    ///
    /// # Errors
    /// Returns [`InvokeError`] on shutdown, affinity mismatch, cancellation,
    /// or an unwinding factory panic.
    pub fn bind_async<F, R, T>(
        &self,
        f: F,
    ) -> impl Future<Output = Result<R, InvokeError>> + Send + 'static + use<F, R, T>
    where
        F: FnOnce(&dyn Fn(T) -> ShardRc<T>) -> R + Send + 'static,
        R: Send + 'static,
        T: Eventful + HasEvents<T::EventSetType> + 'static,
    {
        let handle = self.clone();
        let (tx, rx) = oneshot::channel();
        let posted = if T::Shard::shard_id().is_some_and(|id| id != self.shard_id) {
            Err(InvokeError::WrongShard)
        } else {
            self.post(Box::new(move |store| {
                Box::pin(async move {
                    let result =
                        std::panic::catch_unwind(AssertUnwindSafe(|| bind_here(&store, handle, f)))
                            .map_err(|_| InvokeError::Panicked);
                    let _ = tx.send(result);
                })
            }))
        };
        async move {
            posted?;
            rx.await.map_err(|_| InvokeError::Canceled)?
        }
    }
    /// Queue construction and synchronously wait for its result.
    pub(crate) fn bind_blocking_factory<F, R, T>(&self, f: F) -> Result<R, InvokeError>
    where
        F: FnOnce(&dyn Fn(T) -> ShardRc<T>) -> R + Send + 'static,
        R: Send + 'static,
        T: Eventful + HasEvents<T::EventSetType> + 'static,
    {
        if T::Shard::shard_id().is_some_and(|id| id != self.shard_id) {
            return Err(InvokeError::WrongShard);
        }
        let (tx, rx) = std::sync::mpsc::channel();
        let handle = self.clone();
        self.post(Box::new(move |store| {
            Box::pin(async move {
                let result =
                    std::panic::catch_unwind(AssertUnwindSafe(|| bind_here(&store, handle, f)))
                        .map_err(|_| InvokeError::Panicked);
                let _ = tx.send(result);
            })
        }))?;
        rx.recv().map_err(|_| InvokeError::Canceled)?
    }
    /// Bind directly on the owner thread, otherwise use blocking dispatch.
    pub(crate) fn bind<F, R, T>(&self, owner: ThreadId, f: F) -> R
    where
        F: FnOnce(&dyn Fn(T) -> ShardRc<T>) -> R + Send + 'static,
        R: Send + 'static,
        T: Eventful + HasEvents<T::EventSetType> + 'static,
    {
        if thread::current().id() == owner {
            assert!(
                !self.is_closed() || has_context(self.shard_id),
                "shard is closed"
            );
            return bind_here(&store(self.shard_id), self.clone(), f);
        }
        assert_not_async("cross-thread bind blocks; use bind_async instead");
        self.bind_blocking_factory(f).expect("shard binding failed")
    }
}

/// Reject operations that would synchronously block a Tokio executor.
pub(crate) fn assert_not_async(message: &str) {
    #[cfg(feature = "tokio")]
    assert!(
        ::tokio::runtime::Handle::try_current().is_err(),
        "{message}"
    );
    let _ = message;
}

/// Validate affinity and expose a factory for registering local values.
pub(crate) fn bind_here<F, R, T>(store: &RefCell<ShardRcStore>, handle: ShardEventHandle, f: F) -> R
where
    F: FnOnce(&dyn Fn(T) -> ShardRc<T>) -> R,
    T: Eventful + HasEvents<T::EventSetType> + 'static,
{
    assert!(
        T::Shard::shard_id().is_none_or(|id| id == handle.shard_id),
        "eventful type belongs to a different shard"
    );
    f(&|value| {
        let value = Rc::new(crate::shard_handle::ShardValue::new(value));
        let id = store.borrow_mut().insert(value.clone());
        ShardRc::new(id, value, handle.clone())
    })
}

impl EventLoopHandle for ShardEventHandle {
    fn try_deferred_invoke<T, H, F, R>(
        &self,
        handle: H,
        task: F,
    ) -> futures::future::BoxFuture<'static, Result<R, InvokeError>>
    where
        T: Eventful + HasEvents<T::EventSetType> + 'static,
        H: ShardHandle<T>,
        F: AsyncFnOnce(&T) -> R + Send + 'static,
        R: Send + 'static,
    {
        Box::pin(ShardEventHandle::try_deferred_invoke(self, handle, task))
    }

    fn shard_id(&self) -> ShardId {
        self.shard_id
    }
    fn invoke<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.try_invoke(f).expect("shard submission failed");
    }
    fn invoke_async<F>(&self, f: F)
    where
        F: AsyncFnOnce() -> () + Send + 'static,
    {
        self.try_invoke_async(f).expect("shard submission failed");
    }
    fn invoke_with_handle<T, H, F>(&self, h: H, f: F)
    where
        T: Eventful + HasEvents<T::EventSetType> + 'static,
        H: ShardHandle<T>,
        F: FnOnce(&T) + Send + 'static,
    {
        // A signal connected to a stopped shard is a no-op, like an expired weak target.
        // Explicit callers who need admission errors use try_invoke_with_handle.
        let _ = self.try_invoke_with_handle(h, f);
    }
    fn invoke_with_handle_async<T, H, F>(&self, h: H, f: F)
    where
        T: Eventful + HasEvents<T::EventSetType> + 'static,
        H: ShardHandle<T>,
        F: AsyncFnOnce(&T) -> () + Send + 'static,
    {
        let _ = self.try_invoke_with_handle_async(h, f);
    }
    fn deferred_invoke<T, H, F, R>(&self, h: H, f: F) -> futures::future::BoxFuture<'static, R>
    where
        T: Eventful + HasEvents<T::EventSetType> + 'static,
        H: ShardHandle<T>,
        F: AsyncFnOnce(&T) -> R + Send + 'static,
        R: Send + 'static,
    {
        let result = self.try_deferred_invoke(h, f);
        Box::pin(async move { result.await.expect("deferred shard invocation failed") })
    }
    fn spawn<F>(&self, f: F)
    where
        F: Future + Send + 'static,
    {
        self.try_spawn(f).expect("shard submission failed");
    }
}

/// Whether this thread already contains the selected shard store.
pub(crate) fn has_context(id: ShardId) -> bool {
    STORES.with(|s| s.borrow().contains_key(&id))
}
