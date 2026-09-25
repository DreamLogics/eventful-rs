use crate::{
    EventLoopHandle, Eventful, HasEvents, ShardAffinity, ShardHandle, ShardId, ShardRc,
    ShardRcStore,
};
use futures::{
    FutureExt, StreamExt,
    channel::{mpsc, oneshot},
    stream::FuturesUnordered,
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
    time::Duration,
};

pub(crate) type LocalFuture = Pin<Box<dyn Future<Output = ()> + 'static>>;
type Job = Box<dyn FnOnce(Rc<RefCell<ShardRcStore>>) -> LocalFuture + Send + 'static>;
pub(crate) enum Command {
    Job(Job),
    Stop,
}
pub(crate) type Receiver = mpsc::UnboundedReceiver<Command>;

thread_local! {
    static STORES: RefCell<HashMap<ShardId, Rc<RefCell<ShardRcStore>>>> = RefCell::new(HashMap::new());
}

pub(crate) fn on_shard_thread() -> bool {
    STORES.with(|s| !s.borrow().is_empty())
}

pub(crate) fn store(id: ShardId) -> Rc<RefCell<ShardRcStore>> {
    STORES.with(|s| {
        s.borrow_mut()
            .entry(id)
            .or_insert_with(|| Rc::new(RefCell::new(ShardRcStore::new())))
            .clone()
    })
}

pub(crate) struct ContextGuard(ShardId);
impl ContextGuard {
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
pub enum InvokeError {
    Closed,
    WrongShard,
    /// The target value no longer exists in its shard's store.
    ValueMissing,
    Panicked,
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
/// Only closures and value IDs cross threads; shard values and their futures do not.
#[derive(Clone)]
pub struct ShardEventHandle {
    pub(crate) shard_id: ShardId,
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
    pub(crate) fn post(&self, job: Job) -> Result<(), InvokeError> {
        let command = Command::Job(job);
        let result = {
            let tx = self.sender.lock().unwrap();
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
        let tx = self.sender.lock().unwrap().take();
        if let Some(tx) = tx {
            let _ = tx.unbounded_send(Command::Stop);
        }
    }
    pub fn is_closed(&self) -> bool {
        self.sender
            .lock()
            .unwrap()
            .as_ref()
            .is_none_or(|s| s.is_closed())
    }
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
                let value = { store.borrow().get::<T>(handle.id()) };
                if let Some(value) = value {
                    f(&value);
                }
            })
        }))
    }
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
                let value = { store.borrow().get::<T>(handle.id()) };
                if let Some(value) = value {
                    f(&value).await;
                }
            })
        }))
    }
    /// Submit immediately and asynchronously receive the result. Dropping the
    /// receiver does not cancel the operation. Weak targets may be missing.
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
                        let value = { store.borrow().get::<T>(handle.id()) };
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
    /// Create a value on its owner thread; usable from any executor.
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

pub(crate) fn assert_not_async(message: &str) {
    #[cfg(feature = "tokio")]
    assert!(
        ::tokio::runtime::Handle::try_current().is_err(),
        "{message}"
    );
    let _ = message;
}

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
        let value = Rc::new(value);
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

fn guarded(future: LocalFuture) -> LocalFuture {
    Box::pin(async move {
        if AssertUnwindSafe(future).catch_unwind().await.is_err() {
            eprintln!("eventful-rs: shard callback panicked");
        }
    })
}

/// Remove first, then drop outside the RefCell borrow: destructors may bind values.
fn collect(store: &RefCell<ShardRcStore>) {
    let retired = store.borrow_mut().take_garbage();
    drop(retired);
}

pub(crate) async fn drive(
    mut rx: Receiver,
    handle: ShardEventHandle,
    grace: Duration,
    initial: Option<LocalFuture>,
) {
    let context = store(handle.shard_id);
    let mut pending = FuturesUnordered::<LocalFuture>::new();
    if let Some(initial) = initial {
        pending.push(guarded(initial));
    }
    let mut gc = futures_timer::Delay::new(Duration::from_millis(100)).fuse();
    enum Next {
        Command(Option<Command>),
        Completed,
        Collect,
    }
    let mut turns = 0usize;
    loop {
        turns += 1;
        if turns == 64 {
            turns = 0;
            let mut yielded = false;
            futures::future::poll_fn(|cx| {
                if yielded {
                    std::task::Poll::Ready(())
                } else {
                    yielded = true;
                    cx.waker().wake_by_ref();
                    std::task::Poll::Pending
                }
            })
            .await;
        }
        let event = {
            let next = async {
                if pending.is_empty() {
                    futures::future::pending::<Option<()>>().await
                } else {
                    pending.next().await
                }
            }
            .fuse();
            futures::pin_mut!(next);
            futures::select! {
                command = rx.next().fuse() => Next::Command(command),
                _ = next => Next::Completed,
                _ = gc => Next::Collect,
            }
        };
        match event {
            Next::Command(Some(Command::Job(job))) => {
                let ctx = context.clone();
                let mut future = guarded(Box::pin(async move {
                    job(ctx).await;
                }));
                // Start in admission order. A Pending operation then interleaves
                // with later work; a synchronous callback finishes right here.
                let polled =
                    futures::future::poll_fn(|cx| std::task::Poll::Ready(future.as_mut().poll(cx)))
                        .await;
                if polled.is_pending() {
                    pending.push(future);
                }
            }
            Next::Command(Some(Command::Stop) | None) => break,
            Next::Completed => {}
            Next::Collect => {
                collect(&context);
                gc = futures_timer::Delay::new(Duration::from_millis(100)).fuse();
            }
        }
    }
    rx.close();
    {
        let drain = async { while pending.next().await.is_some() {} }.fuse();
        let deadline = futures_timer::Delay::new(grace).fuse();
        futures::pin_mut!(drain, deadline);
        futures::select! { _ = drain => {}, _ = deadline => {} }
    }
    // Drop canceled futures while the context is still on its owner thread.
    drop(pending);
    collect(&context);
    handle.request_shutdown();
}

pub(crate) fn has_context(id: ShardId) -> bool {
    STORES.with(|s| s.borrow().contains_key(&id))
}
