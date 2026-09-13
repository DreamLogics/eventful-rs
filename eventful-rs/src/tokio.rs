use futures::channel::oneshot;
use std::{
    cell::RefCell,
    panic::{AssertUnwindSafe, catch_unwind},
    rc::Rc,
    sync::{Arc, Mutex},
};
use tokio::{
    runtime::{Handle, Runtime},
    sync::Semaphore,
};

use crate::{
    EventLoop, EventLoopHandle, EventTarget, Eventful, HasEvents, ShardId, ShardRc, ShardRcStore,
    Task,
};

#[derive(Clone)]
pub struct TokioShardHandle {
    tokio_rt: Handle,
    shard_id: ShardId,
    tx: tokio::sync::mpsc::Sender<Task<ShardCtxHandle>>,
}

impl EventLoopHandle for TokioShardHandle {
    fn shard_id(&self) -> ShardId {
        self.shard_id
    }
    fn invoke<F>(&self, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        drop(self.tokio_rt.spawn(async move {
            let _ = catch_unwind(AssertUnwindSafe(task));
        }));
    }

    fn invoke_async<F>(&self, task: F)
    where
        F: AsyncFnOnce() -> () + Send + 'static,
    {
        let _ = self
            .tx
            .blocking_send(Task::CallAsync(Box::new(move || Box::pin(task()))));
    }

    fn invoke_with_handle<T, H, F>(&self, handle: H, f: F)
    where
        T: Eventful<EventLoopHandleType = Self> + HasEvents<T::EventSetType> + Sized + 'static,
        H: crate::ShardHandle<T>,
        F: FnOnce(&T) + Send + 'static,
    {
        let _ = self
            .tx
            .blocking_send(Task::CallWithContext(Box::new(move |ctx| {
                let id = handle.id();
                if let Some(obj) = ctx.get_ctx().object_store.borrow().get(id) {
                    f(&obj);
                }
            })));
    }

    fn invoke_with_handle_async<T, H, F>(&self, handle: H, f: F)
    where
        T: Eventful<EventLoopHandleType = Self> + HasEvents<T::EventSetType> + Sized + 'static,
        H: crate::ShardHandle<T>,
        F: AsyncFnOnce(&T) -> () + Send + 'static,
    {
        let _ = self
            .tx
            .blocking_send(Task::CallWithContextAsync(Box::new(move |ctx| {
                let id = handle.id();
                if let Some(obj) = ctx.get_ctx().object_store.borrow().get::<T>(id) {
                    let obj = obj.clone();
                    Box::pin(async move {
                        let obj = obj;
                        f(&obj).await;
                    })
                } else {
                    Box::pin(async {})
                }
            })));
    }

    fn deferred_invoke<T, H, F, R>(
        &self,
        handle: H,
        task: F,
    ) -> impl Future<Output = R> + Send + 'static
    where
        T: Eventful<EventLoopHandleType = Self> + HasEvents<T::EventSetType> + Sized + 'static,
        H: crate::ShardHandle<T>,
        F: AsyncFnOnce(&T) -> R + Send + 'static,
        R: Send + 'static,
    {
        let (tx, rx) = oneshot::channel::<R>();

        self.invoke_with_handle_async(handle, async move |ctx: &T| {
            let result = task(ctx).await;
            let _ = tx.send(result);
        });

        async move {
            rx.await
                .expect("target event loop dropped deferred invocation")
        }
    }
}

pub struct TokioShard {
    rt: Arc<Mutex<Option<Handle>>>,
    pub shard_id: ShardId,
    thread_id: std::thread::ThreadId,
    join_handle: Mutex<Option<std::thread::JoinHandle<()>>>,
    tx: tokio::sync::mpsc::Sender<Task<ShardCtxHandle>>,
    ctx: Arc<Mutex<Option<ShardCtxHandle>>>,
}

struct ShardCtxHandle(*const TokioShardCtx);
impl ShardCtxHandle {
    fn get_ctx(&self) -> &TokioShardCtx {
        assert!(
            std::thread::current().id() == std::thread::current().id(),
            "ShardCtxHandle can only be accessed from the shard's thread"
        );
        unsafe { &*self.0 }
    }
}
impl Clone for ShardCtxHandle {
    fn clone(&self) -> Self {
        ShardCtxHandle(self.0)
    }
}
unsafe impl Send for ShardCtxHandle {}

struct TokioShardCtx {
    object_store: RefCell<ShardRcStore>,
}

impl TokioShard {
    pub fn new() -> Self {
        let shard_id = ShardId::new();
        let (tx, rx) = tokio::sync::mpsc::channel::<Task<ShardCtxHandle>>(Semaphore::MAX_PERMITS);
        let rt_handle: Arc<Mutex<Option<Handle>>> = Arc::new(Mutex::new(None));
        let rt_handle_clone = Arc::clone(&rt_handle);
        let ctx_handle: Arc<Mutex<Option<ShardCtxHandle>>> = Arc::new(Mutex::new(None));
        let ctx_handle_clone = ctx_handle.clone();
        let join_handle = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt_handle_clone.lock().unwrap().replace(rt.handle().clone());
            rt.block_on(async {
                let mut rx = rx;
                let ctx = TokioShardCtx {
                    object_store: RefCell::new(ShardRcStore::new()),
                };
                ctx_handle_clone
                    .lock()
                    .unwrap()
                    .replace(ShardCtxHandle(&ctx as *const TokioShardCtx));
                let ctx_handle = ShardCtxHandle(&ctx as *const TokioShardCtx);
                while let Some(task) = rx.recv().await {
                    match task {
                        Task::Call(f) => {
                            let _ = catch_unwind(AssertUnwindSafe(f));
                        }
                        Task::CallWithContext(f) => {
                            let _ = catch_unwind(AssertUnwindSafe(|| f(&ctx_handle)));
                        }
                        Task::CallAsync(f) => {
                            f().await;
                        }
                        Task::CallWithContextAsync(f) => {
                            f(&ctx_handle).await;
                        }
                        Task::Wake(_) => {}
                        Task::Stop => break,
                    }
                }
                *ctx_handle_clone.lock().unwrap() = None;
            });
        });
        let thread_id = join_handle.thread().id();

        Self {
            rt: rt_handle,
            thread_id,
            shard_id,
            tx,
            join_handle: Mutex::new(Some(join_handle)),
            ctx: ctx_handle,
        }
    }

    pub fn run<F, R>(&self, main_fn: F) -> R::Output
    where
        F: FnOnce() -> R + Send + 'static,
        R: Future,
    {
        let rt_handle = self.rt.lock().unwrap();
        let handle = rt_handle.as_ref().expect("Runtime handle not initialized");
        handle.block_on(async move { main_fn().await })
    }

    pub fn join(&self) {
        assert!(
            std::thread::current().id() != self.thread_id,
            "Cannot join the shard from its own thread"
        );
        if let Some(join_handle) = self.join_handle.lock().unwrap().take() {
            self.tx
                .blocking_send(Task::Stop)
                .expect("Failed to send stop task");
            let _ = join_handle.join();
        }
    }
}

impl Default for TokioShard {
    fn default() -> Self {
        Self::new()
    }
}

impl EventLoop for TokioShard {
    type HandleType = TokioShardHandle;

    fn handle(&self) -> Self::HandleType {
        TokioShardHandle {
            tokio_rt: self.rt.lock().unwrap().as_ref().unwrap().clone(),
            shard_id: self.shard_id,
            tx: self.tx.clone(),
        }
    }

    fn spawn<F, R, T>(&self, f: F) -> R
    where
        F: FnOnce(&dyn Fn(T) -> ShardRc<T>) -> R + Send + 'static,
        R: Send + 'static,
        T: Eventful<EventLoopHandleType = Self::HandleType>
            + HasEvents<T::EventSetType>
            + Sized
            + 'static,
    {
        let handle = self.handle();
        if std::thread::current().id() != self.thread_id {
            let (tx, rx) = std::sync::mpsc::channel();
            let _ = self
                .tx
                .blocking_send(Task::CallWithContext(Box::new(move |ctx| {
                    let sharded = move |t: T| {
                        //println!("sharded called on {:?}", std::thread::current().id());
                        let rc = Rc::new(t);
                        let id = ctx.get_ctx().object_store.borrow_mut().insert(rc.clone());
                        ShardRc::new(id, rc, handle.clone())
                    };
                    let result = f(&sharded);
                    let _ = tx.send(result);
                })));
            rx.recv().expect("failed to receive result from event loop")
        } else {
            let ctx = self.ctx.lock().unwrap();
            let ctx = ctx
                .as_ref()
                .expect("Shard context not initialized")
                .get_ctx();
            let sharded = move |t: T| {
                //println!("sharded called on {:?}", std::thread::current().id());
                let rc = Rc::new(t);
                let id = ctx.object_store.borrow_mut().insert(rc.clone());
                ShardRc::new(id, rc, handle.clone())
            };
            f(&sharded)
        }
    }

    // fn bind<T>(&self, t: T) -> Erc<T>
    // where
    //     T: EventTarget,
    // {
    //     Erc { arc: Arc::new(t) }
    // }
}

#[macro_export]
macro_rules! shard_tokio {
    ($name:ident) => {
        pub static $name: ::std::sync::LazyLock<::eventful_rs::tokio::TokioShard> =
            ::std::sync::LazyLock::new(|| ::eventful_rs::tokio::TokioShard::new());
        use ::eventful_rs::tokio::TokioShard as DefaultShardType;
        use ::eventful_rs::tokio::TokioShardHandle as DefaultShardHandleType;
        fn default_shard() -> &'static ::eventful_rs::tokio::TokioShard {
            &$name
        }
    };
}
