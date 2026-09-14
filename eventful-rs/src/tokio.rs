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
    EventLoop, EventLoopHandle, Eventful, HasEvents, ShardId, ShardRc, ShardRcStore, Task,
    guarded_refcell::GuardedRefCell,
};

#[derive(Clone)]
pub struct TokioShardHandle {
    tokio_rt: Handle,
    shard_id: ShardId,
    tx: tokio::sync::mpsc::Sender<Task<TokioShardCtx>>,
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
                if let Some(obj) = ctx.object_store.borrow().get(id) {
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
                if let Some(obj) = ctx.object_store.borrow().get::<T>(id) {
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

    fn spawn<F>(&self, f: F)
    where
        F: Future + Send + 'static,
    {
        self.tokio_rt.spawn(async move {
            f.await;
        });
    }
}

pub struct TokioShard {
    name: String,
    rt: Arc<Mutex<Option<Handle>>>,
    pub shard_id: ShardId,
    thread_id: std::thread::ThreadId,
    join_handle: Mutex<Option<std::thread::JoinHandle<()>>>,
    tx: tokio::sync::mpsc::Sender<Task<TokioShardCtx>>,
    ctx: Arc<TokioShardCtx>,
}

struct TokioShardCtx {
    object_store: GuardedRefCell<ShardRcStore>,
}

impl TokioShard {
    pub fn new(name: &str) -> Self {
        let shard_id = ShardId::new();
        let (tx, rx) = tokio::sync::mpsc::channel::<Task<TokioShardCtx>>(Semaphore::MAX_PERMITS);
        let rt_handle: Arc<Mutex<Option<Handle>>> = Arc::new(Mutex::new(None));
        let rt_handle_clone = Arc::clone(&rt_handle);
        let ctx_handle: Arc<TokioShardCtx> = Arc::new(TokioShardCtx {
            object_store: GuardedRefCell::new(ShardRcStore::new()),
        });
        let ctx_handle_clone = ctx_handle.clone();
        let join_handle = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            {
                rt_handle_clone.lock().unwrap().replace(rt.handle().clone());
            }
            rt.block_on(async {
                let mut rx = rx;
                let ctx = ctx_handle_clone;

                while let Some(task) = rx.recv().await {
                    match task {
                        Task::Call(f) => {
                            let _ = catch_unwind(AssertUnwindSafe(f));
                        }
                        Task::CallWithContext(f) => {
                            let _ = catch_unwind(AssertUnwindSafe(|| f(&ctx)));
                        }
                        Task::CallAsync(f) => {
                            f().await;
                        }
                        Task::CallWithContextAsync(f) => {
                            f(&ctx).await;
                        }
                        Task::Wake(_) => {}
                        Task::Stop => break,
                    }
                }
            });
        });
        let thread_id = join_handle.thread().id();

        // wait for the runtime to be initialized
        while rt_handle.lock().unwrap().is_none() {
            std::thread::yield_now();
        }

        Self {
            name: name.to_string(),
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

    fn bind<F, R, T>(&self, f: F) -> R
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
                        let id = ctx.object_store.borrow_mut().insert(rc.clone());
                        ShardRc::new(id, rc, handle.clone())
                    };
                    let result = f(&sharded);
                    let _ = tx.send(result);
                })));
            rx.recv().expect("failed to receive result from event loop")
        } else {
            let ctx = self.ctx.clone();
            let sharded = move |t: T| {
                //println!("sharded called on {:?}", std::thread::current().id());
                let rc = Rc::new(t);
                let id = ctx.object_store.borrow_mut().insert(rc.clone());
                ShardRc::new(id, rc, handle.clone())
            };
            f(&sharded)
        }
    }

    fn join(&self) -> Result<(), crate::ShardError> {
        assert!(
            std::thread::current().id() != self.thread_id,
            "Cannot join the shard from its own thread"
        );
        if let Some(join_handle) = self.join_handle.lock().unwrap().take() {
            self.tx.blocking_send(Task::Stop).map_err(|e| {
                crate::ShardError::JoinError(
                    format!("Failed to send stop task on tokio shard {}", &self.name),
                    Some(Box::new(e)),
                )
            })?;
            join_handle.join().map_err(|_| {
                crate::ShardError::JoinError(
                    format!("Failed to join tokio shard {}", &self.name),
                    None,
                )
            })?;
        }
        Ok(())
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
            ::std::sync::LazyLock::new(|| ::eventful_rs::tokio::TokioShard::new(stringify!($name)));
        use ::eventful_rs::tokio::TokioShard as DefaultShardType;
        use ::eventful_rs::tokio::TokioShardHandle as DefaultShardHandleType;
        fn default_shard() -> &'static ::eventful_rs::tokio::TokioShard {
            &$name
        }
    };
}
