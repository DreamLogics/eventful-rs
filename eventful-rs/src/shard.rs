use futures::channel::oneshot;
use std::cell::RefCell;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::rc::Rc;
use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    task::{Context, Poll, Wake, Waker},
};
use std::{
    sync::{Arc, Mutex, mpsc},
    thread,
};

use crate::shard_futures::poll_future;
use crate::{
    EventLoop, EventLoopHandle, EventTarget, Eventful, FutureId, HasEvents, LocalFuture, ShardId,
    ShardRc, ShardRcStore, Task,
};

#[derive(Clone)]
pub struct ShardEventHandle {
    sender: mpsc::Sender<Task<ShardCtx>>,
    thread_id: thread::ThreadId,
    pub shard_id: ShardId,
}

impl EventLoopHandle for ShardEventHandle {
    fn shard_id(&self) -> ShardId {
        self.shard_id
    }
    fn invoke<F>(&self, task: F)
    // -> Result<(), PostError>
    where
        F: FnOnce() + Send + 'static,
    {
        let _ = self.sender.send(Task::Call(Box::new(task))); //.map_err(|_| PostError)
    }

    fn invoke_async<F>(&self, task: F)
    where
        F: AsyncFnOnce() -> () + Send + 'static,
    {
        let _ = self
            .sender
            .send(Task::CallAsync(Box::new(move || Box::pin(task()))));
    }

    fn invoke_with_handle<T, H, F>(&self, handle: H, f: F)
    where
        T: Eventful<EventLoopHandleType = Self> + HasEvents<T::EventSetType> + Sized + 'static,
        H: crate::ShardHandle<T>,
        F: FnOnce(&T) + Send + 'static,
    {
        let _ = self.sender.send(Task::CallWithContext(Box::new(move |ctx| {
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
            .sender
            .send(Task::CallWithContextAsync(Box::new(move |ctx| {
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
}

struct ShardCtxHandle(*const ShardCtx);

pub struct Shard {
    handle: ShardEventHandle,
    join_handle: Mutex<Option<thread::JoinHandle<()>>>,
    pub shard_id: ShardId,
    thread_id: thread::ThreadId,
    ctx: Arc<Mutex<Option<ShardCtxHandle>>>,
}

struct ShardCtx {
    object_store: RefCell<ShardRcStore>,
}

unsafe impl Send for ShardCtxHandle {}

impl Shard {
    pub fn new(thread_name: &str) -> Self {
        let (sender, receiver) = mpsc::channel::<Task<ShardCtx>>();
        let shard_id = ShardId::new();
        let ctx_handle: Arc<Mutex<Option<ShardCtxHandle>>> = Arc::new(Mutex::new(None));
        let ctx_handle_clone = ctx_handle.clone();
        let sender_clone = sender.clone();
        let join_handle = thread::Builder::new()
            .name(thread_name.into())
            .spawn(move || {
                let sender = sender_clone;
                let ctx = ShardCtx {
                    object_store: RefCell::new(ShardRcStore::new()),
                };
                *ctx_handle_clone.lock().unwrap() = Some(ShardCtxHandle(&ctx as *const ShardCtx));
                let mut futures: HashMap<FutureId, LocalFuture<'_>> = HashMap::new();

                let mut next_future_id: FutureId = 0;

                while let Ok(task) = receiver.recv() {
                    match task {
                        Task::Call(f) => {
                            let _ = catch_unwind(AssertUnwindSafe(f));
                        }

                        Task::CallWithContext(f) => {
                            let _ = catch_unwind(AssertUnwindSafe(|| f(&ctx)));
                        }

                        Task::CallAsync(f) => {
                            let id = next_future_id;
                            next_future_id += 1;

                            futures.insert(id, f());

                            poll_future(id, &mut futures, &sender);
                        }

                        Task::CallWithContextAsync(f) => {
                            let id = next_future_id;
                            next_future_id += 1;

                            futures.insert(id, f(&ctx));

                            poll_future(id, &mut futures, &sender);
                        }

                        Task::Wake(id) => {
                            poll_future(id, &mut futures, &sender);
                        }

                        Task::Stop => break,
                    }
                    ctx.object_store.borrow_mut().garbage_collect();
                }
                drop(futures);
                *ctx_handle_clone.lock().unwrap() = None;
            })
            .expect("failed to spawn event-loop thread");
        let thread_id = join_handle.thread().id();

        Self {
            handle: ShardEventHandle {
                sender,
                shard_id,
                thread_id,
            },
            join_handle: Mutex::new(Some(join_handle)),
            shard_id,
            thread_id,
            ctx: ctx_handle,
        }
    }

    pub fn join(&self) {
        if let Some(join_handle) = self.join_handle.lock().unwrap().take() {
            let _ = self.handle.sender.send(Task::Stop);
            let _ = join_handle.join();
        }
    }
}

impl EventLoop for Shard {
    type HandleType = ShardEventHandle;

    fn handle(&self) -> Self::HandleType {
        self.handle.clone()
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
        if thread::current().id() != self.thread_id {
            let (tx, rx) = std::sync::mpsc::channel();
            let _ = self
                .handle
                .sender
                .send(Task::CallWithContext(Box::new(move |ctx| {
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
            let ctx = self
                .ctx
                .lock()
                .unwrap()
                .as_ref()
                .expect("Shard context is not initialized")
                .0;
            let ctx = unsafe { &*ctx };
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
macro_rules! shard_std {
    ($name:ident) => {
        pub static $name: ::std::sync::LazyLock<::eventful_rs::shard::Shard> =
            ::std::sync::LazyLock::new(|| ::eventful_rs::shard::Shard::new(stringify!($name)));
        use ::eventful_rs::shard::Shard as DefaultShardType;
        use ::eventful_rs::shard::ShardEventHandle as DefaultShardHandleType;
        fn default_shard() -> &'static ::eventful_rs::shard::Shard {
            &$name
        }
    };
}
