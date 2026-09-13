use std::cell::RefCell;
use std::collections::HashMap;
use std::panic::{AssertUnwindSafe, catch_unwind};

use std::rc::Rc;
use std::{
    sync::{Arc, Mutex, mpsc},
    thread,
};

use futures::channel::oneshot;

use crate::guarded_refcell::GuardedRefCell;
use crate::shard_futures::poll_future;
use crate::{
    EventLoop, EventLoopHandle, Eventful, FutureId, HasEvents, LocalFuture, ShardId, ShardRc,
    ShardRcStore, Task,
};

#[derive(Clone)]
pub struct LocalShardHandle {
    sender: mpsc::Sender<Task<LocalShard>>,
    shard_id: ShardId,
}

impl EventLoopHandle for LocalShardHandle {
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
            if let Some(obj) = ctx.object_store.borrow_mut().get(id) {
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
                if let Some(obj) = ctx.object_store.borrow_mut().get::<T>(id) {
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
        let _ = self
            .sender
            .send(Task::CallWithContextAsync(Box::new(move |_ctx| {
                Box::pin(async move {
                    f.await;
                })
            })));
    }
}

pub struct LocalShard {
    handle: LocalShardHandle,
    receiver: Mutex<mpsc::Receiver<Task<LocalShard>>>,
    pub shard_id: ShardId,
    thread_id: thread::ThreadId,
    object_store: GuardedRefCell<ShardRcStore>,
}

impl LocalShard {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel::<Task<LocalShard>>();
        let shard_id = ShardId::new();

        Self {
            handle: LocalShardHandle { sender, shard_id },
            receiver: Mutex::new(receiver),
            shard_id,
            thread_id: thread::current().id(),
            object_store: GuardedRefCell::new(ShardRcStore::new()),
        }
    }

    pub fn run_event_loop(&self) {
        let receiver = self.receiver.lock().unwrap();
        let sender = self.handle.sender.clone();

        let mut futures: HashMap<FutureId, LocalFuture<'_>> = HashMap::new();
        let mut next_future_id: FutureId = 0;

        while let Ok(task) = receiver.recv() {
            match task {
                Task::Call(f) => {
                    let _ = catch_unwind(AssertUnwindSafe(f));
                }

                Task::CallWithContext(f) => {
                    let _ = catch_unwind(AssertUnwindSafe(|| f(self)));
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

                    futures.insert(id, f(self));

                    poll_future(id, &mut futures, &sender);
                }

                Task::Wake(id) => {
                    poll_future(id, &mut futures, &sender);
                }
                Task::Stop => break,
            }
            self.object_store.borrow_mut().garbage_collect();
        }
        drop(futures);
    }

    pub fn exit(&self) {
        let _ = self.handle.sender.send(Task::Stop);
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
        self.handle.clone()
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
            let sharded = move |t: T| {
                //println!("sharded called on {:?}", std::thread::current().id());
                let rc = Rc::new(t);
                let id = self.object_store.borrow_mut().insert(rc.clone());
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
macro_rules! shard_local {
    ($name:ident) => {
        pub static $name: ::std::sync::LazyLock<::eventful_rs::local::LocalShard> =
            ::std::sync::LazyLock::new(|| ::eventful_rs::local::LocalShard::new());
        use ::eventful_rs::local::LocalShard as DefaultShardType;
        use ::eventful_rs::local::LocalShardHandle as DefaultShardHandleType;
        fn default_shard() -> &'static ::eventful_rs::local::LocalShard {
            &$name
        }
    };
}
