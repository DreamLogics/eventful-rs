use std::{cell::RefCell, rc::Rc, sync::Arc};

use futures::channel::oneshot;

use crate::{
    EventLoop, EventLoopHandle, Eventful, HasEvents, ShardId, ShardRc, ShardRcStore,
    guarded_refcell::GuardedRefCell,
};

struct SlintShardCtx {
    object_store: Arc<GuardedRefCell<ShardRcStore>>,
}

#[derive(Clone)]
pub struct SlintShardHandle {
    shard_id: ShardId,
    ctx: Arc<SlintShardCtx>,
}

impl EventLoopHandle for SlintShardHandle {
    fn shard_id(&self) -> ShardId {
        self.shard_id
    }
    fn invoke<F>(&self, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        slint::invoke_from_event_loop(task).unwrap();
    }

    fn invoke_async<F>(&self, f: F)
    where
        F: AsyncFnOnce() -> () + Send + 'static,
    {
        slint::invoke_from_event_loop(move || {
            drop(slint::spawn_local(async move {
                f().await;
            }));
        })
        .unwrap();
    }

    fn invoke_with_handle<T, H, F>(&self, handle: H, f: F)
    where
        T: Eventful<EventLoopHandleType = Self> + HasEvents<T::EventSetType> + Sized + 'static,
        H: crate::ShardHandle<T>,
        F: FnOnce(&T) + Send + 'static,
    {
        let ctx = self.ctx.clone();
        slint::invoke_from_event_loop(move || {
            let id = handle.id();
            if let Some(obj) = ctx.object_store.borrow_mut().get(id) {
                f(&obj);
            }
        })
        .unwrap();
    }

    fn invoke_with_handle_async<T, H, F>(&self, handle: H, f: F)
    where
        T: Eventful<EventLoopHandleType = Self> + HasEvents<T::EventSetType> + Sized + 'static,
        H: crate::ShardHandle<T>,
        F: AsyncFnOnce(&T) -> () + Send + 'static,
    {
        let ctx = self.ctx.clone();
        slint::invoke_from_event_loop(move || {
            let id = handle.id();
            if let Some(obj) = ctx.object_store.borrow_mut().get(id) {
                drop(slint::spawn_local(async move {
                    f(&obj).await;
                }));
            }
        })
        .unwrap();
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
        slint::invoke_from_event_loop(move || {
            drop(slint::spawn_local(f));
        })
        .unwrap();
    }
}

pub struct SlintShard {
    handle: SlintShardHandle,
    pub shard_id: ShardId,
    thread_id: std::thread::ThreadId,
    ctx: Arc<SlintShardCtx>,
}

impl SlintShard {
    pub fn new() -> Self {
        let shard_id = ShardId::new();
        let ctx = Arc::new(SlintShardCtx {
            object_store: Arc::new(GuardedRefCell::new(ShardRcStore::new())),
        });
        Self {
            handle: SlintShardHandle {
                shard_id,
                ctx: ctx.clone(),
            },
            shard_id,
            thread_id: std::thread::current().id(),
            ctx,
        }
    }
}

impl Default for SlintShard {
    fn default() -> Self {
        Self::new()
    }
}

impl EventLoop for SlintShard {
    type HandleType = SlintShardHandle;

    fn handle(&self) -> Self::HandleType {
        self.handle.clone()
    }

    fn bind<F, R, T>(&'static self, f: F) -> R
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
            slint::invoke_from_event_loop(move || {
                let sharded = move |t: T| {
                    let rc = Rc::new(t);
                    let id = self.ctx.object_store.borrow_mut().insert(rc.clone());
                    ShardRc::new(id, rc, handle.clone())
                };
                tx.send(f(&sharded)).expect("failed to send result");
            })
            .expect("failed to invoke from event loop");
            rx.recv().expect("failed to receive result from event loop")
        } else {
            let sharded = move |t: T| {
                //println!("sharded called on {:?}", std::thread::current().id());
                let rc = Rc::new(t);
                let id = self.ctx.object_store.borrow_mut().insert(rc.clone());
                ShardRc::new(id, rc, handle.clone())
            };
            f(&sharded)
        }
    }

    fn join(&self) -> Result<(), crate::ShardError> {
        // No-op for SlintShard, as it runs in the main thread and cannot be joined.
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
macro_rules! shard_slint {
    ($name:ident) => {
        pub static $name: ::std::sync::LazyLock<::eventful_rs::slint::SlintShard> =
            ::std::sync::LazyLock::new(|| ::eventful_rs::tokio::SlintShard::new());
        use ::eventful_rs::slint::Slint as DefaultShardType;
        use ::eventful_rs::slint::SlintHandle as DefaultShardHandleType;
        fn default_shard() -> &'static ::eventful_rs::slint::SlintShard {
            &$name
        }
    };
}
