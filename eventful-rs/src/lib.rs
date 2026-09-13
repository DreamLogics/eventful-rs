pub use eventful_rs_macros::{action, asynced, asynchronize, eventful, events};

mod shard_futures;
// mod handle;
// pub use handle::*;
mod guarded_refcell;

mod shard_handle;
use futures::future::BoxFuture;
pub use shard_handle::*;

pub mod local;

pub mod shard;

#[cfg(feature = "tokio")]
pub mod tokio;

#[cfg(feature = "slint")]
pub mod slint;

use std::{
    pin::Pin,
    rc::Rc,
    sync::{Arc, Mutex, Weak},
};

// type Task = Box<dyn FnOnce() + Send + 'static>;
type FutureId = usize;
type LocalFuture<'a> = Pin<Box<dyn Future<Output = ()> + 'a>>;

enum Task<T> {
    Call(Box<dyn FnOnce() + Send + 'static>),

    CallAsync(Box<dyn FnOnce() -> LocalFuture<'static> + Send + 'static>),

    CallWithContext(Box<dyn FnOnce(&T) + Send + 'static>),

    CallWithContextAsync(Box<dyn for<'a> FnOnce(&'a T) -> LocalFuture<'a> + Send + 'static>),

    Wake(FutureId),

    Stop,
}

static LAST_SHARD_ID: Mutex<usize> = Mutex::new(0);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ShardId(pub usize);

impl ShardId {
    pub fn new() -> Self {
        let mut last_id = LAST_SHARD_ID.lock().unwrap();
        *last_id += 1;
        ShardId(*last_id)
    }
}

impl Default for ShardId {
    fn default() -> Self {
        ShardId::new()
    }
}

pub trait EventLoopHandle: Clone + Send + Sync + 'static {
    fn shard_id(&self) -> ShardId;
    fn invoke<F>(&self, task: F)
    where
        F: FnOnce() + Send + 'static;
    fn invoke_async<F>(&self, f: F)
    where
        F: AsyncFnOnce() -> () + Send + 'static;
    fn invoke_with_handle<T, H, F>(&self, handle: H, f: F)
    where
        T: Eventful<EventLoopHandleType = Self> + HasEvents<T::EventSetType> + Sized + 'static,
        H: ShardHandle<T>,
        F: FnOnce(&T) + Send + 'static;
    fn invoke_with_handle_async<T, H, F>(&self, handle: H, f: F)
    where
        T: Eventful<EventLoopHandleType = Self> + HasEvents<T::EventSetType> + Sized + 'static,
        H: ShardHandle<T>,
        F: AsyncFnOnce(&T) -> () + Send + 'static;

    fn deferred_invoke<T, H, F, R>(
        &self,
        handle: H,
        task: F,
    ) -> impl Future<Output = R> + Send + 'static
    where
        T: Eventful<EventLoopHandleType = Self> + HasEvents<T::EventSetType> + Sized + 'static,
        H: ShardHandle<T>,
        F: AsyncFnOnce(&T) -> R + Send + 'static,
        R: Send + 'static;

    fn spawn<F>(&self, f: F)
    where
        F: Future + Send + 'static;
    // fn invoke_and_then<F, C, R>(&self, task: F, callback: C)
    // where
    //     F: FnOnce() -> R + Send + 'static,
    //     C: FnOnce(R) + Send + 'static,
    //     R: Send + 'static;
    // fn invoke_async_and_then<F, R, C, T>(&self, f: F, callback: C)
    // where
    //     F: FnOnce() -> R + Send + 'static,
    //     R: std::future::Future<Output = T> + Send + 'static,
    //     T: Send + 'static,
    //     C: FnOnce(T) + Send + 'static;
}

pub trait Eventful {
    type EventSetType: ?Sized + Send + Sync + 'static;
    type EventLoopHandleType: EventLoopHandle;
}

pub trait HasEvents<E>
where
    E: ?Sized + Send + 'static,
{
    fn events(&self) -> &Arc<E>;
}

pub trait EventLoop {
    type HandleType: EventLoopHandle;
    fn handle(&self) -> Self::HandleType;
    /// Spawn objects bound to this event loop.
    fn bind<F, R, T>(&'static self, f: F) -> R
    where
        F: FnOnce(&dyn Fn(T) -> ShardRc<T>) -> R + Send + 'static,
        R: Send + 'static,
        T: Eventful<EventLoopHandleType = Self::HandleType>
            + HasEvents<T::EventSetType>
            + Sized
            + 'static;
    fn spawn<F>(&self, f: F)
    where
        F: Future + Send + 'static,
    {
        self.handle().spawn(f);
    }
}

// pub trait EventTarget: Send + Sync + 'static {
//     fn event_loop(&self) -> impl EventLoopHandle;
// }

type TaskFn<Args> = dyn Fn(Args) + Send + Sync + 'static;

/// Type-erased connection storage for one signal signature.
pub struct Event<Args> {
    connections: Mutex<Vec<Arc<TaskFn<Args>>>>,
}

impl<Args> Default for Event<Args> {
    fn default() -> Self {
        Self {
            connections: Mutex::new(Vec::new()),
        }
    }
}

impl<Args> Event<Args>
where
    Args: Clone + Send + 'static,
{
    pub fn add_connection<F>(&self, connection: F)
    where
        F: Fn(Args) + Send + Sync + 'static,
    {
        self.connections.lock().unwrap().push(Arc::new(connection));
    }

    pub fn emit(&self, args: Args) {
        // Never hold the mutex while user-defined connection code runs.
        let snapshot = self.connections.lock().unwrap().clone();
        for connection in snapshot {
            connection(args.clone());
        }
    }

    pub fn connection_count(&self) -> usize {
        self.connections.lock().unwrap().len()
    }
}

#[macro_export]
macro_rules! use_shard {
    ($name:path) => {
        fn default_shard() -> &'static impl ::eventful_rs::EventLoop {
            &$name
        }
    };
}
