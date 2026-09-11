pub use eventful_rs_macros::{action, asynchronize, eventful, events};

mod shard_futures;

mod handle;
pub use handle::*;

pub mod local;

pub mod shard;

#[cfg(feature = "tokio")]
pub mod tokio;

#[cfg(feature = "slint")]
pub mod slint;

use std::{
    rc::Rc,
    sync::{Arc, Mutex, Weak},
};

// type Task = Box<dyn FnOnce() + Send + 'static>;

enum Task {
    Call(Box<dyn FnOnce() + Send + 'static>),
    Stop,
}

pub trait EventLoopHandle: Clone + Send + Sync + 'static {
    fn invoke<F>(&self, task: F)
    where
        F: FnOnce() + Send + 'static;
    fn invoke_async<F, R>(&self, f: F)
    where
        F: FnOnce() -> R + Send + 'static,
        R: std::future::Future<Output = ()> + Send + 'static;
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
    type EventSetType: ?Sized + Send + 'static;
    type EventsTraitType: ?Sized + Send + 'static;
    type EventLoopHandleType: EventLoopHandle;
}

pub trait HasEvents<E>
where
    E: ?Sized + Send + 'static,
{
    fn events(&self) -> &Arc<E>;
}

// #[derive(Debug, Clone, Copy, PartialEq, Eq)]
// pub struct PostError;

pub trait EventLoop {
    type HandleType: EventLoopHandle;

    fn handle(&self) -> Self::HandleType;
    // fn bind<T>(&self, t: T) -> Erc<T>
    // where
    //     T: EventTarget;
}

/// Implemented by `#[eventful(...)]` to declare object affinity.
pub trait EventTarget: Send + Sync + 'static {
    fn event_loop(&self) -> impl EventLoopHandle;
}

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

#[macro_export]
macro_rules! sharded {
    ($e:expr) => {
        ::eventful_rs::ShardRc::new($e, default_shard().handle())
    };
}
