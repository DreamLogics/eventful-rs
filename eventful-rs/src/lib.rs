pub use eventful_rs_macros::{accept_events, events, with_events};

#[cfg(feature = "tokio")]
pub mod tokio;

#[cfg(feature = "slint")]
pub mod slint;

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex, Weak, mpsc};
use std::thread;

type Task = Box<dyn FnOnce() + Send + 'static>;

pub trait EventLoopHandle: Clone + Send + Sync + 'static {
    fn post<F>(&self, task: F)
    where
        F: FnOnce() + Send + 'static;
}

#[derive(Clone)]
pub struct ShardHandle {
    sender: mpsc::Sender<Task>,
}

impl EventLoopHandle for ShardHandle {
    fn post<F>(&self, task: F)
    // -> Result<(), PostError>
    where
        F: FnOnce() + Send + 'static,
    {
        let _ = self.sender.send(Box::new(task)); //.map_err(|_| PostError)
    }
}

// #[derive(Debug, Clone, Copy, PartialEq, Eq)]
// pub struct PostError;

pub trait EventLoop {
    fn handle(&self) -> impl EventLoopHandle;
    fn bind<T>(&self, t: T) -> impl EventTargetRef<T>
    where
        T: EventTarget;
}

pub struct Shard {
    handle: ShardHandle,
    join_handle: thread::JoinHandle<()>,
}

impl Shard {
    pub fn new(thread_name: impl Into<String>) -> Self {
        let (sender, receiver) = mpsc::channel::<Task>();
        let join_handle = thread::Builder::new()
            .name(thread_name.into())
            .spawn(move || {
                while let Ok(task) = receiver.recv() {
                    let _ = catch_unwind(AssertUnwindSafe(task));
                }
            })
            .expect("failed to spawn event-loop thread");

        Self {
            handle: ShardHandle { sender },
            join_handle,
        }
    }

    pub fn join(self) {
        drop(self.handle);
        let _ = self.join_handle.join();
    }
}

impl EventLoop for Shard {
    fn handle(&self) -> impl EventLoopHandle {
        self.handle.clone()
    }

    fn bind<T>(&self, t: T) -> impl EventTargetRef<T>
    where
        T: EventTarget,
    {
        Evr { arc: Arc::new(t) }
    }
}

/// Implemented by `#[accept_events(...)]` to declare object affinity.
pub trait EventTarget: Send + Sync + 'static {
    fn event_loop(&self) -> impl EventLoopHandle;
}

/// Type-erased connection storage for one signal signature.
pub struct Event<Args> {
    connections: Mutex<Vec<Arc<dyn Fn(Args) + Send + Sync + 'static>>>,
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

pub trait EventTargetRef<T>
where
    T: EventTarget,
{
    fn weak(&self) -> Weak<T>;
    fn event_loop(&self) -> impl EventLoopHandle;
}

#[derive(Clone)]
pub struct Evr<T>
where
    T: EventTarget,
{
    arc: Arc<T>,
}

impl<T> Evr<T>
where
    T: EventTarget,
{
    pub fn invoke<F>(&self, f: F)
    where
        F: FnOnce(&T) + Send + 'static,
    {
        let arc = self.arc.clone();
        arc.event_loop().post(move || {
            f(&arc);
        });
    }
}

impl<T> EventTargetRef<T> for Evr<T>
where
    T: EventTarget,
{
    fn weak(&self) -> Weak<T> {
        Arc::downgrade(&self.arc)
    }

    fn event_loop(&self) -> impl EventLoopHandle {
        self.arc.event_loop()
    }
}
