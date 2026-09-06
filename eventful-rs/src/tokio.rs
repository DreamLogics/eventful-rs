use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Mutex, Weak},
};
use tokio::runtime::{Handle, Runtime};
use tokio::sync::Semaphore;

use crate::{EventLoop, EventLoopHandle, EventTarget, EventTargetRef, Task};

#[derive(Clone)]
pub struct TokioShardHandle {
    sender: tokio::sync::mpsc::Sender<Task>,
    tokio_rt: Handle,
}

impl EventLoopHandle for TokioShardHandle {
    fn post<F>(&self, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        // let _ = self.sender.send(Task::Call(Box::new(task)));
        let sender = self.sender.clone();
        drop(self.tokio_rt.spawn(async move {
            let _ = sender.send(Task::Call(Box::new(task))).await;
        }));
    }
}

pub struct TokioShard {
    name: String,
    handle: TokioShardHandle,
    rt: Mutex<Option<Runtime>>,
}

impl TokioShard {
    pub fn new(name: String) -> Self {
        let (sender, mut receiver) = tokio::sync::mpsc::channel::<Task>(Semaphore::MAX_PERMITS);

        let rt = Runtime::new().unwrap();

        rt.spawn(async move {
            while let Some(task) = receiver.recv().await {
                match task {
                    Task::Call(f) => {
                        let _ = catch_unwind(AssertUnwindSafe(f));
                    }
                    Task::Stop => break,
                }
            }
        });

        Self {
            name,
            handle: TokioShardHandle {
                sender,
                tokio_rt: rt.handle().clone(),
            },
            rt: Mutex::new(Some(rt)),
        }
    }

    pub fn join(&self) {
        if let Some(_rt) = self.rt.lock().unwrap().take() {
            loop {
                match self.handle.sender.try_send(Task::Stop) {
                    Ok(_) => break,
                    Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                        std::thread::yield_now();
                    }
                    Err(e) => {
                        eprintln!("Error sending stop task for shard {}: {:?}", self.name, e);
                        break;
                    }
                }
            }
        }
    }
}

impl EventLoop for TokioShard {
    fn handle(&self) -> impl EventLoopHandle {
        self.handle.clone()
    }

    fn bind<T>(&self, t: T) -> impl EventTargetRef<T>
    where
        T: EventTarget,
    {
        let handle = self.rt.lock().unwrap().as_ref().unwrap().handle().clone();
        TokioEvr {
            arc: Arc::new(t),
            tokio_rt: handle,
        }
    }
}

#[derive(Clone)]
pub struct TokioEvr<T>
where
    T: EventTarget,
{
    arc: Arc<T>,
    tokio_rt: Handle,
}

impl<T> TokioEvr<T>
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

    pub fn invoke_async<F, R>(&self, f: F)
    where
        F: FnOnce(&T) -> R + Send + 'static,
        R: std::future::Future<Output = ()> + Send + 'static,
    {
        let arc = self.arc.clone();
        self.tokio_rt.spawn(f(&arc));
    }
}

impl<T> EventTargetRef<T> for TokioEvr<T>
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

#[macro_export]
macro_rules! shard_tokio {
    ($name:ident) => {
        static $name: LazyLock<TokioShard> = LazyLock::new(|| TokioShard::new("$name"));
    };
}
