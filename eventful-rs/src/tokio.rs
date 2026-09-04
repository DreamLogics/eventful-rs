use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Weak},
};
use tokio::runtime::{Handle, Runtime};
use tokio::sync::Semaphore;

use crate::{EventLoop, EventLoopHandle, EventTarget, EventTargetRef, Task};

#[derive(Clone)]
pub struct TokioShardHandle {
    sender: tokio::sync::mpsc::Sender<Task>,
}

impl EventLoopHandle for TokioShardHandle {
    fn post<F>(&self, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let _ = self.sender.send(Box::new(task));
    }
}

pub struct TokioShard {
    handle: TokioShardHandle,
    rt: Runtime,
}

impl TokioShard {
    pub fn new() -> Self {
        let (sender, mut receiver) = tokio::sync::mpsc::channel::<Task>(Semaphore::MAX_PERMITS);

        let rt = Runtime::new().unwrap();

        rt.spawn(async move {
            while let Some(task) = receiver.recv().await {
                let _ = catch_unwind(AssertUnwindSafe(task));
            }
        });

        Self {
            handle: TokioShardHandle { sender },
            rt,
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
        TokioEvr {
            arc: Arc::new(t),
            tokio_rt: self.rt.handle().clone(),
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
