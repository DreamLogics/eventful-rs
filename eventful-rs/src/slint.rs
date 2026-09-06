use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, mpsc},
    time::Duration,
};

use crate::{EventLoop, EventLoopHandle, EventTarget, EventTargetRef, Evr, Task};

#[derive(Clone)]
pub struct SlintShardHandle {
    sender: mpsc::Sender<Task>,
}

impl EventLoopHandle for SlintShardHandle {
    fn post<F>(&self, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let _ = self.sender.send(Task::Call(Box::new(task)));
    }
}

pub struct SlintShard {
    handle: SlintShardHandle,
    _timer: slint::Timer,
}

impl SlintShard {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel();

        let timer = slint::Timer::default();
        timer.start(
            slint::TimerMode::Repeated,
            Duration::from_millis(10),
            move || {
                while let Ok(task) = receiver.try_recv() {
                    match task {
                        Task::Call(task) => {
                            let _ = catch_unwind(AssertUnwindSafe(task));
                        }
                        Task::Stop => (),
                    }
                }
            },
        );

        Self {
            handle: SlintShardHandle { sender },
            _timer: timer,
        }
    }
}

impl Default for SlintShard {
    fn default() -> Self {
        Self::new()
    }
}

impl EventLoop for SlintShard {
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

#[macro_export]
macro_rules! shard_slint {
    ($name:ident) => {
        static $name: LazyLock<SlintShard> = LazyLock::new(|| SlintShard::new());
    };
}
