use std::panic::{AssertUnwindSafe, catch_unwind};

use std::rc::Rc;
use std::{
    sync::{Arc, Mutex, mpsc},
    thread,
};

use crate::{EventLoop, EventLoopHandle, EventTarget, HasEvents, Task};

#[derive(Clone)]
pub struct LocalShardHandle {
    sender: mpsc::Sender<Task>,
}

impl EventLoopHandle for LocalShardHandle {
    fn invoke<F>(&self, task: F)
    // -> Result<(), PostError>
    where
        F: FnOnce() + Send + 'static,
    {
        let _ = self.sender.send(Task::Call(Box::new(task))); //.map_err(|_| PostError)
    }

    fn invoke_async<F, R>(&self, _f: F)
    where
        F: FnOnce() -> R + Send + 'static,
        R: std::future::Future<Output = ()> + Send + 'static,
    {
        panic!("invoke_async is not supported in LocalShard");
    }
}

pub struct LocalShard {
    handle: LocalShardHandle,
    receiver: Mutex<mpsc::Receiver<Task>>,
}

impl LocalShard {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel::<Task>();

        Self {
            handle: LocalShardHandle { sender },
            receiver: Mutex::new(receiver),
        }
    }

    pub fn run_event_loop(&self) {
        let receiver = self.receiver.lock().unwrap();
        while let Ok(task) = receiver.recv() {
            match task {
                Task::Call(f) => {
                    let _ = catch_unwind(AssertUnwindSafe(f));
                }
                Task::Stop => break,
            }
        }
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

    fn asynchronize<T, S>(&self, t: &std::rc::Rc<T>) -> crate::ShardHandle<T, Self::HandleType, S>
    where
        T: HasEvents<S> + ?Sized + 'static,
        S: ?Sized + Send + 'static,
    {
        crate::ShardHandle::new(t, self.handle.clone())
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
        fn default_shard() -> &'static ::eventful_rs::local::LocalShard {
            &$name
        }
    };
}
