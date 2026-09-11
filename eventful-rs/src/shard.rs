use std::panic::{AssertUnwindSafe, catch_unwind};

use std::rc::Rc;
use std::{
    sync::{Arc, Mutex, mpsc},
    thread,
};

use crate::{EventLoop, EventLoopHandle, EventTarget, HasEvents, Task};

#[derive(Clone)]
pub struct ShardEventHandle {
    sender: mpsc::Sender<Task>,
}

impl EventLoopHandle for ShardEventHandle {
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
        panic!("invoke_async is not supported in Shard");
    }
}

pub struct Shard {
    handle: ShardEventHandle,
    join_handle: Mutex<Option<thread::JoinHandle<()>>>,
}

impl Shard {
    pub fn new(thread_name: &str) -> Self {
        let (sender, receiver) = mpsc::channel::<Task>();
        let join_handle = thread::Builder::new()
            .name(thread_name.into())
            .spawn(move || {
                while let Ok(task) = receiver.recv() {
                    match task {
                        Task::Call(f) => {
                            let _ = catch_unwind(AssertUnwindSafe(f));
                        }
                        Task::Stop => break,
                    }
                }
            })
            .expect("failed to spawn event-loop thread");

        Self {
            handle: ShardEventHandle { sender },
            join_handle: Mutex::new(Some(join_handle)),
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
            ::std::sync::LazyLock::new(|| ::eventful_rs::shard::Shard::new("$name"));
        type DefaultShardType = ::eventful_rs::shard::Shard;
        fn default_shard() -> &'static ::eventful_rs::shard::Shard {
            &$name
        }
    };
}
