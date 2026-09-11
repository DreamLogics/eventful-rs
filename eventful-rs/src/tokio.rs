use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    rc::Rc,
    sync::{Arc, Mutex},
};
use tokio::runtime::{Handle, Runtime};

use crate::{EventLoop, EventLoopHandle, EventTarget, HasEvents};

#[derive(Clone)]
pub struct TokioShardHandle {
    tokio_rt: Handle,
}

impl EventLoopHandle for TokioShardHandle {
    fn invoke<F>(&self, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        drop(self.tokio_rt.spawn(async move {
            let _ = catch_unwind(AssertUnwindSafe(task));
        }));
    }

    fn invoke_async<F, R>(&self, f: F)
    where
        F: FnOnce() -> R + Send + 'static,
        R: std::future::Future<Output = ()> + Send + 'static,
    {
        drop(self.tokio_rt.spawn(async move {
            f().await;
        }));
    }

    // fn invoke_and_then<F, C, R>(&self, task: F, callback: C)
    // where
    //     F: FnOnce() -> R + Send + 'static,
    //     C: FnOnce(R) + Send + 'static,
    //     R: Send + 'static,
    // {
    //     drop(self.tokio_rt.spawn(async move {
    //         //let _ = catch_unwind(AssertUnwindSafe(task));
    //         callback(task());
    //     }));
    // }

    // fn invoke_async_and_then<F, R, C, T>(&self, f: F, callback: C)
    // where
    //     F: FnOnce() -> R + Send + 'static,
    //     R: std::future::Future<Output = T> + Send + 'static,
    //     T: Send + 'static,
    //     C: FnOnce(T) + Send + 'static,
    // {
    //     drop(self.tokio_rt.spawn(async move {
    //         callback(f().await);
    //     }));
    // }
}

pub struct TokioShard {
    handle: TokioShardHandle,
    rt: Mutex<Option<Runtime>>,
}

impl TokioShard {
    pub fn new() -> Self {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();

        Self {
            handle: TokioShardHandle {
                tokio_rt: rt.handle().clone(),
            },
            rt: Mutex::new(Some(rt)),
        }
    }

    pub fn run<F, R>(&self, main_fn: F) -> R::Output
    where
        F: FnOnce() -> R + Send + 'static,
        R: Future,
    {
        self.handle.tokio_rt.block_on(main_fn())
    }

    pub fn join(&self) {
        if let Some(_rt) = self.rt.lock().unwrap().take() {}
    }
}

impl Default for TokioShard {
    fn default() -> Self {
        Self::new()
    }
}

impl EventLoop for TokioShard {
    type HandleType = TokioShardHandle;

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
macro_rules! shard_tokio {
    ($name:ident) => {
        pub static $name: ::std::sync::LazyLock<::eventful_rs::tokio::TokioShard> =
            ::std::sync::LazyLock::new(|| ::eventful_rs::tokio::TokioShard::new());
        fn default_shard() -> &'static ::eventful_rs::tokio::TokioShard {
            &$name
        }
    };
}
