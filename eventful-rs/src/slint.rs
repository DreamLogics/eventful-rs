use std::{rc::Rc, sync::Arc};

use crate::{EventLoop, EventLoopHandle, EventTarget, HasEvents};

#[derive(Clone)]
pub struct SlintShardHandle;

impl EventLoopHandle for SlintShardHandle {
    fn invoke<F>(&self, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        slint::invoke_from_event_loop(task).unwrap();
    }

    fn invoke_async<F, R>(&self, f: F)
    where
        F: FnOnce() -> R + Send + 'static,
        R: std::future::Future<Output = ()> + Send + 'static,
    {
        slint::invoke_from_event_loop(move || {
            drop(slint::spawn_local(async move {
                f().await;
            }));
        })
        .unwrap();
    }
}

pub struct SlintShard {
    handle: SlintShardHandle,
}

impl SlintShard {
    pub fn new() -> Self {
        Self {
            handle: SlintShardHandle {},
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
macro_rules! shard_slint {
    ($name:ident) => {
        pub static $name: ::std::sync::LazyLock<::eventful_rs::slint::SlintShard> =
            ::std::sync::LazyLock::new(|| ::eventful_rs::tokio::SlintShard::new());
        fn default_shard() -> &'static ::eventful_rs::slint::SlintShard {
            &$name
        }
    };
}
