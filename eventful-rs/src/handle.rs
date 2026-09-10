use std::{
    mem::ManuallyDrop,
    rc::{Rc, Weak},
    sync::Arc,
    thread::{self, ThreadId},
};

use crate::EventLoopHandle;

struct WeakWrapper<T, H>
where
    T: ?Sized + 'static,
    H: EventLoopHandle,
{
    weak: Option<ManuallyDrop<Weak<T>>>,
    shard_handle: H,
    thread_id: ThreadId,
}

impl<T, H> WeakWrapper<T, H>
where
    T: ?Sized + 'static,
    H: EventLoopHandle,
{
    fn new(weak: Weak<T>, shard_handle: H) -> Self {
        Self {
            weak: Some(ManuallyDrop::new(weak)),
            shard_handle,
            thread_id: thread::current().id(),
        }
    }

    fn upgrade(&self) -> Option<Rc<T>> {
        assert_eq!(thread::current().id(), self.thread_id);
        self.weak.as_ref().and_then(|w| w.upgrade())
    }
}

impl<T, H> Drop for WeakWrapper<T, H>
where
    T: ?Sized + 'static,
    H: EventLoopHandle,
{
    fn drop(&mut self) {
        let Some(w) = self.weak.take() else {
            return;
        };

        let t = WeakWrapper {
            weak: Some(w),
            shard_handle: self.shard_handle.clone(),
            thread_id: self.thread_id,
        };

        self.shard_handle.invoke(move || {
            assert_eq!(thread::current().id(), t.thread_id);

            let mut t = t;

            if let Some(mut w) = t.weak.take() {
                unsafe {
                    ManuallyDrop::drop(&mut w);
                }
            }
        });
    }
}

unsafe impl<T, H> Send for WeakWrapper<T, H>
where
    T: ?Sized + 'static,
    H: EventLoopHandle,
{
}

pub struct ShardHandle<T, H>
where
    T: ?Sized + 'static,
    H: EventLoopHandle,
{
    object: Arc<WeakWrapper<T, H>>,
    shard_handle: H,
}

impl<T, H> ShardHandle<T, H>
where
    T: ?Sized + 'static,
    H: EventLoopHandle,
{
    pub(crate) fn new(object: Weak<T>, shard_handle: H) -> Self {
        Self {
            object: Arc::new(WeakWrapper::new(object, shard_handle.clone())),
            shard_handle,
        }
    }

    pub fn in_shard<F>(&self, f: F)
    where
        F: FnOnce(&T) + Send + 'static,
    {
        let handle = self.clone();
        self.shard_handle.invoke(move || {
            handle.invoke(f);
        });
    }

    fn invoke<F>(&self, f: F)
    where
        F: FnOnce(&T) + Send + 'static,
    {
        if let Some(object) = self.object.upgrade() {
            f(&object);
        }
    }
}

impl<T, H> Clone for ShardHandle<T, H>
where
    T: ?Sized + 'static,
    H: EventLoopHandle,
{
    fn clone(&self) -> Self {
        Self {
            object: self.object.clone(),
            shard_handle: self.shard_handle.clone(),
        }
    }
}

unsafe impl<T, H> Send for ShardHandle<T, H>
where
    T: ?Sized + 'static,
    H: EventLoopHandle,
{
}
