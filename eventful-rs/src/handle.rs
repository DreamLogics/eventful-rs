use std::{
    default,
    mem::ManuallyDrop,
    rc::{Rc, Weak},
    sync::Arc,
    thread::{self, ThreadId},
};

use crate::{EventLoopHandle, Eventful, HasEvents};

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

unsafe impl<T, H> Sync for WeakWrapper<T, H>
where
    T: ?Sized + 'static,
    H: EventLoopHandle,
{
}

pub struct ShardHandle<T>
where
    T: Eventful + HasEvents<T::EventSetType> + Sized + 'static,
{
    object: Arc<WeakWrapper<T, T::EventLoopHandleType>>,
    shard_handle: T::EventLoopHandleType,
    events: Arc<T::EventSetType>,
}

impl<T> ShardHandle<T>
where
    T: Eventful + HasEvents<T::EventSetType> + Sized + 'static,
{
    pub(crate) fn new(object: &Rc<T>, shard_handle: T::EventLoopHandleType) -> Self {
        Self {
            object: Arc::new(WeakWrapper::new(
                Rc::downgrade(object),
                shard_handle.clone(),
            )),
            shard_handle,
            events: object.events().clone(),
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

    pub fn events(&self) -> &Arc<T::EventSetType> {
        &self.events
    }
}

impl<T> Clone for ShardHandle<T>
where
    T: Eventful + HasEvents<T::EventSetType> + Sized + 'static,
{
    fn clone(&self) -> Self {
        Self {
            object: self.object.clone(),
            shard_handle: self.shard_handle.clone(),
            events: self.events.clone(),
        }
    }
}

unsafe impl<T> Send for ShardHandle<T> where
    T: Eventful + HasEvents<T::EventSetType> + Sized + 'static
{
}

unsafe impl<T> Sync for ShardHandle<T> where
    T: Eventful + HasEvents<T::EventSetType> + Sized + 'static
{
}

pub struct ShardRc<T>
where
    T: Eventful + HasEvents<T::EventSetType> + Sized + 'static,
{
    inner: Rc<T>,
    shard_handle: T::EventLoopHandleType,
}

impl<T> ShardRc<T>
where
    T: Eventful + HasEvents<T::EventSetType> + Sized + 'static,
{
    pub fn new(value: T, shard_handle: T::EventLoopHandleType) -> Self {
        Self {
            inner: Rc::new(value),
            shard_handle,
        }
    }

    pub fn asynchronize(&self) -> ShardHandle<T> {
        ShardHandle::new(&self.inner, self.shard_handle.clone())
    }
}

impl<T> Clone for ShardRc<T>
where
    T: Eventful + HasEvents<T::EventSetType> + Sized + 'static,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            shard_handle: self.shard_handle.clone(),
        }
    }
}

impl<T> std::ops::Deref for ShardRc<T>
where
    T: Eventful + HasEvents<T::EventSetType> + Sized + 'static,
{
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
