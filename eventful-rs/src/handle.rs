use std::{
    default,
    mem::ManuallyDrop,
    rc::{Rc, Weak},
    sync::Arc,
    thread::{self, ThreadId},
};

use crate::{EventLoopHandle, Eventful, HasEvents};

pub trait Sharded<T>
where
    T: Eventful + HasEvents<T::EventSetType> + Sized + 'static,
{
    fn downgrade(&self) -> ShardWeakHandle<T>;
    //fn events(&self) -> &Arc<T::EventSetType>;
}

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

struct StrongWrapper<T, H>
where
    T: ?Sized + 'static,
    H: EventLoopHandle,
{
    strong: Option<ManuallyDrop<Rc<T>>>,
    shard_handle: H,
    thread_id: ThreadId,
}

impl<T, H> StrongWrapper<T, H>
where
    T: ?Sized + 'static,
    H: EventLoopHandle,
{
    fn new(strong: Rc<T>, shard_handle: H) -> Self {
        Self {
            strong: Some(ManuallyDrop::new(strong)),
            shard_handle,
            thread_id: thread::current().id(),
        }
    }

    fn upgrade(&self) -> Option<Rc<T>> {
        assert_eq!(thread::current().id(), self.thread_id);
        self.strong.as_ref().map(|w| Rc::clone(w))
    }
}

impl<T, H> Drop for StrongWrapper<T, H>
where
    T: ?Sized + 'static,
    H: EventLoopHandle,
{
    fn drop(&mut self) {
        let Some(w) = self.strong.take() else {
            return;
        };

        let t = StrongWrapper {
            strong: Some(w),
            shard_handle: self.shard_handle.clone(),
            thread_id: self.thread_id,
        };

        self.shard_handle.invoke(move || {
            assert_eq!(thread::current().id(), t.thread_id);

            let mut t = t;

            if let Some(mut w) = t.strong.take() {
                unsafe {
                    ManuallyDrop::drop(&mut w);
                }
            }
        });
    }
}

unsafe impl<T, H> Send for StrongWrapper<T, H>
where
    T: ?Sized + 'static,
    H: EventLoopHandle,
{
}

unsafe impl<T, H> Sync for StrongWrapper<T, H>
where
    T: ?Sized + 'static,
    H: EventLoopHandle,
{
}

/// A weak handle to an object that is bound to a specific event loop (shard).
pub struct ShardWeakHandle<T>
where
    T: Eventful + HasEvents<T::EventSetType> + Sized + 'static,
{
    object: Arc<WeakWrapper<T, T::EventLoopHandleType>>,
    shard_handle: T::EventLoopHandleType,
    events: Arc<T::EventSetType>,
}

impl<T> ShardWeakHandle<T>
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

impl<T> Clone for ShardWeakHandle<T>
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

unsafe impl<T> Send for ShardWeakHandle<T> where
    T: Eventful + HasEvents<T::EventSetType> + Sized + 'static
{
}

unsafe impl<T> Sync for ShardWeakHandle<T> where
    T: Eventful + HasEvents<T::EventSetType> + Sized + 'static
{
}

impl<T> Sharded<T> for ShardWeakHandle<T>
where
    T: Eventful + HasEvents<T::EventSetType> + Sized + 'static,
{
    fn downgrade(&self) -> ShardWeakHandle<T> {
        self.clone()
    }
}

/// A strong handle to an object that is bound to a specific event loop (shard).
pub struct ShardRcHandle<T>
where
    T: Eventful + HasEvents<T::EventSetType> + Sized + 'static,
{
    object: Arc<StrongWrapper<T, T::EventLoopHandleType>>,
    shard_handle: T::EventLoopHandleType,
    events: Arc<T::EventSetType>,
    thread_id: ThreadId,
    weak_handle: ShardWeakHandle<T>,
}

impl<T> ShardRcHandle<T>
where
    T: Eventful + HasEvents<T::EventSetType> + Sized + 'static,
{
    pub(crate) fn new(object: &Rc<T>, shard_handle: T::EventLoopHandleType) -> Self {
        Self {
            weak_handle: ShardWeakHandle::new(object, shard_handle.clone()),
            object: Arc::new(StrongWrapper::new(object.clone(), shard_handle.clone())),
            shard_handle,
            events: object.events().clone(),
            thread_id: thread::current().id(),
        }
    }

    pub fn in_shard<F>(&self, f: F)
    where
        F: FnOnce(&T) + Send + 'static,
    {
        let handle = self.clone();
        let thread_id = self.thread_id;
        self.shard_handle.invoke(move || {
            println!(
                "in_shard invoke on {:?} from shard {:?}",
                std::thread::current().id(),
                thread_id
            );
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

impl<T> Clone for ShardRcHandle<T>
where
    T: Eventful + HasEvents<T::EventSetType> + Sized + 'static,
{
    fn clone(&self) -> Self {
        Self {
            object: self.object.clone(),
            shard_handle: self.shard_handle.clone(),
            events: self.events.clone(),
            thread_id: self.thread_id,
            weak_handle: self.weak_handle.clone(),
        }
    }
}

unsafe impl<T> Send for ShardRcHandle<T> where
    T: Eventful + HasEvents<T::EventSetType> + Sized + 'static
{
}

unsafe impl<T> Sync for ShardRcHandle<T> where
    T: Eventful + HasEvents<T::EventSetType> + Sized + 'static
{
}

impl<T> Sharded<T> for ShardRcHandle<T>
where
    T: Eventful + HasEvents<T::EventSetType> + Sized + 'static,
{
    fn downgrade(&self) -> ShardWeakHandle<T> {
        self.weak_handle.clone()
    }
}

pub struct ShardRc<T>
where
    T: Eventful + HasEvents<T::EventSetType> + Sized + 'static,
{
    inner: Rc<T>,
    shard_handle: T::EventLoopHandleType,
    events: Arc<T::EventSetType>,
    rc_handle: ShardRcHandle<T>,
}

impl<T> ShardRc<T>
where
    T: Eventful + HasEvents<T::EventSetType> + Sized + 'static,
{
    pub(crate) fn new(value: T, shard_handle: T::EventLoopHandleType) -> Self {
        let events = value.events().clone();
        let rc = Rc::new(value);
        Self {
            events,
            rc_handle: ShardRcHandle::new(&rc, shard_handle.clone()),
            shard_handle,
            inner: rc,
        }
    }

    pub fn downgrade(&self) -> ShardWeakHandle<T> {
        self.rc_handle.downgrade()
    }

    pub fn as_handle(&self) -> ShardRcHandle<T> {
        self.rc_handle.clone()
    }

    pub fn events(&self) -> &Arc<T::EventSetType> {
        &self.events
    }
}

impl<T> Sharded<T> for ShardRc<T>
where
    T: Eventful + HasEvents<T::EventSetType> + Sized + 'static,
{
    fn downgrade(&self) -> ShardWeakHandle<T> {
        self.downgrade()
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
            events: self.events.clone(),
            rc_handle: self.rc_handle.clone(),
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
