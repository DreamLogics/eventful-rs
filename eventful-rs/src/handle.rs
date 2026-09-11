use std::{any::Any, collections::HashMap, rc::Rc};

pub struct ShardObjectStore {
    next_id: usize,
    objects: HashMap<usize, Rc<dyn Any>>,
}

pub struct ShardHandle<T, H>
where
    T: ?Sized + 'static,
    H: EventLoopHandle,
{
    id: usize,
    shard_handle: H,
    _marker: std::marker::PhantomData<T>,
}

pub struct ShardRc<T, H>
where
    T: ?Sized + 'static,
    H: EventLoopHandle,
{
    id: usize,
    shard_handle: H,
    _marker: std::marker::PhantomData<T>,
}

// use std::{
//     default,
//     mem::ManuallyDrop,
//     rc::{Rc, Weak},
//     sync::Arc,
//     thread::{self, ThreadId},
// };

// use crate::{EventLoopHandle, HasEvents};

// struct WeakWrapper<T, H>
// where
//     T: ?Sized + 'static,
//     H: EventLoopHandle,
// {
//     weak: Option<ManuallyDrop<Weak<T>>>,
//     shard_handle: H,
//     thread_id: ThreadId,
// }

// impl<T, H> WeakWrapper<T, H>
// where
//     T: ?Sized + 'static,
//     H: EventLoopHandle,
// {
//     fn new(weak: Weak<T>, shard_handle: H) -> Self {
//         Self {
//             weak: Some(ManuallyDrop::new(weak)),
//             shard_handle,
//             thread_id: thread::current().id(),
//         }
//     }

//     fn upgrade(&self) -> Option<Rc<T>> {
//         assert_eq!(thread::current().id(), self.thread_id);
//         self.weak.as_ref().and_then(|w| w.upgrade())
//     }
// }

// impl<T, H> Drop for WeakWrapper<T, H>
// where
//     T: ?Sized + 'static,
//     H: EventLoopHandle,
// {
//     fn drop(&mut self) {
//         let Some(w) = self.weak.take() else {
//             return;
//         };

//         let t = WeakWrapper {
//             weak: Some(w),
//             shard_handle: self.shard_handle.clone(),
//             thread_id: self.thread_id,
//         };

//         self.shard_handle.invoke(move || {
//             assert_eq!(thread::current().id(), t.thread_id);

//             let mut t = t;

//             if let Some(mut w) = t.weak.take() {
//                 unsafe {
//                     ManuallyDrop::drop(&mut w);
//                 }
//             }
//         });
//     }
// }

// unsafe impl<T, H> Send for WeakWrapper<T, H>
// where
//     T: ?Sized + 'static,
//     H: EventLoopHandle,
// {
// }

// unsafe impl<T, H> Sync for WeakWrapper<T, H>
// where
//     T: ?Sized + 'static,
//     H: EventLoopHandle,
// {
// }

// pub struct ShardHandle<T, H, S>
// where
//     T: ?Sized + 'static,
//     H: EventLoopHandle,
//     S: ?Sized + Send + 'static,
// {
//     object: Arc<WeakWrapper<T, H>>,
//     shard_handle: H,
//     events: Arc<S>,
// }

// impl<T, H, S> ShardHandle<T, H, S>
// where
//     T: HasEvents<S> + ?Sized + 'static,
//     H: EventLoopHandle,
//     S: ?Sized + Send + 'static,
// {
//     pub(crate) fn new(object: &Rc<T>, shard_handle: H) -> Self {
//         Self {
//             object: Arc::new(WeakWrapper::new(
//                 Rc::downgrade(object),
//                 shard_handle.clone(),
//             )),
//             shard_handle,
//             events: object.events().clone(),
//         }
//     }

//     pub fn in_shard<F>(&self, f: F)
//     where
//         F: FnOnce(&T) + Send + 'static,
//     {
//         let handle = self.clone();
//         self.shard_handle.invoke(move || {
//             handle.invoke(f);
//         });
//     }

//     fn invoke<F>(&self, f: F)
//     where
//         F: FnOnce(&T) + Send + 'static,
//     {
//         if let Some(object) = self.object.upgrade() {
//             f(&object);
//         }
//     }

//     pub fn events(&self) -> &Arc<S> {
//         &self.events
//     }
// }

// impl<T, H, S> Clone for ShardHandle<T, H, S>
// where
//     T: ?Sized + 'static,
//     H: EventLoopHandle,
//     S: ?Sized + Send + 'static,
// {
//     fn clone(&self) -> Self {
//         Self {
//             object: self.object.clone(),
//             shard_handle: self.shard_handle.clone(),
//             events: self.events.clone(),
//         }
//     }
// }

// unsafe impl<T, H, S> Send for ShardHandle<T, H, S>
// where
//     T: ?Sized + 'static,
//     H: EventLoopHandle,
//     S: ?Sized + Send + 'static,
// {
// }

// unsafe impl<T, H, S> Sync for ShardHandle<T, H, S>
// where
//     T: ?Sized + 'static,
//     H: EventLoopHandle,
//     S: ?Sized + Send + 'static,
// {
// }
