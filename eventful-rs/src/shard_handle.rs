use std::{any::Any, collections::HashMap, rc::Rc, sync::Arc};

use crate::{EventLoopHandle, Eventful, HasEvents};

mod joined;
pub use joined::JoinedHandles;

#[doc(hidden)]
pub trait ShardHandleInternal<T>
where
    T: Eventful + HasEvents<T::EventSetType> + Sized + 'static,
{
    fn id(&self) -> usize;
    fn shard_id(&self) -> crate::ShardId;
}

/// Dispatch work to a value's shard through a thread-safe handle.
///
/// The callbacks receive local references on the value's owner thread. They can
/// call ordinary methods that have no generated handle wrapper.
#[allow(async_fn_in_trait)]
pub trait ShardHandle<T>: ShardHandleInternal<T> + Clone + Send + Sync + 'static
where
    T: Eventful + HasEvents<T::EventSetType> + Sized + 'static,
{
    /// Queue a callback with a reference to a shard-local value and return without waiting.
    ///
    /// Usable from synchronous code. The reference stays on the value's shard;
    /// a missing target or stopped built-in shard silently skips the callback.
    fn upgrade_in_shard<F>(&self, task: F)
    where
        F: FnOnce(&T) + Send + 'static;

    /// Queue an async callback without requiring an async caller.
    ///
    /// The shard awaits the callback; other jobs may run while it is suspended.
    fn upgrade_in_shard_async<F>(&self, task: F)
    where
        F: AsyncFnOnce(&T) -> () + Send + 'static;

    /// Await a callback's result while it runs on the value's shard.
    ///
    /// Submission starts when this future is polled. The built-in backend panics
    /// if the result cannot be delivered. Untracked events emitted by the callback
    /// may still be pending when this returns.
    async fn deferred_upgrade_in_shard<F, R>(&self, task: F) -> R
    where
        F: AsyncFnOnce(&T) -> R + Send + 'static,
        R: Send + 'static;

    fn downgrade(&self) -> ShardWeakHandle<T>
    where
        T: Eventful + HasEvents<T::EventSetType> + Sized + 'static;
}

pub trait Sharded<T>
where
    T: Eventful + HasEvents<T::EventSetType> + Sized + 'static,
{
    fn as_handle(&self) -> impl ShardHandle<T>;
}

#[derive(Debug)]
pub struct ShardRc<T>
where
    T: Eventful + HasEvents<T::EventSetType> + Sized + 'static,
{
    id: ShardRcId,
    inner: Rc<T>,
    shard_handle: crate::ShardEventHandle,
    pub events: Arc<T::EventSetType>,
}

impl<T> ShardRc<T>
where
    T: Eventful + HasEvents<T::EventSetType> + Sized + 'static,
{
    pub(crate) fn new(id: ShardRcId, value: Rc<T>, shard_handle: crate::ShardEventHandle) -> Self {
        let events = value.events().clone();
        Self {
            id,
            inner: value,
            shard_handle,
            events,
        }
    }

    pub fn as_handle(&self) -> ShardRcHandle<T> {
        ShardRcHandle {
            id: self.id.clone(),
            shard_handle: self.shard_handle.clone(),
            events: self.events.clone(),
        }
    }

    pub fn events(&self) -> &Arc<T::EventSetType> {
        &self.events
    }
}

impl<T> Sharded<T> for ShardRc<T>
where
    T: Eventful + HasEvents<T::EventSetType> + Sized + 'static,
{
    fn as_handle(&self) -> impl ShardHandle<T> {
        self.as_handle()
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

#[derive(Debug)]
pub struct ShardRcHandle<T>
where
    T: Eventful + HasEvents<T::EventSetType> + Sized + 'static,
{
    id: ShardRcId,
    shard_handle: crate::ShardEventHandle,
    pub events: Arc<T::EventSetType>,
}

impl<T> Clone for ShardRcHandle<T>
where
    T: Eventful + HasEvents<T::EventSetType> + Sized + 'static,
{
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            shard_handle: self.shard_handle.clone(),
            events: self.events.clone(),
        }
    }
}

impl<T> ShardHandleInternal<T> for ShardRcHandle<T>
where
    T: Eventful + HasEvents<T::EventSetType> + Sized + 'static,
{
    fn id(&self) -> usize {
        *self.id.id
    }
    fn shard_id(&self) -> crate::ShardId {
        self.shard_handle.shard_id()
    }
}

impl<T> ShardHandle<T> for ShardRcHandle<T>
where
    T: Eventful + HasEvents<T::EventSetType> + Sized + 'static,
{
    fn upgrade_in_shard<F>(&self, task: F)
    where
        F: FnOnce(&T) + Send + 'static,
    {
        self.shard_handle.invoke_with_handle(self.clone(), task);
    }

    fn upgrade_in_shard_async<F>(&self, task: F)
    where
        F: AsyncFnOnce(&T) -> () + Send + 'static,
    {
        self.shard_handle
            .invoke_with_handle_async(self.clone(), task);
    }

    async fn deferred_upgrade_in_shard<F, R>(&self, task: F) -> R
    where
        F: AsyncFnOnce(&T) -> R + Send + 'static,
        R: Send + 'static,
    {
        self.shard_handle.deferred_invoke(self.clone(), task).await
    }

    fn downgrade(&self) -> ShardWeakHandle<T>
    where
        T: Eventful + HasEvents<<T as Eventful>::EventSetType> + Sized + 'static,
    {
        ShardWeakHandle {
            id: *self.id.id,
            shard_handle: self.shard_handle.clone(),
            events: Arc::downgrade(&self.events),
        }
    }
}

impl<T> Sharded<T> for ShardRcHandle<T>
where
    T: Eventful + HasEvents<T::EventSetType> + Sized + 'static,
{
    fn as_handle(&self) -> impl ShardHandle<T> {
        self.clone()
    }
}

#[derive(Debug)]
pub struct ShardWeakHandle<T>
where
    T: Eventful + HasEvents<T::EventSetType> + Sized + 'static,
{
    id: usize,
    shard_handle: crate::ShardEventHandle,
    pub events: std::sync::Weak<T::EventSetType>,
}

impl<T> Clone for ShardWeakHandle<T>
where
    T: Eventful + HasEvents<T::EventSetType> + Sized + 'static,
{
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            shard_handle: self.shard_handle.clone(),
            events: self.events.clone(),
        }
    }
}

impl<T> ShardHandleInternal<T> for ShardWeakHandle<T>
where
    T: Eventful + HasEvents<T::EventSetType> + Sized + 'static,
{
    fn id(&self) -> usize {
        self.id
    }
    fn shard_id(&self) -> crate::ShardId {
        self.shard_handle.shard_id()
    }
}

impl<T> ShardWeakHandle<T>
where
    T: Eventful + HasEvents<T::EventSetType> + Sized + 'static,
{
    /// Submit immediately and observe completion, including delivery failures.
    pub fn try_deferred_upgrade_in_shard<F, R>(
        &self,
        task: F,
    ) -> futures::future::BoxFuture<'static, Result<R, crate::InvokeError>>
    where
        F: AsyncFnOnce(&T) -> R + Send + 'static,
        R: Send + 'static,
    {
        Box::pin(self.shard_handle.try_deferred_invoke(self.clone(), task))
    }
}

impl<T> ShardHandle<T> for ShardWeakHandle<T>
where
    T: Eventful + HasEvents<T::EventSetType> + Sized + 'static,
{
    fn upgrade_in_shard<F>(&self, task: F)
    where
        F: FnOnce(&T) + Send + 'static,
    {
        self.shard_handle.invoke_with_handle(self.clone(), task);
    }

    fn upgrade_in_shard_async<F>(&self, task: F)
    where
        F: AsyncFnOnce(&T) -> () + Send + 'static,
    {
        self.shard_handle
            .invoke_with_handle_async(self.clone(), task);
    }

    async fn deferred_upgrade_in_shard<F, R>(&self, task: F) -> R
    where
        F: AsyncFnOnce(&T) -> R + Send + 'static,
        R: Send + 'static,
    {
        self.shard_handle.deferred_invoke(self.clone(), task).await
    }

    fn downgrade(&self) -> ShardWeakHandle<T>
    where
        T: Eventful + HasEvents<<T as Eventful>::EventSetType> + Sized + 'static,
    {
        self.clone()
    }
}

impl<T> Sharded<T> for ShardWeakHandle<T>
where
    T: Eventful + HasEvents<T::EventSetType> + Sized + 'static,
{
    fn as_handle(&self) -> impl ShardHandle<T> {
        self.clone()
    }
}

#[derive(Debug, Clone)]
pub struct ShardRcId {
    id: Arc<usize>,
}

pub struct ShardRcStore {
    values: HashMap<usize, (Rc<dyn Any>, ShardRcId)>,
    last_id: usize,
}

impl ShardRcStore {
    pub fn new() -> Self {
        Self {
            values: HashMap::new(),
            last_id: 0,
        }
    }

    pub fn insert<T>(&mut self, value: Rc<T>) -> ShardRcId
    where
        T: Eventful + HasEvents<T::EventSetType> + Sized + 'static,
    {
        self.last_id += 1;
        let id = self.last_id;
        let sid = ShardRcId { id: Arc::new(id) };
        self.values.insert(id, (value, sid.clone()));
        sid
    }

    pub fn get<T>(&self, id: usize) -> Option<Rc<T>>
    where
        T: Eventful + HasEvents<T::EventSetType> + Sized + 'static,
    {
        self.values
            .get(&id)
            .and_then(|(value, _)| value.clone().downcast::<T>().ok())
    }

    pub fn remove(&mut self, id: usize) {
        self.values.remove(&id);
    }

    pub(crate) fn take_garbage(&mut self) -> Vec<Rc<dyn Any>> {
        let ids_to_remove: Vec<usize> = self
            .values
            .iter()
            .filter_map(|(&id, (value, sid))| {
                if Arc::strong_count(&sid.id) == 1 && Rc::strong_count(value) == 1 {
                    Some(id)
                } else {
                    None
                }
            })
            .collect();

        ids_to_remove
            .into_iter()
            .filter_map(|id| self.values.remove(&id).map(|(value, _)| value))
            .collect()
    }
}

impl Default for ShardRcStore {
    fn default() -> Self {
        Self::new()
    }
}
impl<T> Clone for ShardRc<T>
where
    T: Eventful + HasEvents<T::EventSetType> + 'static,
{
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            inner: self.inner.clone(),
            shard_handle: self.shard_handle.clone(),
            events: self.events.clone(),
        }
    }
}

impl<T> From<T> for ShardRc<T>
where
    T: Eventful + HasEvents<T::EventSetType> + 'static,
    T::Shard: crate::ShardBinding,
{
    fn from(value: T) -> Self {
        let handle = <T::Shard as crate::ShardBinding>::handle();
        if crate::engine::has_context(handle.shard_id) {
            crate::engine::bind_here(&crate::engine::store(handle.shard_id), handle, |bind| {
                bind(value)
            })
        } else {
            panic!("may only create ShardRc within the same shard that T was declared in");
        }
    }
}

impl<T> From<T> for ShardRcHandle<T>
where
    T: Eventful + HasEvents<T::EventSetType> + Send + 'static,
    T::Shard: crate::ShardBinding,
{
    fn from(value: T) -> Self {
        let handle = <T::Shard as crate::ShardBinding>::handle();
        if crate::engine::has_context(handle.shard_id) {
            crate::engine::bind_here(&crate::engine::store(handle.shard_id), handle, |bind| {
                bind(value).as_handle()
            })
        } else {
            crate::engine::assert_not_async(
                "conversion to shard handle blocks; use bind_async from Tokio",
            );
            handle
                .bind_blocking_factory(|bind| bind(value).as_handle())
                .expect("binding failed")
        }
    }
}
impl<T> HasEvents<T::EventSetType> for ShardRc<T>
where
    T: Eventful + HasEvents<T::EventSetType> + 'static,
{
    fn events(&self) -> &Arc<T::EventSetType> {
        &self.events
    }
}
impl<T> HasEvents<T::EventSetType> for ShardRcHandle<T>
where
    T: Eventful + HasEvents<T::EventSetType> + 'static,
{
    fn events(&self) -> &Arc<T::EventSetType> {
        &self.events
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    struct Value {
        events: Arc<()>,
    }
    impl Eventful for Value {
        type EventSetType = ();
        type Shard = crate::DynamicShard;
    }
    impl HasEvents<()> for Value {
        fn events(&self) -> &Arc<()> {
            &self.events
        }
    }
    #[test]
    fn collection_preserves_local_references_and_strong_handle_tokens() {
        let mut store = ShardRcStore::new();
        let value = Rc::new(Value {
            events: Arc::new(()),
        });
        let token = store.insert(value.clone());
        assert!(store.take_garbage().is_empty());
        drop(token);
        assert!(store.take_garbage().is_empty());
        drop(value);
        assert_eq!(store.take_garbage().len(), 1);
        assert!(store.take_garbage().is_empty());
    }
    #[test]
    fn weak_handle_does_not_keep_event_set_alive() {
        let (handle, _rx) = crate::ShardEventHandle::channel();
        let mut store = ShardRcStore::new();
        let value = Rc::new(Value {
            events: Arc::new(()),
        });
        let id = store.insert(value.clone());
        let local = ShardRc::new(id, value, handle);
        let weak = local.as_handle().downgrade();
        let events = Arc::downgrade(&local.events);
        drop(local);
        drop(store.take_garbage());
        assert!(events.upgrade().is_none());
        assert!(weak.events.upgrade().is_none());
    }
}
