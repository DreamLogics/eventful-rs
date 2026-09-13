use std::{any::Any, collections::HashMap, rc::Rc, sync::Arc};

use crate::{EventLoopHandle, Eventful, HasEvents};

pub(crate) trait ShardHandleInternal<T>
where
    T: Eventful + HasEvents<T::EventSetType> + Sized + 'static,
{
    fn id(&self) -> usize;
}

pub trait ShardHandle<T>: ShardHandleInternal<T> + Clone + Send + Sync + 'static
where
    T: Eventful + HasEvents<T::EventSetType> + Sized + 'static,
{
    fn upgrade_in_shard<F>(&self, task: F)
    where
        F: FnOnce(&T) + Send + 'static;

    fn upgrade_in_shard_async<F>(&self, task: F)
    where
        F: AsyncFnOnce(&T) -> () + Send + 'static;

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

#[derive(Debug, Clone)]
pub struct ShardRc<T>
where
    T: Eventful + HasEvents<T::EventSetType> + Sized + 'static,
{
    id: ShardRcId,
    inner: Rc<T>,
    shard_handle: T::EventLoopHandleType,
    pub events: Arc<T::EventSetType>,
}

impl<T> ShardRc<T>
where
    T: Eventful + HasEvents<T::EventSetType> + Sized + 'static,
{
    pub(crate) fn new(id: ShardRcId, value: Rc<T>, shard_handle: T::EventLoopHandleType) -> Self {
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
    shard_handle: T::EventLoopHandleType,
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
            events: self.events.clone(),
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
    shard_handle: T::EventLoopHandleType,
    pub events: Arc<T::EventSetType>,
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
    objects: HashMap<usize, (Rc<dyn Any>, ShardRcId)>,
    last_id: usize,
}

impl ShardRcStore {
    pub fn new() -> Self {
        Self {
            objects: HashMap::new(),
            last_id: 0,
        }
    }

    pub fn insert<T>(&mut self, object: Rc<T>) -> ShardRcId
    where
        T: Eventful + HasEvents<T::EventSetType> + Sized + 'static,
    {
        self.last_id += 1;
        let id = self.last_id;
        let sid = ShardRcId { id: Arc::new(id) };
        self.objects.insert(id, (object, sid.clone()));
        sid
    }

    pub fn get<T>(&self, id: usize) -> Option<Rc<T>>
    where
        T: Eventful + HasEvents<T::EventSetType> + Sized + 'static,
    {
        self.objects
            .get(&id)
            .and_then(|(obj, _)| obj.clone().downcast::<T>().ok())
    }

    pub fn remove(&mut self, id: usize) {
        self.objects.remove(&id);
    }

    pub fn garbage_collect(&mut self) {
        let ids_to_remove: Vec<usize> = self
            .objects
            .iter()
            .filter_map(|(&id, (_, sid))| {
                if Arc::strong_count(&sid.id) == 1 {
                    Some(id)
                } else {
                    None
                }
            })
            .collect();

        for id in ids_to_remove {
            self.objects.remove(&id);
        }
    }
}
