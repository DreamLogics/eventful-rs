//! Shard-local values and strong or weak cross-thread handles.
use std::{rc::Rc, sync::Arc};

use crate::{EventLoopHandle, Eventful, HasEvents};

mod store;
pub(crate) use store::{ShardRcId, ShardRcStore};
mod joined;
pub use joined::JoinedHandles;

mod sealed;
use sealed::Sealed;

/// Dispatch work to a shard-local value through a thread-safe handle.
/// See the [calling methods guide](crate#calling-methods).
///
/// The callbacks receive local references on the value's owner thread. They can
/// call ordinary methods that have no generated handle wrapper.
/// This trait is sealed: only the built-in strong and weak handles implement it.
#[allow(async_fn_in_trait)]
pub trait ShardHandle<T>: Sealed<T> + Clone + Send + Sync + 'static
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
    /// Submission starts when this future is polled. Untracked events emitted by
    /// the callback may still be pending when this returns.
    ///
    /// # Panics
    /// Panics if the result cannot be delivered; use the weak handle
    /// [`ShardWeakHandle::try_deferred_upgrade_in_shard`] for fallible dispatch.
    async fn deferred_upgrade_in_shard<F, R>(&self, task: F) -> R
    where
        F: AsyncFnOnce(&T) -> R + Send + 'static,
        R: Send + 'static;

    /// Make a handle that does not keep the target value alive.
    fn downgrade(&self) -> ShardWeakHandle<T>
    where
        T: Eventful + HasEvents<T::EventSetType> + Sized + 'static;
}

/// Obtain a dispatch handle from a local reference or remote handle.
/// See the [`crate::DynamicShard`] construction example.
pub trait Sharded<T>
where
    T: Eventful + HasEvents<T::EventSetType> + Sized + 'static,
{
    /// Concrete owned dispatch handle.
    type Handle: ShardHandle<T>;
    /// Clone a handle suitable for cross-thread dispatch.
    fn to_handle(&self) -> Self::Handle;
}

// Keep lifetime-bound subscriptions in the same Rc allocation as the value.
// Store entries, local clones, and in-flight callbacks all retain this allocation.
// Connections drop before T, including during shard shutdown.
/// Keep value-owned connections in the same allocation as the value.
pub(crate) struct ShardValue<T> {
    /// Subscriptions dropped before the value, including during shutdown.
    connections: std::cell::RefCell<Vec<crate::ScopedConnectionGroup>>,
    /// The shard-local application value.
    value: T,
}

impl<T> ShardValue<T> {
    /// Store an eventful value with its owned subscriptions.
    pub(crate) fn new(value: T) -> Self {
        Self {
            connections: Default::default(),
            value,
        }
    }
}

impl<T> std::ops::Deref for ShardValue<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.value
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for ShardValue<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.value.fmt(f)
    }
}

/// Strong reference to a shard-local value. This reference cannot cross threads.
/// See the [`crate::DynamicShard`] construction example.
/// Dereferences to the value; use [`Sharded::to_handle`] for remote access.
pub struct ShardRc<T>
where
    T: Eventful + HasEvents<T::EventSetType> + Sized + 'static,
{
    /// Store key and strong lifetime token for the local value.
    id: ShardRcId,
    /// Keep value-owned connections in the same allocation as the value.
    inner: Rc<ShardValue<T>>,
    /// Admission handle for the value owner.
    shard_handle: crate::ShardEventHandle,
    /// Shared typed signal interface. Keeping it alive does not retain the value.
    events: Arc<T::EventSetType>,
}

impl<T> ShardRc<T>
where
    T: Eventful + HasEvents<T::EventSetType> + Sized + 'static,
{
    /// Wrap a stored value with its identity, shard handle, and events.
    pub(crate) fn new(
        id: ShardRcId,
        value: Rc<ShardValue<T>>,
        shard_handle: crate::ShardEventHandle,
    ) -> Self {
        let events = value.events().clone();
        Self {
            id,
            inner: value,
            shard_handle,
            events,
        }
    }
}

impl<T> Sharded<T> for ShardRc<T>
where
    T: Eventful + HasEvents<T::EventSetType> + 'static,
{
    type Handle = ShardRcHandle<T>;
    fn to_handle(&self) -> Self::Handle {
        ShardRcHandle {
            id: self.id.clone(),
            shard_handle: self.shard_handle.clone(),
            events: self.events.clone(),
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

/// Strong thread-safe handle retaining a shard-local value while its shard runs.
/// See the [quick start](crate#quick-start) and [`Self::join`] example.
/// Shutdown destroys the store even when handles remain alive.
pub struct ShardRcHandle<T>
where
    T: Eventful + HasEvents<T::EventSetType> + Sized + 'static,
{
    /// Store key and strong lifetime token for the local value.
    id: ShardRcId,
    /// Admission handle for the value owner.
    shard_handle: crate::ShardEventHandle,
    /// Shared typed signal interface. Keeping it alive does not retain the value.
    events: Arc<T::EventSetType>,
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

impl<T> Sealed<T> for ShardRcHandle<T>
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

    /// Make a handle that does not keep the target value alive.
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
    type Handle = Self;
    fn to_handle(&self) -> Self::Handle {
        self.clone()
    }
}

/// Thread-safe handle that does not retain its target or event storage.
/// See the [calling methods guide](crate#calling-methods).
/// Untracked calls skip expired targets; tracked calls report delivery failures.
pub struct ShardWeakHandle<T>
where
    T: Eventful + HasEvents<T::EventSetType> + Sized + 'static,
{
    /// Stable key identifying this entry within its owner storage.
    id: usize,
    /// Admission handle for the value owner.
    shard_handle: crate::ShardEventHandle,
    /// Weak reference to the typed signal interface.
    events: std::sync::Weak<T::EventSetType>,
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

impl<T> Sealed<T> for ShardWeakHandle<T>
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
    /// Upgrade the weak event-storage reference without retaining the target value.
    pub fn events(&self) -> Option<Arc<T::EventSetType>> {
        self.events.upgrade()
    }

    /// Submit immediately and observe completion, including delivery failures.
    ///
    /// # Errors
    /// Returns [`crate::InvokeError`] for a closed shard, expired or mismatched
    /// target, canceled callback, or an unwinding panic in the callback.
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

    /// Make a handle that does not keep the target value alive.
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
    type Handle = Self;
    fn to_handle(&self) -> Self::Handle {
        self.clone()
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

impl<T> ShardRc<T>
where
    T: Eventful + HasEvents<T::EventSetType> + 'static,
    T::Shard: crate::ShardBinding,
{
    /// Bind an eventful value in its named shard's current execution context.
    ///
    /// # Errors
    /// Returns [`crate::InvokeError::WrongShard`] outside that context.
    /// Use [`Eventful::spawn`] to construct from another thread.
    ///
    /// # Panics
    /// A custom shard binding may panic while obtaining its singleton handle.
    pub fn try_bind(value: T) -> Result<Self, crate::InvokeError> {
        let handle = <T::Shard as crate::ShardBinding>::handle();
        if !crate::engine::has_context(handle.shard_id) {
            return Err(crate::InvokeError::WrongShard);
        }
        Ok(crate::engine::bind_here(
            &crate::engine::store(handle.shard_id),
            handle,
            |bind| bind(value),
        ))
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

impl<T> ShardRc<T>
where
    T: Eventful + HasEvents<T::EventSetType> + 'static,
{
    /// Connect every event in this source's interface to a compatible listener.
    /// The underlying value owns these subscriptions and disconnects them when
    /// it is destroyed. Local clones and strong handles keep that value alive
    /// while its shard runs. Dropping the returned token does not disconnect;
    /// use disconnect() or scoped() for earlier cleanup.
    /// Registration is per signal, not atomic.
    pub fn connect<U, S>(this: &Self, target: &S) -> crate::ConnectionGroup
    where
        U: Eventful + HasEvents<U::EventSetType> + 'static,
        S: Sharded<U>,
        T::EventSetType: crate::ConnectEvents<U>,
    {
        let group = crate::ConnectEvents::connect_events(&*this.events, target);
        this.inner
            .connections
            .borrow_mut()
            .push(group.clone().scoped());
        group
    }
}

impl<T> ShardRcHandle<T>
where
    T: Eventful + HasEvents<T::EventSetType> + 'static,
{
    /// Connect every event in this source's interface to a compatible listener.
    /// Dropping the returned group keeps subscriptions active; use disconnect()
    /// or scoped() to remove them. Registration is per signal, not atomic.
    pub fn connect<U, S>(&self, target: &S) -> crate::ConnectionGroup
    where
        U: Eventful + HasEvents<U::EventSetType> + 'static,
        S: Sharded<U>,
        T::EventSetType: crate::ConnectEvents<U>,
    {
        crate::ConnectEvents::connect_events(&*self.events, target)
    }
}

impl<T> ShardWeakHandle<T>
where
    T: Eventful + HasEvents<T::EventSetType> + 'static,
{
    /// Connect all events if the source's event set still exists.
    /// Returns None for an expired event set. Targets remain weak.
    pub fn connect<U, S>(&self, target: &S) -> Option<crate::ConnectionGroup>
    where
        U: Eventful + HasEvents<U::EventSetType> + 'static,
        S: Sharded<U>,
        T::EventSetType: crate::ConnectEvents<U>,
    {
        let events = self.events.upgrade()?;
        Some(crate::ConnectEvents::connect_events(&*events, target))
    }
}

#[cfg(test)]
mod tests;

impl<T> std::fmt::Debug for ShardRc<T>
where
    T: Eventful + HasEvents<T::EventSetType> + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardRc")
            .field("id", &self.id.id)
            .field("shard", &self.shard_handle.shard_id())
            .finish_non_exhaustive()
    }
}

impl<T> std::fmt::Debug for ShardRcHandle<T>
where
    T: Eventful + HasEvents<T::EventSetType> + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardRcHandle")
            .field("id", &self.id.id)
            .field("shard", &self.shard_handle.shard_id())
            .finish_non_exhaustive()
    }
}

impl<T> std::fmt::Debug for ShardWeakHandle<T>
where
    T: Eventful + HasEvents<T::EventSetType> + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardWeakHandle")
            .field("id", &self.id)
            .field("shard", &self.shard_handle.shard_id())
            .finish_non_exhaustive()
    }
}
