//! Shard-local values and strong or weak cross-thread handles.
use std::{rc::Rc, sync::Arc};

use crate::{EventLoopHandle, Eventful, HasEvents};

mod store;
pub(crate) use store::{ShardRcId, ShardRcStore};
mod joined;
pub use joined::JoinedHandles;

mod sealed;
use sealed::Sealed;
pub(crate) use sealed::Target;

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
// Connections drop and owned tasks abort before T, including during shard shutdown.
/// Keep value-owned connections and tasks in the same allocation as the value.
pub(crate) struct ShardValue<T> {
    /// Subscriptions dropped before the value, including during shutdown.
    connections: std::cell::RefCell<Vec<crate::ScopedConnectionGroup>>,
    /// Local tasks aborted before the value drops.
    tasks: std::cell::RefCell<Vec<crate::TaskHandle>>,
    /// The shard-local application value.
    value: T,
}

impl<T> ShardValue<T> {
    /// Store an eventful value with its owned subscriptions.
    pub(crate) fn new(value: T) -> Self {
        Self {
            connections: Default::default(),
            tasks: Default::default(),
            value,
        }
    }

    /// Abort a local task when the value is destroyed, first pruning finished
    /// tasks so repeated spawns stay bounded.
    pub(crate) fn own_task(&self, task: crate::TaskHandle) {
        let mut tasks = self.tasks.borrow_mut();
        tasks.retain(|task| !task.is_finished());
        tasks.push(task);
    }

    /// Retain a subscription group until the value is destroyed, first pruning
    /// groups that were already disconnected so repeated connects stay bounded.
    pub(crate) fn own_connections(&self, group: crate::ConnectionGroup) {
        let pruned: Vec<_> = {
            let mut connections = self.connections.borrow_mut();
            let (pruned, live) = std::mem::take(&mut *connections)
                .into_iter()
                .partition(crate::ScopedConnectionGroup::is_disconnected);
            *connections = live;
            connections.push(group.scoped());
            pruned
        };
        // Guards may release user captures; never drop them under the borrow.
        drop(pruned);
    }
}

impl<T> Drop for ShardValue<T> {
    fn drop(&mut self) {
        // Aborting only flags the tasks; their shard drops the futures later.
        for task in self.tasks.get_mut().drain(..) {
            task.abort();
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

/// Weak shard-local reference. It is neither Send nor Sync and does not keep the value alive.
/// Values remain upgradeable until the shard collects them. The shard collects a
/// value soon after its last strong reference is released and in-flight callbacks finish.
/// A surviving local strong reference also permits upgrades after shard shutdown.
pub struct ShardWeak<T>
where
    T: Eventful + HasEvents<T::EventSetType> + 'static,
{
    /// Weak reference to the local allocation.
    inner: std::rc::Weak<ShardValue<T>>,
    /// Weak lifetime token shared with remote strong handles.
    id: std::sync::Weak<store::StoreEntry>,
    /// Submission handle, which does not retain the value.
    shard_handle: crate::ShardEventHandle,
}

impl<T> Clone for ShardWeak<T>
where
    T: Eventful + HasEvents<T::EventSetType> + 'static,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            id: self.id.clone(),
            shard_handle: self.shard_handle.clone(),
        }
    }
}

impl<T> std::fmt::Debug for ShardWeak<T>
where
    T: Eventful + HasEvents<T::EventSetType> + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardWeak").finish_non_exhaustive()
    }
}

impl<T> ShardWeak<T>
where
    T: Eventful + HasEvents<T::EventSetType> + 'static,
{
    /// Upgrade synchronously if the local allocation is still alive.
    pub fn upgrade(&self) -> Option<ShardRc<T>> {
        let inner = self.inner.upgrade()?;
        Some(ShardRc::new(
            ShardRcId::upgrade(&self.id)?,
            inner,
            self.shard_handle.clone(),
        ))
    }
}

impl<T> ShardRc<T>
where
    T: Eventful + HasEvents<T::EventSetType> + 'static,
{
    /// Create a weak local reference without retaining the value.
    pub fn downgrade(this: &Self) -> ShardWeak<T> {
        ShardWeak {
            inner: Rc::downgrade(&this.inner),
            id: this.id.downgrade(),
            shard_handle: this.shard_handle.clone(),
        }
    }

    /// Create a synchronous callback that skips calls after the value is collected.
    /// Use `()` for no arguments and a tuple for multiple arguments. The callback
    /// receives a local strong reference for the duration of the call.
    /// Capturing another strong reference to this value defeats weak ownership.
    pub fn weak_callback<A, F>(&self, callback: F) -> impl Fn(A) + 'static
    where
        F: Fn(&ShardRc<T>, A) + 'static,
    {
        let weak = Self::downgrade(self);
        move |args| {
            if let Some(value) = weak.upgrade() {
                callback(&value, args);
            }
        }
    }

    /// Create a synchronous callback with an explicit fallback for an expired value.
    /// Use `()` for no arguments and a tuple for multiple arguments.
    pub fn weak_callback_or_else<A, R, F, G>(
        &self,
        callback: F,
        fallback: G,
    ) -> impl Fn(A) -> R + 'static
    where
        F: Fn(&ShardRc<T>, A) -> R + 'static,
        G: Fn(A) -> R + 'static,
    {
        let weak = Self::downgrade(self);
        move |args| match weak.upgrade() {
            Some(value) => callback(&value, args),
            None => fallback(args),
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
        self.id.key()
    }
    fn shard_id(&self) -> crate::ShardId {
        self.shard_handle.shard_id()
    }
    fn target(&self) -> Target {
        Target {
            key: self.id.key(),
            _retain: Some(self.id.clone()),
        }
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
        // A stopped shard skips the callback, as for an expired target.
        let _ = self.shard_handle.run_with(self.target(), task);
    }

    fn upgrade_in_shard_async<F>(&self, task: F)
    where
        F: AsyncFnOnce(&T) -> () + Send + 'static,
    {
        let _ = self.shard_handle.post_with(self.target(), task);
    }

    async fn deferred_upgrade_in_shard<F, R>(&self, task: F) -> R
    where
        F: AsyncFnOnce(&T) -> R + Send + 'static,
        R: Send + 'static,
    {
        self.shard_handle
            .deferred_with(self.target(), task)
            .await
            .expect("deferred shard invocation failed")
    }

    /// Make a handle that does not keep the target value alive.
    fn downgrade(&self) -> ShardWeakHandle<T>
    where
        T: Eventful + HasEvents<<T as Eventful>::EventSetType> + Sized + 'static,
    {
        ShardWeakHandle {
            id: self.id.key(),
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
    fn target(&self) -> Target {
        Target {
            key: self.id,
            _retain: None,
        }
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
        Box::pin(self.deferred_in_shard(task))
    }

    /// Unboxed form of [`Self::try_deferred_upgrade_in_shard`] for internal callers.
    pub(crate) fn deferred_in_shard<F, R>(
        &self,
        task: F,
    ) -> impl Future<Output = Result<R, crate::InvokeError>> + Send + 'static + use<T, F, R>
    where
        F: AsyncFnOnce(&T) -> R + Send + 'static,
        R: Send + 'static,
    {
        self.shard_handle.deferred_with(self.target(), task)
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
        // A stopped shard skips the callback, as for an expired target.
        let _ = self.shard_handle.run_with(self.target(), task);
    }

    fn upgrade_in_shard_async<F>(&self, task: F)
    where
        F: AsyncFnOnce(&T) -> () + Send + 'static,
    {
        let _ = self.shard_handle.post_with(self.target(), task);
    }

    async fn deferred_upgrade_in_shard<F, R>(&self, task: F) -> R
    where
        F: AsyncFnOnce(&T) -> R + Send + 'static,
        R: Send + 'static,
    {
        self.deferred_in_shard(task)
            .await
            .expect("deferred shard invocation failed")
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

    /// Recover a local strong reference from a reference to a bound value,
    /// such as `&self` inside a method. Lookup is by identity, not equality.
    ///
    /// ```
    /// use eventful_rs::*;
    /// declare_shard!(Ui, runtime = main);
    ///
    /// #[eventful(shard = Ui)]
    /// struct Window;
    /// impl Window {
    ///     fn this(&self) -> ShardRc<Self> {
    ///         ShardRc::try_from_ref(self).expect("bound on its shard")
    ///     }
    /// }
    ///
    /// Ui::shard().run_main(|| async {
    ///     let window = Window::bind_local(Window { events: Default::default() })?;
    ///     assert!(std::ptr::eq(&*window.this(), &*window));
    ///     // A value that was never bound has no shard entry.
    ///     let unbound = Window { events: Default::default() };
    ///     assert_eq!(ShardRc::try_from_ref(&unbound).err(), Some(InvokeError::ValueMissing));
    ///     Ok::<(), InvokeError>(())
    /// })?;
    /// # Ok::<(), InvokeError>(())
    /// ```
    ///
    /// # Errors
    /// Returns [`crate::InvokeError::WrongShard`] outside the named shard's
    /// context, or [`crate::InvokeError::ValueMissing`] if `value` is not stored
    /// in that shard (for example, a value that was never bound).
    ///
    /// # Panics
    /// A custom shard binding may panic while obtaining its singleton handle.
    pub fn try_from_ref(value: &T) -> Result<Self, crate::InvokeError> {
        <T::Shard as crate::ShardBinding>::handle().find_local(value)
    }
}

impl<T> ShardRcHandle<T>
where
    T: Eventful + HasEvents<T::EventSetType> + 'static,
    T::Shard: crate::ShardBinding,
{
    /// Recover a strong remote handle from a reference to a bound value.
    /// Must run on the value's shard; see [`ShardRc::try_from_ref`].
    ///
    /// # Errors
    /// Returns [`crate::InvokeError::WrongShard`] outside the named shard's
    /// context, or [`crate::InvokeError::ValueMissing`] if `value` is not stored
    /// in that shard.
    ///
    /// # Panics
    /// A custom shard binding may panic while obtaining its singleton handle.
    pub fn try_from_ref(value: &T) -> Result<Self, crate::InvokeError> {
        ShardRc::try_from_ref(value).map(|local| local.to_handle())
    }
}

impl<T> ShardRcHandle<T>
where
    T: Eventful + HasEvents<T::EventSetType> + 'static,
{
    /// Recover a local strong reference when running on the value's shard.
    /// Synchronous: nothing is queued. The result is the same reference
    /// [`ShardRc::try_from_ref`] returns, and still cannot leave this thread.
    ///
    /// ```
    /// use eventful_rs::*;
    /// declare_shard!(Ui, runtime = main);
    ///
    /// #[eventful(shard = Ui)]
    /// struct Client;
    ///
    /// Ui::shard().run_main(|| async {
    ///     let client = Client::bind_local(Client { events: Default::default() })?;
    ///     let handle = client.to_handle();
    ///     let local = handle.try_local()?;
    ///     assert!(std::ptr::eq(&*local, &*client));
    ///     let weak = handle.downgrade();
    ///     assert!(weak.try_local()?.is_some());
    ///     Ok::<(), InvokeError>(())
    /// })?;
    /// # Ok::<(), InvokeError>(())
    /// ```
    ///
    /// # Errors
    /// Returns [`crate::InvokeError::WrongShard`] outside the value's shard
    /// context, or [`crate::InvokeError::ValueMissing`] if the value is no
    /// longer stored.
    pub fn try_local(&self) -> Result<ShardRc<T>, crate::InvokeError> {
        let store = crate::engine::local_store(self.shard_handle.shard_id)
            .ok_or(crate::InvokeError::WrongShard)?;
        let value = store
            .borrow()
            .get::<ShardValue<T>>(self.id.key())
            .ok_or(crate::InvokeError::ValueMissing)?;
        Ok(ShardRc::new(
            self.id.clone(),
            value,
            self.shard_handle.clone(),
        ))
    }
}

impl<T> ShardWeakHandle<T>
where
    T: Eventful + HasEvents<T::EventSetType> + 'static,
{
    /// Like [`ShardRcHandle::try_local`], but yields `Ok(None)` once the value
    /// has been collected.
    ///
    /// # Errors
    /// Returns [`crate::InvokeError::WrongShard`] outside the value's shard context.
    pub fn try_local(&self) -> Result<Option<ShardRc<T>>, crate::InvokeError> {
        let store = crate::engine::local_store(self.shard_handle.shard_id)
            .ok_or(crate::InvokeError::WrongShard)?;
        let found = store.borrow().get_retained::<T>(self.id);
        Ok(found.map(|(id, value)| ShardRc::new(id, value, self.shard_handle.clone())))
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
    /// Connect this source's events to a receiver, retaining subscriptions with this value.
    /// The receiver is held weakly. Sources without events cannot be bulk-connected.
    pub fn connect_to<U, S>(&self, target: &S) -> crate::ConnectionGroup
    where
        U: Eventful + HasEvents<U::EventSetType> + 'static,
        S: Sharded<U>,
        T::EventSetType: crate::ConnectEvents<U>,
    {
        Self::connect(self, target)
    }

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
        this.inner.own_connections(group.clone());
        group
    }

    /// Spawn a future on the value's shard; the caller owns the returned task.
    /// See [`crate::ShardBinding::spawn_local`] for scheduling, panics, and
    /// shutdown. The future may capture this reference, but then keeps the
    /// value alive until it ends; [`Self::spawn_owned`] avoids that.
    ///
    /// # Errors
    /// Returns [`crate::InvokeError::Closed`] once the shard is shutting down.
    pub fn spawn_local<F>(
        this: &Self,
        future: F,
    ) -> Result<crate::LocalTask<F::Output>, crate::InvokeError>
    where
        F: Future + 'static,
        F::Output: 'static,
    {
        crate::local_task::spawn(&this.shard_handle, future)
    }

    /// Spawn a task owned by the value: it is aborted when the value is
    /// destroyed, or earlier through the returned handle. The closure receives
    /// a weak reference, so the task does not keep the value alive.
    ///
    /// ```
    /// use eventful_rs::*;
    /// use std::cell::Cell;
    /// declare_shard!(Ui, runtime = main);
    ///
    /// #[eventful(shard = Ui)]
    /// struct Feed {
    ///     polls: Cell<u32>,
    /// }
    ///
    /// Ui::shard().run_main(|| async {
    ///     let feed = Feed::bind_local(Feed { polls: Cell::new(0), events: Default::default() })?;
    ///     let (first, polled) = futures::channel::oneshot::channel();
    ///     let task = ShardRc::spawn_owned(&feed, |feed| async move {
    ///         let mut first = Some(first);
    ///         // A real feed would await a request or timer between polls.
    ///         while let Some(feed) = feed.upgrade() {
    ///             feed.polls.set(feed.polls.get() + 1);
    ///             drop(feed); // Never hold a strong reference across an await.
    ///             if let Some(first) = first.take() {
    ///                 let _ = first.send(());
    ///             }
    ///             futures::future::pending::<()>().await;
    ///         }
    ///     })?;
    ///     polled.await.expect("the feed polled");
    ///     assert_eq!(feed.polls.get(), 1);
    ///     // Destroying the value aborts its task; `task.abort()` stops it earlier.
    ///     drop(feed);
    ///     # let _ = task;
    ///     Ok::<(), InvokeError>(())
    /// })?;
    /// # Ok::<(), InvokeError>(())
    /// ```
    ///
    /// # Errors
    /// Returns [`crate::InvokeError::Closed`] once the shard is shutting down.
    pub fn spawn_owned<F, Fut>(
        this: &Self,
        task: F,
    ) -> Result<crate::TaskHandle, crate::InvokeError>
    where
        F: FnOnce(ShardWeak<T>) -> Fut,
        Fut: Future<Output = ()> + 'static,
    {
        let task = crate::local_task::spawn(&this.shard_handle, task(Self::downgrade(this)))?;
        let handle = task.detach();
        this.inner.own_task(handle.clone());
        Ok(handle)
    }

    /// Select a role for source-owned connections to every event.
    pub fn role<Role>(this: &Self) -> RoleConnections<'_, Self, Role> {
        RoleConnections {
            source: this,
            role: std::marker::PhantomData,
        }
    }

    /// Connect all signals with a receiver role; the local source owns cleanup.
    /// Dropping the returned token leaves connections active until source destruction.
    fn connect_as<Role: 'static, U>(this: &Self, target: &impl Sharded<U>) -> crate::ConnectionGroup
    where
        U: Eventful + HasEvents<U::EventSetType> + 'static,
        T::EventSetType: crate::ConnectEventsAs<U, Role>,
    {
        let group = crate::ConnectEventsAs::<U, Role>::connect_events_as(&*this.events, target);
        this.inner.own_connections(group.clone());
        group
    }
}

impl<T> ShardRcHandle<T>
where
    T: Eventful + HasEvents<T::EventSetType> + 'static,
{
    /// Connect this source's events to a receiver held weakly.
    /// Dropping the returned token leaves subscriptions active.
    pub fn connect_to<U, S>(&self, target: &S) -> crate::ConnectionGroup
    where
        U: Eventful + HasEvents<U::EventSetType> + 'static,
        S: Sharded<U>,
        T::EventSetType: crate::ConnectEvents<U>,
    {
        self.connect(target)
    }

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

    /// Select a receiver role for every event in this source.
    pub fn role<Role>(&self) -> RoleConnections<'_, Self, Role> {
        RoleConnections {
            source: self,
            role: std::marker::PhantomData,
        }
    }

    /// Connect all signals with a receiver role, holding the receiver weakly.
    /// Use disconnect() or scoped() for cleanup; dropping the token leaves it active.
    fn connect_as<Role: 'static, U>(&self, target: &impl Sharded<U>) -> crate::ConnectionGroup
    where
        U: Eventful + HasEvents<U::EventSetType> + 'static,
        T::EventSetType: crate::ConnectEventsAs<U, Role>,
    {
        crate::ConnectEventsAs::<U, Role>::connect_events_as(&*self.events, target)
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

    /// Select a receiver role, resolving weak event storage when connected.
    pub fn role<Role>(&self) -> RoleConnections<'_, Self, Role> {
        RoleConnections {
            source: self,
            role: std::marker::PhantomData,
        }
    }

    /// Connect all signals with a receiver role if the source event set still exists.
    /// Returns None for an expired source; the receiver is held weakly.
    fn connect_as<Role: 'static, U>(
        &self,
        target: &impl Sharded<U>,
    ) -> Option<crate::ConnectionGroup>
    where
        U: Eventful + HasEvents<U::EventSetType> + 'static,
        T::EventSetType: crate::ConnectEventsAs<U, Role>,
    {
        let events = self.events.upgrade()?;
        Some(crate::ConnectEventsAs::<U, Role>::connect_events_as(
            &*events, target,
        ))
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
            .field("id", &self.id.key())
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
            .field("id", &self.id.key())
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

/// Receiver-role adapter for a whole source interface.
#[must_use = "call connect to register the subscriptions"]
pub struct RoleConnections<'a, S, Role> {
    /// Source whose lifetime and storage rules govern these connections.
    source: &'a S,
    /// The role carries no runtime value.
    role: std::marker::PhantomData<fn() -> Role>,
}
impl<S, Role> std::fmt::Debug for RoleConnections<'_, S, Role> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RoleConnections").finish_non_exhaustive()
    }
}
impl<T, Role: 'static> RoleConnections<'_, ShardRc<T>, Role>
where
    T: Eventful + HasEvents<T::EventSetType> + 'static,
{
    /// Connect every event using the selected role and the source's ownership rules.
    pub fn connect<U>(self, target: &impl Sharded<U>) -> crate::ConnectionGroup
    where
        U: Eventful + HasEvents<U::EventSetType> + 'static,
        T::EventSetType: crate::ConnectEventsAs<U, Role>,
    {
        ShardRc::connect_as::<Role, U>(self.source, target)
    }
}
impl<T, Role: 'static> RoleConnections<'_, ShardRcHandle<T>, Role>
where
    T: Eventful + HasEvents<T::EventSetType> + 'static,
{
    /// Connect every event using the selected role and the source's ownership rules.
    pub fn connect<U>(self, target: &impl Sharded<U>) -> crate::ConnectionGroup
    where
        U: Eventful + HasEvents<U::EventSetType> + 'static,
        T::EventSetType: crate::ConnectEventsAs<U, Role>,
    {
        self.source.connect_as::<Role, U>(target)
    }
}
impl<T, Role: 'static> RoleConnections<'_, ShardWeakHandle<T>, Role>
where
    T: Eventful + HasEvents<T::EventSetType> + 'static,
{
    /// Connect every event using the selected role and the source's ownership rules.
    pub fn connect<U>(self, target: &impl Sharded<U>) -> Option<crate::ConnectionGroup>
    where
        U: Eventful + HasEvents<U::EventSetType> + 'static,
        T::EventSetType: crate::ConnectEventsAs<U, Role>,
    {
        self.source.connect_as::<Role, U>(target)
    }
}
