#![doc = include_str!("../README.md")]

pub use eventful_rs_macros::{
    action, asynced, asynchronize, eventful, events, scope, sharded_main,
};

mod binding;
pub use binding::*;

mod engine;
pub use engine::{InvokeError, ShardEventHandle};

/// A tracked event handler failed to complete. Uses the same failure reasons as
/// deferred shard invocations.
pub type DeliveryError = InvokeError;

mod shard_handle;
pub use shard_handle::*;
mod connection;
pub use connection::*;

pub mod local;

pub mod shard;

pub mod task;

//pub mod rv;

#[cfg(feature = "tokio")]
pub mod tokio;

#[cfg(feature = "tokio")]
pub mod tokio_local;

#[cfg(feature = "slint")]
pub mod slint;

use std::sync::{Arc, Mutex};

type JoinCallback = Box<dyn Fn() -> Result<(), ShardError> + Send + Sync + 'static>;
static SHARD_REGISTRY: Mutex<Vec<(ShardId, JoinCallback)>> = Mutex::new(Vec::new());

/// Stop and join registered background shards without holding the registry lock.
/// Call from synchronous code, after producers have finished submitting work.
pub fn join_all_shards() -> Result<(), ShardError> {
    if engine::on_shard_thread() {
        return Err(ShardError::JoinError(
            "join all shards from outside their event loops".into(),
            None,
        ));
    }
    #[cfg(feature = "tokio")]
    if ::tokio::runtime::Handle::try_current().is_ok() {
        return Err(ShardError::JoinError(
            "use join_all_shards_async() from Tokio".into(),
            None,
        ));
    }
    join_all_shards_blocking()
}

fn join_all_shards_blocking() -> Result<(), ShardError> {
    let entries = std::mem::take(&mut *SHARD_REGISTRY.lock().unwrap());
    let mut errors = Vec::new();
    let mut retry = Vec::new();
    for (id, join) in entries {
        if let Err(error) = join() {
            errors.push(error.to_string());
            retry.push((id, join));
        }
    }
    SHARD_REGISTRY.lock().unwrap().extend(retry);
    if errors.is_empty() {
        Ok(())
    } else {
        Err(ShardError::JoinError(errors.join("; "), None))
    }
}

/// Join background shards without blocking a Tokio worker.
#[cfg(feature = "tokio")]
pub async fn join_all_shards_async() -> Result<(), ShardError> {
    if engine::on_shard_thread() {
        return Err(ShardError::JoinError(
            "cannot join all shards from a shard thread".into(),
            None,
        ));
    }
    ::tokio::task::spawn_blocking(join_all_shards_blocking)
        .await
        .map_err(|e| ShardError::JoinError(e.to_string(), None))?
}

fn register_shard(shard_id: ShardId, join: JoinCallback) {
    SHARD_REGISTRY.lock().unwrap().push((shard_id, join));
}

#[derive(Debug)]
pub enum ShardError {
    JoinError(
        String,
        Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    ),
    PostError(
        String,
        Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    ),
}

impl std::fmt::Display for ShardError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ShardError::JoinError(msg, original_err) => {
                write!(f, "join error: {msg}")?;

                if let Some(err) = original_err {
                    write!(f, ": {err}")?;
                }

                Ok(())
            }

            ShardError::PostError(msg, original_err) => {
                write!(f, "post error: {msg}")?;

                if let Some(err) = original_err {
                    write!(f, ": {err}")?;
                }

                Ok(())
            }
        }
    }
}

impl std::error::Error for ShardError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ShardError::JoinError(_, Some(err)) | ShardError::PostError(_, Some(err)) => {
                Some(err.as_ref())
            }

            _ => None,
        }
    }
}

static LAST_SHARD_ID: Mutex<usize> = Mutex::new(0);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ShardId(pub usize);

impl ShardId {
    pub fn new() -> Self {
        let mut last_id = LAST_SHARD_ID.lock().unwrap();
        *last_id += 1;
        ShardId(*last_id)
    }
}

impl Default for ShardId {
    fn default() -> Self {
        ShardId::new()
    }
}

pub trait EventLoopHandle: Clone + Send + Sync + 'static {
    fn shard_id(&self) -> ShardId;
    fn invoke<F>(&self, task: F)
    where
        F: FnOnce() + Send + 'static;
    fn invoke_async<F>(&self, f: F)
    where
        F: AsyncFnOnce() -> () + Send + 'static;
    fn invoke_with_handle<T, H, F>(&self, handle: H, f: F)
    where
        T: Eventful + HasEvents<T::EventSetType> + Sized + 'static,
        H: ShardHandle<T>,
        F: FnOnce(&T) + Send + 'static;
    fn invoke_with_handle_async<T, H, F>(&self, handle: H, f: F)
    where
        T: Eventful + HasEvents<T::EventSetType> + Sized + 'static,
        H: ShardHandle<T>,
        F: AsyncFnOnce(&T) -> () + Send + 'static;

    fn deferred_invoke<T, H, F, R>(
        &self,
        handle: H,
        task: F,
    ) -> futures::future::BoxFuture<'static, R>
    where
        T: Eventful + HasEvents<T::EventSetType> + Sized + 'static,
        H: ShardHandle<T>,
        F: AsyncFnOnce(&T) -> R + Send + 'static,
        R: Send + 'static;

    /// Submit a callback immediately and report its completion. Backends may
    /// override this to distinguish rejection and missing targets from cancellation.
    fn try_deferred_invoke<T, H, F, R>(
        &self,
        handle: H,
        task: F,
    ) -> futures::future::BoxFuture<'static, Result<R, InvokeError>>
    where
        T: Eventful + HasEvents<T::EventSetType> + Sized + 'static,
        H: ShardHandle<T>,
        F: AsyncFnOnce(&T) -> R + Send + 'static,
        R: Send + 'static,
    {
        use futures::FutureExt;
        let (tx, rx) = futures::channel::oneshot::channel();
        self.invoke_with_handle_async(handle, async move |target| {
            let result = std::panic::AssertUnwindSafe(async move { task(target).await })
                .catch_unwind()
                .await
                .map_err(|_| InvokeError::Panicked);
            let _ = tx.send(result);
        });
        Box::pin(async move { rx.await.map_err(|_| InvokeError::Canceled)? })
    }

    fn spawn<F>(&self, f: F)
    where
        F: Future + Send + 'static;
}

/// A value's event interface and shard affinity.
pub trait Eventful {
    type EventSetType: ?Sized + Send + Sync + 'static;
    type Shard: ShardAffinity;

    /// Construct on the designated shard and return a thread-safe handle.
    /// Only the factory's captures must be Send; Self may contain Rc or RefCell.
    /// The designated event loop must be running to complete this operation.
    fn spawn<F>(
        factory: F,
    ) -> impl Future<Output = Result<ShardRcHandle<Self>, InvokeError>> + Send + 'static
    where
        Self: Sized + HasEvents<Self::EventSetType> + 'static,
        Self::Shard: ShardBinding,
        F: FnOnce() -> Self + Send + 'static,
    {
        Self::Shard::handle().bind_async(move |bind| bind(factory()).as_handle())
    }
}

pub trait HasEvents<E>
where
    E: ?Sized + Send + 'static,
{
    fn events(&self) -> &Arc<E>;
}

pub trait EventLoop {
    type HandleType: EventLoopHandle;
    fn handle(&self) -> Self::HandleType;
    /// Spawn values bound to this event loop.
    fn bind<F, R, T>(&self, f: F) -> R
    where
        F: FnOnce(&dyn Fn(T) -> ShardRc<T>) -> R + Send + 'static,
        R: Send + 'static,
        T: Eventful + HasEvents<T::EventSetType> + Sized + 'static;
    fn spawn<F>(&self, f: F)
    where
        F: Future + Send + 'static,
    {
        self.handle().spawn(f);
    }
    fn join(&self) -> Result<(), ShardError>;
}

type TaskFn<Args> = dyn Fn(Args) + Send + Sync + 'static;
type TrackedTaskFn<Args> = dyn Fn(Args) -> futures::future::BoxFuture<'static, Result<(), DeliveryError>>
    + Send
    + Sync
    + 'static;

/// Routing metadata for a labelled event.
///
/// `subscription.matches(&emitted)` is evaluated on the emitting thread before
/// cloning arguments or scheduling the receiver. Matching need not be symmetric
/// or use equality: labels can represent masks, ranges, or application rules.
/// Implementations should be fast and free of side effects. Concurrent emissions
/// may call this method concurrently. Arbitrary matching requires scanning labels.
/// Labels are not passed to event handlers and need not implement `Clone` or `Eq`.
///
/// ```
/// use eventful_rs::{events, EventLabel};
///
/// struct Topics(u8);
/// impl EventLabel for Topics {
///     fn matches(&self, emitted: &Self) -> bool {
///         self.0 & emitted.0 != 0
///     }
/// }
///
/// #[events]
/// trait Updates {
///     #[with_label(Topics)]
///     fn changed(&self, value: String);
/// }
/// ```
pub trait EventLabel: Send + Sync + 'static {
    /// Whether this subscription accepts the emitted label.
    fn matches(&self, emitted: &Self) -> bool;
}

impl EventLabel for () {
    fn matches(&self, _: &Self) -> bool {
        true
    }
}

struct EventConnectionRecord<Args, Label> {
    id: usize,
    label: Option<Label>,
    emit: Arc<TaskFn<Args>>,
    tracked: Arc<TrackedTaskFn<Args>>,
}

struct EventInternal<Args, Label> {
    connections: Mutex<Vec<Arc<EventConnectionRecord<Args, Label>>>>,
    last_id: Mutex<usize>,
}

/// Type-erased connection storage for one signal signature and optional label type.
///
/// `Event<Args>` retains unlabelled emission. `Event<Args, Label>` routes using
/// [`EventLabel`] through [`Self::emit_labelled`] and [`Self::emit_labelled_tracked`].
/// Wildcard subscriptions created with [`Self::add_connection`] or
/// [`Self::add_tracked_connection`] receive every emission.
pub struct Event<Args, Label = ()> {
    internal: Arc<EventInternal<Args, Label>>,
}

impl<Args, Label> Clone for Event<Args, Label> {
    fn clone(&self) -> Self {
        Self {
            internal: self.internal.clone(),
        }
    }
}

impl<Args, Label> Default for Event<Args, Label> {
    fn default() -> Self {
        Self {
            internal: Arc::new(EventInternal {
                connections: Mutex::new(Vec::new()),
                last_id: Mutex::new(0),
            }),
        }
    }
}

impl<Args, Label> Event<Args, Label>
where
    Args: Clone + Send + 'static,
    Label: EventLabel,
{
    pub fn add_connection<F>(&self, connection: F) -> Connection<Args, Label>
    where
        F: Fn(Args) + Send + Sync + 'static,
    {
        let connection = Arc::new(connection);
        let emit = connection.clone();
        self.add_tracked_connection(
            move |args| emit(args),
            move |args| {
                connection(args);
                std::future::ready(Ok(()))
            },
        )
    }

    /// Register ordinary dispatch and tracked dispatch for the same connection.
    /// Tracked dispatch must submit work immediately; its returned future observes
    /// completion. Dropping that future should not cancel the submitted work.
    pub fn add_tracked_connection<F, G, Fut>(&self, emit: F, tracked: G) -> Connection<Args, Label>
    where
        F: Fn(Args) + Send + Sync + 'static,
        G: Fn(Args) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), DeliveryError>> + Send + 'static,
    {
        self.add_labelled_tracked_connection(None, emit, tracked)
    }

    /// Register a subscription and its ordinary and tracked delivery callbacks.
    /// `None` subscribes to every emission; `Some(label)` matches on the emitting
    /// thread before arguments are cloned or delivery callbacks are invoked.
    /// Matching runs outside the connection lock. Labels need not be `Clone`.
    /// Tracked callbacks must submit immediately, as in `add_tracked_connection`.
    pub fn add_labelled_tracked_connection<F, G, Fut>(
        &self,
        label: Option<Label>,
        emit: F,
        tracked: G,
    ) -> Connection<Args, Label>
    where
        F: Fn(Args) + Send + Sync + 'static,
        G: Fn(Args) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), DeliveryError>> + Send + 'static,
    {
        let mut last_id = self.internal.last_id.lock().unwrap();
        let id = *last_id;
        *last_id += 1;
        self.internal
            .connections
            .lock()
            .unwrap()
            .push(Arc::new(EventConnectionRecord {
                id,
                label,
                emit: Arc::new(emit),
                tracked: Arc::new(move |args| Box::pin(tracked(args))),
            }));
        Connection::new(id, self.internal.clone())
    }

    /// Dispatch only to wildcard and matching subscriptions, once per connection.
    /// Matching scans a connection snapshot and calls `subscription.matches(&label)`
    /// on the emitting thread. A matching panic propagates to the caller.
    pub fn emit_labelled(&self, label: Label, args: Args) {
        let snapshot = self.internal.connections.lock().unwrap().clone();
        for connection in snapshot {
            if connection
                .label
                .as_ref()
                .is_none_or(|subscription| subscription.matches(&label))
            {
                (connection.emit)(args.clone());
            }
        }
    }

    /// Dispatch to a snapshot of the connections immediately, then wait for every
    /// handler to complete. Returns the first error in connection order after all
    /// deliveries settle; an empty event succeeds. Panics are reported as errors.
    /// Dropping the returned future does not cancel submitted shard deliveries.
    /// Plain `add_connection` callbacks are complete when they return; work they
    /// independently spawn is not tracked. Rejected subscriptions are successful
    /// skips, even when their destination shard is closed. Matching panics are
    /// reported as `DeliveryError::Panicked`; other connections are still processed.
    pub fn emit_labelled_tracked(
        &self,
        label: Label,
        args: Args,
    ) -> impl Future<Output = Result<(), DeliveryError>> + Send + 'static + use<Args, Label> {
        use futures::FutureExt;
        use std::panic::{AssertUnwindSafe, catch_unwind};

        let snapshot = self.internal.connections.lock().unwrap().clone();
        let mut deliveries = Vec::with_capacity(snapshot.len());
        for connection in snapshot {
            let delivery = catch_unwind(AssertUnwindSafe(|| {
                if connection
                    .label
                    .as_ref()
                    .is_none_or(|subscription| subscription.matches(&label))
                {
                    Some((connection.tracked)(args.clone()))
                } else {
                    None
                }
            }));
            if matches!(delivery, Ok(None)) {
                continue;
            }
            deliveries.push(async move {
                match delivery {
                    Ok(Some(future)) => AssertUnwindSafe(future)
                        .catch_unwind()
                        .await
                        .unwrap_or(Err(DeliveryError::Panicked)),
                    Ok(None) => Ok(()),
                    Err(_) => Err(DeliveryError::Panicked),
                }
            });
        }
        async move {
            futures::future::join_all(deliveries)
                .await
                .into_iter()
                .collect()
        }
    }

    pub fn connection_count(&self) -> usize {
        self.internal.connections.lock().unwrap().len()
    }
}

impl<Args: Clone + Send + 'static> Event<Args> {
    /// Dispatch an unlabelled event to a snapshot of its connections.
    pub fn emit(&self, args: Args) {
        self.emit_labelled((), args);
    }

    /// Dispatch immediately and observe completion of all snapshot deliveries.
    /// Panics are reported as errors; dropping the future does not cancel delivery.
    pub fn emit_tracked(
        &self,
        args: Args,
    ) -> impl Future<Output = Result<(), DeliveryError>> + Send + 'static + use<Args> {
        self.emit_labelled_tracked((), args)
    }
}

mod background;
mod main_loop;
