//! Typed signal storage, label matching, and tracked delivery.
use crate::{Connection, DeliveryError};
use std::sync::{Arc, Mutex};

/// Untracked dispatch callback invoked on the emitting thread.
type TaskFn<Args> = dyn Fn(Args) + Send + Sync + 'static;
/// Immediate dispatch callback returning a delivery completion observer.
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

/// Immutable subscription entry shared by emission snapshots.
pub(crate) struct EventConnectionRecord<Args, Label> {
    /// Stable key identifying this entry within its owner storage.
    id: usize,
    /// Optional routing subscription; None accepts every emission.
    label: Option<Label>,
    /// Untracked dispatch callback invoked on the emitting thread.
    emit: Arc<TaskFn<Args>>,
    /// Immediate dispatch callback returning a delivery completion observer.
    tracked: Arc<TrackedTaskFn<Args>>,
}

/// Shared subscription storage for a signal signature and label type.
pub(crate) struct EventInternal<Args, Label> {
    /// Immutable subscription entry shared by emission snapshots.
    connections: Mutex<Vec<Arc<EventConnectionRecord<Args, Label>>>>,
    /// Serializes allocation of unique subscription keys.
    last_id: Mutex<usize>,
}

impl<Args, Label> EventInternal<Args, Label> {
    /// Remove under the lock, but run captured user destructors after releasing it.
    pub(crate) fn disconnect(&self, id: usize) {
        let removed = {
            let mut connections = self.connections.lock().unwrap_or_else(|e| e.into_inner());
            connections
                .iter()
                .position(|c| c.id == id)
                .map(|index| connections.remove(index))
        };
        drop(removed);
    }
}

/// Type-erased connection storage for one signal signature and optional label type.
///
/// `Event<Args>` retains unlabelled emission. `Event<Args, Label>` routes using
/// [`EventLabel`] through [`Self::emit_labelled`] and [`Self::emit_labelled_tracked`].
/// Wildcard subscriptions created with [`Self::add_connection`] or
/// [`Self::add_tracked_connection`] receive every emission.
///
/// # Example
///
/// For eventful types, prefer the [typed interface example](crate#quick-start).
/// Raw events also support inline callbacks and explicit subscription lifetimes:
///
/// ```
/// use eventful_rs::{Event, ConnectionGroup};
/// use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
/// let received = Arc::new(AtomicUsize::new(0));
/// let total = received.clone();
/// let updates = Event::<usize>::default();
/// let subscription = updates.add_connection(move |count| {
///     total.fetch_add(count, Ordering::Relaxed);
/// });
/// let group: ConnectionGroup = [subscription].into_iter().collect();
/// let scope = group.scoped();
/// futures::executor::block_on(updates.emit_tracked(3))?;
/// assert_eq!(received.load(Ordering::Relaxed), 3);
/// drop(scope);
/// assert_eq!(updates.connection_count(), 0);
/// # Ok::<(), eventful_rs::DeliveryError>(())
/// ```
pub struct Event<Args, Label = ()> {
    /// Shared subscription storage for a signal signature and label type.
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
    /// Subscribe an inline callback to all emissions. Dropping the token keeps it active.
    /// The callback runs on the emitting thread; panics propagate on untracked emission.
    ///
    /// # Panics
    /// Panics if this event has exhausted its connection identities.
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
    ///
    /// # Panics
    /// Panics if this event has exhausted its connection identities.
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
    ///
    /// # Panics
    /// Panics if this event has exhausted its connection identities.
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
        let mut last_id = self
            .internal
            .last_id
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let id = *last_id;
        *last_id = last_id
            .checked_add(1)
            .expect("connection identity space exhausted");
        self.internal
            .connections
            .lock()
            .unwrap_or_else(|e| e.into_inner())
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
    /// on the emitting thread.
    ///
    /// # Panics
    /// Propagates panics from label matching, payload cloning, and inline callbacks.
    pub fn emit_labelled(&self, label: Label, args: Args) {
        let snapshot = self
            .internal
            .connections
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
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
    ///
    /// # Errors
    /// Returns the first handler delivery error, or [`DeliveryError::Panicked`]
    /// for unwinding label, clone, or callback panics.
    pub fn emit_labelled_tracked(
        &self,
        label: Label,
        args: Args,
    ) -> impl Future<Output = Result<(), DeliveryError>> + Send + 'static + use<Args, Label> {
        use futures::FutureExt;
        use std::panic::{AssertUnwindSafe, catch_unwind};

        let snapshot = self
            .internal
            .connections
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
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

    /// Number of registered subscriptions, including expired weak targets.
    pub fn connection_count(&self) -> usize {
        self.internal
            .connections
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .len()
    }
}

impl<Args: Clone + Send + 'static> Event<Args> {
    /// Dispatch an unlabelled event to a snapshot of its connections.
    /// See the [`Event`] example for subscription setup.
    ///
    /// # Panics
    /// Propagates panics from payload cloning and inline callbacks.
    pub fn emit(&self, args: Args) {
        self.emit_labelled((), args);
    }

    /// Dispatch immediately and observe completion of all snapshot deliveries.
    /// Panics are reported as errors; dropping the future does not cancel delivery.
    /// See the [`Event`] example.
    ///
    /// # Errors
    /// Returns the first delivery error, including [`DeliveryError::Panicked`]
    /// for unwinding clone or callback panics.
    pub fn emit_tracked(
        &self,
        args: Args,
    ) -> impl Future<Output = Result<(), DeliveryError>> + Send + 'static + use<Args> {
        self.emit_labelled_tracked((), args)
    }
}

impl<Args, Label> std::fmt::Debug for Event<Args, Label> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Event")
            .field(
                "subscriptions",
                &self
                    .internal
                    .connections
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .len(),
            )
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::Event;

    #[test]
    fn scoped_cleanup_recovers_a_poisoned_subscription_lock() {
        let event = Event::<()>::default();
        let guard = event.add_connection(|()| {}).scoped();
        let _ = std::panic::catch_unwind(|| {
            let _lock = event.internal.connections.lock().unwrap();
            panic!("poison subscription storage");
        });
        drop(guard);
        assert_eq!(event.connection_count(), 0);
    }

    #[test]
    fn exhausted_connection_ids_never_wrap() {
        let event = Event::<()>::default();
        *event.internal.last_id.lock().unwrap() = usize::MAX;
        assert!(std::panic::catch_unwind(|| event.add_connection(|()| {})).is_err());
        assert_eq!(event.connection_count(), 0);
    }
}
