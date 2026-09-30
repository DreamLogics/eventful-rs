//! Implementation support for generated typed builders.
use crate::{
    Connection, Event, EventLabel, Eventful, HasEvents, ShardEventHandle, ShardHandle, Sharded,
};
use std::{marker::PhantomData, sync::Arc};

/// No subscription label selected (wildcard).
#[derive(Debug)]
pub struct All;
/// An explicitly selected label.
#[derive(Debug)]
pub struct Selected<L>(pub L);
/// No destination selected.
#[derive(Debug)]
pub struct Unselected;
/// Ordinary queued delivery.
#[derive(Debug)]
pub struct Untracked;
/// Delivery with a completion observer.
#[derive(Debug)]
pub struct Tracked;
/// Default event-interface role.
#[derive(Debug)]
pub struct NoRole;
/// Explicit event-interface role; no role value is stored.
#[derive(Debug)]
pub struct Role<R>(PhantomData<fn() -> R>);
impl<R> Default for Role<R> {
    fn default() -> Self {
        Self(PhantomData)
    }
}
/// Resolve a builder's receiver role.
pub trait ReceiverRole {
    /// Interface role type.
    type Type: 'static;
}
impl ReceiverRole for NoRole {
    type Type = ();
}
impl<R: 'static> ReceiverRole for Role<R> {
    type Type = R;
}
/// Consume a subscription's optional label.
pub trait SubscriptionLabel<L> {
    /// None subscribes to every emission.
    fn subscription(self) -> Option<L>;
}
impl<L> SubscriptionLabel<L> for All {
    fn subscription(self) -> Option<L> {
        None
    }
}
impl<L> SubscriptionLabel<L> for Selected<L> {
    fn subscription(self) -> Option<L> {
        Some(self.0)
    }
}

/// Register ordinary and tracked delivery to a weak receiver.
pub fn connect_receiver<A, L, T, F>(
    event: &Event<A, L>,
    label: Option<L>,
    target: &impl Sharded<T>,
    callback: F,
) -> Connection<A, L>
where
    A: Clone + Send + 'static,
    L: EventLabel,
    T: Eventful + HasEvents<T::EventSetType> + 'static,
    F: Fn(&T, A) + Send + Sync + 'static,
{
    let handle = target.to_handle().downgrade();
    let tracked_handle = handle.clone();
    let callback = Arc::new(callback);
    let tracked_callback = callback.clone();
    event.add_labelled_tracked_connection(
        label,
        move |args| {
            let callback = callback.clone();
            handle.upgrade_in_shard(move |target| callback(target, args));
        },
        move |args| {
            let callback = tracked_callback.clone();
            tracked_handle.try_deferred_upgrade_in_shard(async move |target| callback(target, args))
        },
    )
}

/// Register ordinary and tracked delivery without a receiver value.
pub fn connect_shard<A, L, F>(
    event: &Event<A, L>,
    label: Option<L>,
    handle: ShardEventHandle,
    callback: F,
) -> Connection<A, L>
where
    A: Clone + Send + 'static,
    L: EventLabel,
    F: Fn(A) + Send + Sync + 'static,
{
    let tracked_handle = handle.clone();
    let callback = Arc::new(callback);
    let tracked_callback = callback.clone();
    event.add_labelled_tracked_connection(
        label,
        move |args| {
            let callback = callback.clone();
            let _ = handle.try_invoke(move || callback(args));
        },
        move |args| {
            let callback = tracked_callback.clone();
            tracked_handle.try_invoke_tracked(move || callback(args))
        },
    )
}
