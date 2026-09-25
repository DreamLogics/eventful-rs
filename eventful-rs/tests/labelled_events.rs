use eventful_rs::*;
use futures::executor::block_on;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

// Intentionally neither Clone nor Eq: subscriptions accept emitted subsets.
struct Mask(u8);
impl EventLabel for Mask {
    fn matches(&self, emitted: &Self) -> bool {
        emitted.0 != 0 && self.0 & emitted.0 == emitted.0
    }
}

struct Payload(Arc<AtomicUsize>);
impl Clone for Payload {
    fn clone(&self) -> Self {
        self.0.fetch_add(1, Ordering::SeqCst);
        Self(self.0.clone())
    }
}

#[test]
fn matching_precedes_cloning_and_dispatch_and_preserves_connection_lifetimes() {
    let event = Event::<Payload, Mask>::default();
    let clones = Arc::new(AtomicUsize::new(0));
    let selected = Arc::new(AtomicUsize::new(0));
    let ordinary = selected.clone();
    let tracked = selected.clone();
    let connection = event.add_labelled_tracked_connection(
        Some(Mask(3)),
        move |_| {
            ordinary.fetch_add(1, Ordering::SeqCst);
        },
        move |_| {
            tracked.fetch_add(1, Ordering::SeqCst);
            std::future::ready(Ok(()))
        },
    );
    event.add_labelled_tracked_connection(
        Some(Mask(2)),
        |_| panic!("rejected"),
        |_| async { panic!("rejected") },
    );
    let wildcards = Arc::new(AtomicUsize::new(0));
    let calls = wildcards.clone();
    event.add_connection(move |_| {
        calls.fetch_add(1, Ordering::SeqCst);
    });

    event.emit_labelled(Mask(1), Payload(clones.clone()));
    let delivery = event.emit_labelled_tracked(Mask(1), Payload(clones.clone()));
    // Both matching and submission happen before the future is polled.
    assert_eq!(selected.load(Ordering::SeqCst), 2);
    assert_eq!(wildcards.load(Ordering::SeqCst), 2);
    assert_eq!(clones.load(Ordering::SeqCst), 4);
    block_on(delivery).unwrap();

    // Cloning a connection does not require a cloneable label or payload type.
    drop(connection.clone());
    event.emit_labelled(Mask(1), Payload(clones.clone()));
    assert_eq!(selected.load(Ordering::SeqCst), 3);
    let scoped = connection.scoped();
    drop(scoped.clone());
    event.emit_labelled(Mask(1), Payload(clones));
    assert_eq!(selected.load(Ordering::SeqCst), 3);
}

struct PanicLabel;
impl EventLabel for PanicLabel {
    fn matches(&self, _: &Self) -> bool {
        panic!("matching failed")
    }
}

#[test]
fn tracked_matching_panics_do_not_prevent_other_deliveries() {
    let event = Event::<(), PanicLabel>::default();
    event.add_labelled_tracked_connection(
        Some(PanicLabel),
        |_| panic!("must not dispatch"),
        |_| async { panic!("must not dispatch") },
    );
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    event.add_connection(move |_| {
        seen.fetch_add(1, Ordering::SeqCst);
    });
    assert_eq!(
        block_on(event.emit_labelled_tracked(PanicLabel, ())),
        Err(DeliveryError::Panicked)
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

struct ReentrantLabel(Option<Box<dyn Fn() + Send + Sync>>);
impl EventLabel for ReentrantLabel {
    fn matches(&self, _: &Self) -> bool {
        if let Some(callback) = &self.0 {
            callback();
        }
        true
    }
}

#[test]
fn matching_runs_outside_lock_and_uses_connection_snapshot() {
    let event = Event::<(), ReentrantLabel>::default();
    let calls = Arc::new(AtomicUsize::new(0));
    let added_calls = calls.clone();
    let added_event = event.clone();
    let connection = event.add_labelled_tracked_connection(
        Some(ReentrantLabel(Some(Box::new(move || {
            let seen = added_calls.clone();
            added_event.add_connection(move |_| {
                seen.fetch_add(1, Ordering::SeqCst);
            });
        })))),
        |_| {},
        |_| std::future::ready(Ok(())),
    );
    event.emit_labelled(ReentrantLabel(None), ());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    block_on(event.emit_labelled_tracked(ReentrantLabel(None), ())).unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    connection.disconnect(); // Break the test callback's owning event cycle.
}

#[events]
trait Updates {
    #[with_label(Mask)]
    fn changed(&self, label: usize, __eventful_target: usize);
    #[with_label(Mask)]
    fn ping(&self);
    fn ordinary(&self);
}

#[eventful(Updates, shard = DynamicShard)]
struct Receiver {
    count: std::cell::Cell<usize>,
}
impl Updates for Receiver {
    fn changed(&self, label: usize, __eventful_target: usize) {
        self.count.set(self.count.get() + label + __eventful_target);
    }
    fn ping(&self) {
        self.count.set(self.count.get() + 10);
    }
    fn ordinary(&self) {
        self.count.set(self.count.get() + 100);
    }
}

#[test]
fn generated_labels_work_across_shards_with_bulk_connections_and_closed_targets() {
    let source_shard = shard::Shard::new("labels-source");
    let target_shard = shard::Shard::new("labels-target");
    let make = |bind: &dyn Fn(Receiver) -> ShardRc<Receiver>| {
        bind(Receiver {
            count: Default::default(),
            events: Default::default(),
        })
        .to_handle()
    };
    let source = source_shard.bind(make);
    let target = target_shard.bind(make);
    let connection = source.changed().connect_labelled(&target, Mask(3));
    source.ping().connect_labelled(&target, Mask(1));
    source.changed().emit(Mask(4), 999, 999); // Rejected.
    source.changed().emit(Mask(1), 1, 2);
    block_on(source.changed().emit_tracked(Mask(2), 3, 4)).unwrap();
    let emit_source = source.clone();
    block_on(
        source
            .downgrade()
            .try_deferred_upgrade_in_shard(async move |value| {
                value.emit_changed_tracked(Mask(1), 5, 6).await.unwrap();
                value.emit_ping(Mask(1));
                value.emit_ping_tracked(Mask(4)).await.unwrap();
                // Labels also work through signal access on a handle.
                emit_source.ping().emit_tracked(Mask(1)).await.unwrap();
            }),
    )
    .unwrap();
    assert_eq!(
        block_on(
            target
                .downgrade()
                .try_deferred_upgrade_in_shard(async |value| value.count.get())
        ),
        Ok(41)
    );

    connection.disconnect();
    let group = source.events().connect_events(&target);
    block_on(source.changed().emit_tracked(Mask(0), 1, 0)).unwrap();
    block_on(source.ordinary().emit_tracked()).unwrap();
    assert_eq!(
        block_on(
            target
                .downgrade()
                .try_deferred_upgrade_in_shard(async |value| value.count.get())
        ),
        Ok(142)
    );
    group.disconnect();
    target_shard.join().unwrap();
    // Rejection requires no live destination; an accepted emission reports closure.
    assert_eq!(block_on(source.ping().emit_tracked(Mask(2))), Ok(()));
    assert_eq!(
        block_on(source.ping().emit_tracked(Mask(1))),
        Err(DeliveryError::Closed)
    );
    source_shard.join().unwrap();
}
