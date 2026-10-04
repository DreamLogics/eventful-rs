//! Handle ownership and store collection regression tests.
use crate::Sharded;

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
    let value = value();
    let token = store.insert(value.clone());
    assert!(store.take_garbage().is_empty());
    drop(token);
    assert!(store.take_garbage().is_empty());
    drop(value);
    assert_eq!(store.take_garbage().len(), 1);
    assert!(store.take_garbage().is_empty());
}
fn value() -> Rc<ShardValue<Value>> {
    Rc::new(ShardValue::new(Value {
        events: Arc::new(()),
    }))
}

#[test]
fn only_the_last_release_queues_a_candidate_and_wakes_the_driver() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::{Context, Poll};
    struct Flag(AtomicBool);
    impl futures::task::ArcWake for Flag {
        fn wake_by_ref(this: &Arc<Self>) {
            this.0.store(true, Ordering::SeqCst);
        }
    }
    let flag = Arc::new(Flag(AtomicBool::new(false)));
    let waker = futures::task::waker(flag.clone());
    let mut cx = Context::from_waker(&waker);

    let mut store = ShardRcStore::new();
    let signal = store.signal();
    let token = store.insert(value());
    assert!(signal.poll_candidates(&mut cx).is_pending());
    let remote = token.clone();
    drop(token);
    assert!(
        !flag.0.load(Ordering::SeqCst),
        "a remaining token defers collection"
    );
    assert!(signal.poll_candidates(&mut cx).is_pending());

    std::thread::spawn(move || drop(remote)).join().unwrap();
    assert!(
        flag.0.load(Ordering::SeqCst),
        "a remote release wakes the owner"
    );
    assert_eq!(signal.poll_candidates(&mut cx), Poll::Ready(()));
    assert_eq!(store.take_garbage().len(), 1);
    assert!(signal.poll_candidates(&mut cx).is_pending());
}

#[test]
fn borrowed_values_are_rechecked_as_orphans() {
    let mut store = ShardRcStore::new();
    let token = store.insert(value());
    let borrowed = store.get::<ShardValue<Value>>(token.key()).unwrap();
    drop(token);
    assert!(store.take_garbage().is_empty());
    assert!(store.has_orphans());
    drop(borrowed);
    assert_eq!(store.take_garbage().len(), 1);
    assert!(!store.has_orphans());
}

#[test]
fn reacquired_values_are_not_collected_until_released_again() {
    let mut store = ShardRcStore::new();
    let token = store.insert(value());
    let weak = token.downgrade();
    drop(token);
    let again = ShardRcId::upgrade(&weak).unwrap();
    assert!(store.take_garbage().is_empty());
    assert!(!store.has_orphans());
    drop(again);
    assert_eq!(store.take_garbage().len(), 1);
    assert!(ShardRcId::upgrade(&weak).is_none());
}

#[test]
fn weak_handle_does_not_keep_event_set_alive() {
    let (handle, _rx) = crate::ShardEventHandle::channel();
    let mut store = ShardRcStore::new();
    let value = Rc::new(ShardValue::new(Value {
        events: Arc::new(()),
    }));
    let id = store.insert(value.clone());
    let local = ShardRc::new(id, value, handle);
    let weak = local.to_handle().downgrade();
    let events = Arc::downgrade(&local.events);
    drop(local);
    drop(store.take_garbage());
    assert!(events.upgrade().is_none());
    assert!(weak.events.upgrade().is_none());
}

#[test]
fn lookup_matches_value_identity_and_retains_the_entry() {
    let (handle, _rx) = crate::ShardEventHandle::channel();
    let _context = crate::engine::ContextGuard::new(handle.shard_id);
    let store = crate::engine::store(handle.shard_id);
    let events = Arc::new(());
    let local = crate::engine::bind_here(&store, handle.clone(), |bind| {
        bind(Value {
            events: events.clone(),
        })
    });
    // Sharing an event interface does not make another value the same target.
    assert_eq!(
        handle.find_local(&Value { events }).err(),
        Some(crate::InvokeError::ValueMissing)
    );
    let found = handle.find_local(&*local).unwrap().to_handle();
    assert_eq!(found.id(), local.to_handle().id());
    assert_eq!(found.shard_id(), handle.shard_id);
    assert!(Arc::ptr_eq(found.events(), local.events()));
    drop(local);
    assert!(store.borrow_mut().take_garbage().is_empty());
    let stored = store.borrow().get::<ShardValue<Value>>(found.id()).unwrap();
    assert_eq!(
        handle.find_local(&**stored).unwrap().to_handle().id(),
        found.id()
    );
    drop(stored);
    drop(found);
    assert_eq!(store.borrow_mut().take_garbage().len(), 1);
}

#[test]
fn disconnected_owned_groups_are_pruned_on_reconnect() {
    let value = ShardValue::new(Value {
        events: Arc::new(()),
    });
    let event = crate::Event::<()>::default();
    for _ in 0..100 {
        let group: crate::ConnectionGroup = [event.add_connection(|()| {})].into_iter().collect();
        value.own_connections(group.clone());
        group.disconnect();
    }
    assert_eq!(value.connections.borrow().len(), 1);
    assert_eq!(event.connection_count(), 0);

    let live: crate::ConnectionGroup = [event.add_connection(|()| {})].into_iter().collect();
    value.own_connections(live);
    assert_eq!(value.connections.borrow().len(), 1);
    assert_eq!(event.connection_count(), 1);
    drop(value); // Owned groups still disconnect with the value.
    assert_eq!(event.connection_count(), 0);
}

#[test]
fn lookup_requires_the_matching_thread_local_store() {
    let (handle, _rx) = crate::ShardEventHandle::channel();
    let value = Value {
        events: Arc::new(()),
    };
    assert_eq!(
        handle.find_local(&value).err(),
        Some(crate::InvokeError::WrongShard)
    );
    assert!(!crate::engine::has_context(handle.shard_id));

    let context = crate::engine::ContextGuard::new(handle.shard_id);
    let local = crate::engine::bind_here(
        &crate::engine::store(handle.shard_id),
        handle.clone(),
        |bind| bind(value),
    );
    let (other, _other_rx) = crate::ShardEventHandle::channel();
    let _other_context = crate::engine::ContextGuard::new(other.shard_id);
    assert_eq!(
        other.find_local(&*local).err(),
        Some(crate::InvokeError::ValueMissing)
    );
    let value: &Value = &local;
    std::thread::scope(|scope| {
        scope.spawn(|| {
            assert_eq!(
                handle.find_local(value).err(),
                Some(crate::InvokeError::WrongShard)
            );
            assert!(!crate::engine::has_context(handle.shard_id));
        });
    });
    drop(context);
    assert_eq!(
        handle.find_local(&*local).err(),
        Some(crate::InvokeError::WrongShard)
    );
}

#[test]
fn lookup_forgets_collected_addresses() {
    let (handle, _rx) = crate::ShardEventHandle::channel();
    let _context = crate::engine::ContextGuard::new(handle.shard_id);
    let store = crate::engine::store(handle.shard_id);
    // Bind and collect repeatedly; a reused allocation address must resolve
    // to the live value, never to a collected entry.
    for _ in 0..32 {
        let local = crate::engine::bind_here(&store, handle.clone(), |bind| {
            bind(Value {
                events: Arc::new(()),
            })
        });
        let found = handle.find_local(&*local).unwrap();
        assert_eq!(found.to_handle().id(), local.to_handle().id());
        drop((found, local));
        assert_eq!(store.borrow_mut().take_garbage().len(), 1);
    }
}
