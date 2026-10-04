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
    assert!(handle.try_get_handle(&Value { events }).is_none());
    let found = handle.try_get_handle(&*local).unwrap();
    assert_eq!(found.id(), local.to_handle().id());
    assert_eq!(found.shard_id(), handle.shard_id);
    assert!(Arc::ptr_eq(found.events(), local.events()));
    drop(local);
    assert!(store.borrow_mut().take_garbage().is_empty());
    let stored = store.borrow().get::<ShardValue<Value>>(found.id()).unwrap();
    assert_eq!(handle.try_get_handle(&**stored).unwrap().id(), found.id());
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
    assert!(handle.try_get_handle(&value).is_none());
    assert!(!crate::engine::has_context(handle.shard_id));

    let context = crate::engine::ContextGuard::new(handle.shard_id);
    let local = crate::engine::bind_here(
        &crate::engine::store(handle.shard_id),
        handle.clone(),
        |bind| bind(value),
    );
    let (other, _other_rx) = crate::ShardEventHandle::channel();
    let _other_context = crate::engine::ContextGuard::new(other.shard_id);
    assert!(other.try_get_handle(&*local).is_none());
    let value: &Value = &local;
    std::thread::scope(|scope| {
        scope.spawn(|| {
            assert!(handle.try_get_handle(value).is_none());
            assert!(!crate::engine::has_context(handle.shard_id));
        });
    });
    drop(context);
    assert!(handle.try_get_handle(&*local).is_none());
}
