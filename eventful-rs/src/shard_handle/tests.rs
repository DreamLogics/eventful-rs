//! Handle ownership and store collection regression tests.

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
    let weak = local.as_handle().downgrade();
    let events = Arc::downgrade(&local.events);
    drop(local);
    drop(store.take_garbage());
    assert!(events.upgrade().is_none());
    assert!(weak.events.upgrade().is_none());
}
