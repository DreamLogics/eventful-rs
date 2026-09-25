use eventful_rs::*;
use futures::executor::block_on;
use std::sync::{Arc, Mutex};

trait ListenerKind {}
#[events(ListenerKind)]
trait Updates {
    fn names(&self, names: Vec<String>);
    fn count(&self, count: usize);
    fn finished(&self);
}
#[eventful(Updates, shard = DynamicShard)]
struct Source;
#[eventful(shard = DynamicShard)]
struct Listener {
    log: Arc<Mutex<Vec<String>>>,
    owner: std::thread::ThreadId,
}
impl ListenerKind for Listener {}
impl Updates for Listener {
    fn names(&self, names: Vec<String>) {
        assert_eq!(std::thread::current().id(), self.owner);
        self.log.lock().unwrap().extend(names);
    }
    fn count(&self, count: usize) {
        self.log.lock().unwrap().push(count.to_string());
    }
    fn finished(&self) {
        self.log.lock().unwrap().push("done".into());
    }
}
fn emit(source: &ShardRcHandle<Source>) {
    block_on(source.emit_names_tracked(vec!["name".into()])).unwrap();
    block_on(source.emit_count_tracked(7)).unwrap();
    block_on(source.emit_finished_tracked()).unwrap();
}

#[test]
fn bulk_connections_deliver_and_disconnect_through_local_strong_and_weak_sources() {
    let a = shard::Shard::new("source");
    let b = shard::Shard::new("listener");
    let log = Arc::new(Mutex::new(Vec::new()));
    let output = log.clone();
    let target = b.bind(move |bind| {
        bind(Listener {
            log: output,
            owner: std::thread::current().id(),
            events: Default::default(),
        })
        .as_handle()
    });
    let source = a.bind(|bind| {
        bind(Source {
            events: Default::default(),
        })
        .as_handle()
    });
    let retained = source.count().connect(&target);
    let group = source.connect(&target);
    emit(&source);
    assert_eq!(*log.lock().unwrap(), ["name", "7", "7", "done"]);
    log.lock().unwrap().clear();
    group.clone().disconnect();
    group.disconnect(); // Stale clones are harmless and leave individual subscriptions intact.
    source.emit_names(vec!["ignored".into()]);
    source.emit_count(8);
    source.emit_finished();
    block_on(target.deferred_upgrade_in_shard(async |_| ()));
    assert_eq!(*log.lock().unwrap(), ["8"]);
    retained.disconnect();
    log.lock().unwrap().clear();

    // Dropping a plain group keeps all its subscriptions active.
    drop(source.connect(&target));
    emit(&source);
    assert_eq!(*log.lock().unwrap(), ["name", "7", "done"]);
    log.lock().unwrap().clear();

    let weak = source.downgrade();
    let scoped = weak.connect(&target).unwrap().scoped();
    drop(scoped.clone()); // Any scoped clone disconnects the group's subscriptions.
    drop(scoped);
    emit(&source);
    assert_eq!(*log.lock().unwrap(), ["name", "7", "done"]);
    log.lock().unwrap().clear();

    let destination = target.clone();
    let (local_source, local_group) = a.bind(move |bind| {
        let local = bind(Source {
            events: Default::default(),
        });
        let group = local.connect(&destination);
        (local.as_handle(), group)
    });
    emit(&local_source);
    assert_eq!(*log.lock().unwrap(), ["name", "7", "done"]);
    local_group.disconnect();
    log.lock().unwrap().clear();
    emit(&local_source);
    assert!(log.lock().unwrap().is_empty());

    // Empty event sets produce a valid no-op group, and a local target is accepted.
    b.bind(move |bind| {
        let local = bind(Listener {
            log,
            owner: std::thread::current().id(),
            events: Default::default(),
        });
        local.connect(&local).disconnect();
    });
    drop(source);
    a.join().unwrap();
    assert!(weak.connect(&target).is_none());
    b.join().unwrap();
}

#[test]
fn groups_do_not_keep_targets_alive() {
    let a = shard::Shard::new("weak-target-source");
    let b = shard::Shard::new("weak-target");
    let source = a.bind(|bind| {
        bind(Source {
            events: Default::default(),
        })
        .as_handle()
    });
    let target = b.bind(|bind| {
        bind(Listener {
            log: Arc::default(),
            owner: std::thread::current().id(),
            events: Default::default(),
        })
        .as_handle()
    });
    let group = source.connect(&target);
    let weak_events = Arc::downgrade(&target.events);
    drop(target);
    b.join().unwrap();
    assert!(weak_events.upgrade().is_none());
    assert_eq!(
        block_on(source.emit_finished_tracked()),
        Err(DeliveryError::Closed)
    );
    group.disconnect();
    a.join().unwrap();
}
