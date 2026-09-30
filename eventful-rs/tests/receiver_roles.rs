use eventful_rs::*;
use futures::executor::block_on;
use std::{
    cell::RefCell,
    rc::Rc,
    sync::{Arc, Mutex},
};

struct A;
struct B;
trait ListenerKind {}
struct Label(u8);
impl EventLabel for Label {
    fn matches(&self, emitted: &Self) -> bool {
        self.0 == emitted.0
    }
}
#[events(ListenerKind)]
trait Buttons {
    fn clicked(&self);
    #[with_label(Label)]
    fn changed(&self, callback: String, tracked_callback: usize);
}
#[eventful(Buttons, shard = DynamicShard)]
struct Button;
#[eventful(shard = DynamicShard)]
struct Dialog {
    local: Rc<RefCell<usize>>,
    log: Arc<Mutex<Vec<String>>>,
    owner: std::thread::ThreadId,
}
impl ListenerKind for Dialog {}
impl Dialog {
    fn record(&self, text: String) {
        assert_eq!(self.owner, std::thread::current().id());
        *self.local.borrow_mut() += 1;
        self.log.lock().unwrap().push(text);
    }
    fn pressed(&self) {
        self.record("method".into());
    }
}
impl Buttons<A> for Dialog {
    fn clicked(&self) {
        self.record("A".into());
    }
    fn changed(&self, value: String, n: usize) {
        self.record(format!("A:{value}:{n}"));
    }
}
impl Buttons<B> for Dialog {
    fn clicked(&self) {
        self.record("B".into());
    }
    fn changed(&self, value: String, n: usize) {
        self.record(format!("B:{value}:{n}"));
    }
}
fn button(shard: &std_rt::Shard) -> ShardRcHandle<Button> {
    shard.bind(|bind| {
        bind(Button {
            events: Default::default(),
        })
        .to_handle()
    })
}
fn dialog(shard: &std_rt::Shard, log: Arc<Mutex<Vec<String>>>) -> ShardRcHandle<Dialog> {
    shard.bind(move |bind| {
        bind(Dialog {
            local: Rc::default(),
            log,
            owner: std::thread::current().id(),
            events: Default::default(),
        })
        .to_handle()
    })
}

#[test]
fn roles_and_callbacks_dispatch_to_non_send_receiver_on_its_shard() {
    let source = std_rt::Shard::new("role-source");
    let destination = std_rt::Shard::new("role-target");
    let log = Arc::new(Mutex::new(Vec::new()));
    let receiver = dialog(&destination, log.clone());
    let a = button(&source);
    let b = button(&source);
    a.clicked().connect_as::<A, _>(&receiver);
    b.clicked().connect_as::<B, _>(&receiver);
    a.clicked().connect_fn(&receiver, Dialog::pressed);
    let prefix = String::from("capture");
    a.changed().connect_fn(&receiver, move |d, value, n| {
        d.record(format!("{prefix}:{value}:{n}"))
    });
    // Ordinary delivery followed by a tracked barrier on the same destination.
    a.emit_clicked();
    block_on(b.emit_clicked_tracked()).unwrap();
    block_on(a.emit_changed_tracked(Label(9), "value".into(), 7)).unwrap();
    assert_eq!(
        *log.lock().unwrap(),
        ["A", "method", "B", "capture:value:7"]
    );
    let scoped = b
        .clicked()
        .connect_fn(&receiver, |_| panic!("disconnected"))
        .scoped();
    drop(scoped);
    block_on(b.emit_clicked_tracked()).unwrap();
    let panic_connection = b
        .clicked()
        .connect_fn(&receiver, |_| panic!("handler failure"));
    assert_eq!(
        block_on(b.emit_clicked_tracked()),
        Err(DeliveryError::Panicked)
    );
    panic_connection.disconnect();
    let weak_events = Arc::downgrade(receiver.events());
    drop(receiver);
    destination.join().unwrap();
    assert!(weak_events.upgrade().is_none());
    assert_eq!(
        block_on(a.emit_clicked_tracked()),
        Err(DeliveryError::Closed)
    );
    source.join().unwrap();
}

#[test]
fn labelled_roles_callbacks_and_bulk_handles_compose() {
    let source = std_rt::Shard::new("label-role-source");
    let destination = std_rt::Shard::new("label-role-target");
    let log = Arc::new(Mutex::new(Vec::new()));
    let receiver = dialog(&destination, log.clone());
    let a = button(&source);
    a.changed().connect_labelled_as::<A, _>(&receiver, Label(1));
    a.changed()
        .connect_labelled_fn(&receiver, Label(2), |d, s, n| {
            d.record(format!("fn:{s}:{n}"))
        });
    block_on(a.emit_changed_tracked(Label(3), "ignored".into(), 0)).unwrap();
    block_on(a.emit_changed_tracked(Label(1), "one".into(), 1)).unwrap();
    block_on(a.emit_changed_tracked(Label(2), "two".into(), 2)).unwrap();
    let strong = a.connect_as::<B, _>(&receiver);
    let weak = a.downgrade();
    let weak_group = weak.connect_as::<A, _>(&receiver).unwrap();
    let set_group = a.events().connect_events_as::<A, _>(&receiver);
    block_on(a.emit_clicked_tracked()).unwrap();
    strong.disconnect();
    weak_group.disconnect();
    set_group.disconnect();
    block_on(a.emit_clicked_tracked()).unwrap();
    assert_eq!(*log.lock().unwrap(), ["A:one:1", "fn:two:2", "B", "A", "A"]);
    drop(a);
    source.join().unwrap();
    assert!(weak.connect_as::<B, _>(&receiver).is_none());
    destination.join().unwrap();
}

#[test]
fn local_bulk_roles_disconnect_when_source_value_dies() {
    declare_shard!(Local, runtime = main);
    #[eventful(Buttons, shard = Local)]
    struct LocalButton;
    let destination = std_rt::Shard::new("owned-role-target");
    let log = Arc::new(Mutex::new(Vec::new()));
    let receiver = dialog(&destination, log.clone());
    let (local, token) = Local::shard().run_main(move || async move {
        let local = ShardRc::try_bind(LocalButton {
            events: Default::default(),
        })
        .unwrap();
        let token = ShardRc::connect_as::<A, _>(&local, &receiver);
        (local, token)
    });
    let events = local.events().clone();
    block_on(events.clicked().emit_tracked()).unwrap();
    drop(local);
    block_on(events.clicked().emit_tracked()).unwrap();
    assert_eq!(*log.lock().unwrap(), ["A"]);
    token.disconnect();
    destination.join().unwrap();
}

#[test]
fn default_roles_callback_only_receivers_and_marker_hygiene() {
    #[derive(Clone)]
    struct __EventfulRole;
    #[events]
    trait Updates {
        fn updated(&self, value: __EventfulRole);
    }
    // Marker types need not be Send, Sync, Clone, or Default.
    struct LocalRole {
        _local: Rc<()>,
    }
    #[eventful(Updates, shard = DynamicShard)]
    struct Source;
    #[eventful(shard = DynamicShard)]
    struct Receiver {
        seen: Arc<Mutex<Vec<&'static str>>>,
    }
    impl Updates for Receiver {
        fn updated(&self, _: __EventfulRole) {
            self.seen.lock().unwrap().push("default");
        }
    }
    impl Updates<LocalRole> for Receiver {
        fn updated(&self, _: __EventfulRole) {
            self.seen.lock().unwrap().push("role");
        }
    }
    impl Receiver {
        // An inherent method with the same name must not intercept trait dispatch.
        fn updated(&self, _: __EventfulRole) {
            self.seen.lock().unwrap().push("callback");
        }
    }
    #[eventful(shard = DynamicShard)]
    struct CallbackOnly {
        seen: Arc<Mutex<Vec<&'static str>>>,
    }
    impl ListenerKind for CallbackOnly {}
    let shard = std_rt::Shard::new("default-role");
    let seen = Arc::new(Mutex::new(Vec::new()));
    let source = shard.bind(|bind| {
        bind(Source {
            events: Default::default(),
        })
        .to_handle()
    });
    let out = seen.clone();
    let receiver = shard.bind(move |bind| {
        bind(Receiver {
            seen: out,
            events: Default::default(),
        })
        .to_handle()
    });
    let out = seen.clone();
    let callback_only = shard.bind(move |bind| {
        bind(CallbackOnly {
            seen: out,
            events: Default::default(),
        })
        .to_handle()
    });
    source.updated().connect(&receiver);
    source.updated().connect_as::<LocalRole, _>(&receiver);
    source.updated().connect_fn(&receiver, Receiver::updated);
    source
        .updated()
        .connect_fn(&callback_only, |r, _| r.seen.lock().unwrap().push("only"));
    block_on(source.emit_updated_tracked(__EventfulRole)).unwrap();
    assert_eq!(
        *seen.lock().unwrap(),
        ["default", "role", "callback", "only"]
    );
    let button = button(&shard);
    button.clicked().connect_fn(&callback_only, |r| {
        r.seen.lock().unwrap().push("extra-bound")
    });
    block_on(button.emit_clicked_tracked()).unwrap();
    assert_eq!(seen.lock().unwrap().last(), Some(&"extra-bound"));
    shard.join().unwrap();
}
