//! Private event interfaces must support private payloads in tracked emissions.
use eventful_rs::*;
use futures::executor::block_on;

#[derive(Clone)]
struct Payload(usize);

struct Label;
impl EventLabel for Label {
    fn matches(&self, _: &Self) -> bool {
        true
    }
}

#[events]
trait Updates {
    fn changed(&self, value: Payload);
    #[with_label(Label)]
    fn labelled(&self, value: Payload);
}

#[eventful(Updates, shard = DynamicShard)]
struct Listener {
    count: std::cell::Cell<usize>,
}

impl Updates for Listener {
    fn changed(&self, value: Payload) {
        self.count.set(self.count.get() + value.0);
    }
    fn labelled(&self, value: Payload) {
        self.changed(value);
    }
}

#[test]
fn private_payloads_support_tracked_dispatch() {
    let shard = shard::Shard::new("private-payload");
    let listener = shard.bind(|bind| {
        bind(Listener {
            count: std::cell::Cell::new(0),
            events: Default::default(),
        })
        .as_handle()
    });
    let _plain = listener.changed().connect(&listener).scoped();
    let _labelled = listener
        .labelled()
        .connect_labelled(&listener, Label)
        .scoped();
    block_on(listener.emit_changed_tracked(Payload(2))).unwrap();
    block_on(listener.emit_labelled_tracked(Label, Payload(3))).unwrap();
    assert_eq!(
        block_on(listener.deferred_upgrade_in_shard(async |value| value.count.get())),
        5
    );
    shard.join().unwrap();
}
