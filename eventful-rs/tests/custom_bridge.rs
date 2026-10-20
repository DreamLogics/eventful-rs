//! Third-party bridges build on the documented `{Trait}Emissions` contract,
//! like the Slint bridge does, without access to generated internals.
use eventful_rs::*;
use futures::executor::block_on;
use std::{cell::RefCell, sync::Arc};

declare_shard!(Worker, runtime = std);

mod keys {
    #[eventful_rs::events]
    pub trait Keys {
        fn pressed(&self, key: char);
    }
}
use keys::{Keys, KeysEmissions, KeysEventSet, KeysSignalsExt};

/// What an external macro would generate: an input adapter owning emission access.
#[derive(Default)]
struct KeyBridge {
    events: KeysEmissions,
}
impl KeyBridge {
    /// Forward bound keys and report synchronously whether one matched.
    fn dispatch(&self, key: char) -> bool {
        if !key.is_ascii_lowercase() {
            return false;
        }
        self.events.pressed().emit(key);
        true
    }
}
impl HasEvents<KeysEventSet> for KeyBridge {
    fn events(&self) -> &Arc<KeysEventSet> {
        self.events.signals()
    }
}
impl KeysSignalsExt for KeyBridge {}

#[eventful(shard = Worker)]
struct Recorder {
    keys: RefCell<String>,
}
impl Keys for Recorder {
    fn pressed(&self, key: char) {
        self.keys.borrow_mut().push(key);
    }
}
#[asynchronize]
impl Recorder {
    #[asynced]
    fn keys(&self) -> String {
        self.keys.borrow().clone()
    }
}

#[test]
fn external_bridge_emits_through_generated_emissions() {
    let recorder = block_on(Recorder::spawn(|| Recorder {
        keys: RefCell::default(),
        events: Default::default(),
    }))
    .unwrap();
    let bridge = KeyBridge::default();
    let _connection = bridge.pressed().connect(&recorder).scoped();

    assert!(bridge.dispatch('q'));
    assert!(!bridge.dispatch('Q'));
    block_on(bridge.events.pressed().tracked().emit('w')).unwrap();
    assert_eq!(block_on(recorder.keys()), "qw");

    // Bulk connection through the subscription view works as well.
    let _all = bridge
        .events
        .signals()
        .role::<()>()
        .connect(&recorder)
        .scoped();
    block_on(bridge.events.pressed().tracked().emit('e')).unwrap();
    assert_eq!(block_on(recorder.keys()), "qwee");
    Worker::shard().join().unwrap();
}
