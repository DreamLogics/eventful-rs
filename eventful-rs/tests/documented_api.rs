//! Ensure generated public APIs work in applications that require documentation.
#![deny(missing_docs)]

use eventful_rs::*;

declare_shard!(pub Worker, runtime = std);

/// Notifications published by a documented application type.
#[events]
pub trait Updates {
    /// Report the current value.
    fn changed(&self, value: usize);
}

/// Example value whose generated public items must also have documentation.
#[eventful(Updates, shard = Worker)]
pub struct Counter;

#[asynchronize]
impl Counter {
    /// Read the current value.
    #[asynced]
    pub fn value(&self) -> usize {
        1
    }

    /// Queue a notification.
    #[action]
    pub fn notify(&self) {
        self.emit_changed(1);
    }
}

/// An eventful value without an explicit interface also generates documented items.
#[eventful(shard = Worker)]
pub struct Empty;

#[test]
fn generated_wrappers_are_usable_with_missing_docs_denied() {
    let counter = futures::executor::block_on(Counter::spawn(|| Counter {
        events: Default::default(),
    }))
    .unwrap();
    counter.notify();
    assert_eq!(futures::executor::block_on(counter.value()), 1);
    Worker::shard().join().unwrap();
}
