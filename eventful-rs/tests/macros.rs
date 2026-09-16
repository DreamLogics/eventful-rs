use eventful_rs::*;
use std::{cell::RefCell, rc::Rc};
shard_std!(WORKER);

mod declarations {
    #[eventful_rs::events]
    pub trait Updates {
        fn changed(&self, value: usize);
    }
}
use declarations::{UpdatesEmittersExt, UpdatesSignalsExt};

#[eventful(declarations::Updates)]
struct Counter {
    value: RefCell<usize>,
    local: Rc<()>,
}
#[asynchronize]
impl Counter {
    #[eventful_rs::action]
    fn set(&self, __eventful_target: usize) {
        self.value.replace(__eventful_target);
    }
    #[asynced]
    fn read(&self) -> usize {
        *self.value.borrow()
    }
    #[asynced]
    async fn increment(&self, amount: usize) -> usize {
        *self.value.borrow_mut() += amount;
        self.emit_changed(*self.value.borrow());
        *self.value.borrow()
    }
}
impl declarations::Updates for Counter {
    fn changed(&self, value: usize) {
        self.value.replace(value);
    }
}
#[test]
fn generated_dispatch_supports_non_send_values_and_events_between_shards() {
    let counter = WORKER.bind(|bind| {
        bind(Counter {
            value: RefCell::new(0),
            local: Rc::new(()),
            events: Default::default(),
        })
        .as_handle()
    });
    let target = WORKER.bind(|bind| {
        bind(Counter {
            value: RefCell::new(0),
            local: Rc::new(()),
            events: Default::default(),
        })
        .as_handle()
    });
    counter.changed().connect(&target);
    counter.set(4);
    assert_eq!(futures::executor::block_on(counter.read()), 4);
    assert_eq!(futures::executor::block_on(counter.increment(5)), 9);
    // Await the event using a oneshot, rather than assuming completion order.
    let (tx, rx) = futures::channel::oneshot::channel();
    target.upgrade_in_shard(move |value| {
        assert_eq!(Rc::strong_count(&value.local), 1);
        tx.send(()).unwrap();
    });
    futures::executor::block_on(rx).unwrap();
    // All synchronous operations are polled to completion when dispatched.
    assert_eq!(futures::executor::block_on(target.read()), 9);
    WORKER.join().unwrap();
}

#[test]
fn generated_tracked_signals_observe_cross_shard_completion_and_closed_targets() {
    let source_shard = shard::Shard::new("tracked-source");
    let target_shard = shard::Shard::new("tracked-target");
    let make_counter = |bind: &dyn Fn(Counter) -> ShardRc<Counter>| {
        bind(Counter {
            value: RefCell::new(0),
            local: Rc::new(()),
            events: Default::default(),
        })
        .as_handle()
    };
    let source = source_shard.bind(make_counter);
    let target = target_shard.bind(make_counter);
    source.changed().connect(&target);
    assert_eq!(
        futures::executor::block_on(source.emit_changed_tracked(12)),
        Ok(())
    );
    assert_eq!(futures::executor::block_on(target.read()), 12);
    // Submission happens even if the completion future is dropped.
    drop(source.changed().emit_tracked(23));
    assert_eq!(futures::executor::block_on(target.read()), 23);
    target_shard.join().unwrap();
    assert_eq!(
        futures::executor::block_on(source.changed().emit_tracked(99)),
        Err(DeliveryError::Closed)
    );
    source.changed().emit(99);
    source_shard.join().unwrap();
}
