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
fn generated_dispatch_supports_non_send_objects_and_cross_object_events() {
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
