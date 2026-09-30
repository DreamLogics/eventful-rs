use eventful_rs::*;
use std::{cell::RefCell, rc::Rc};

mod declarations {
    #[eventful_rs::events]
    pub trait Updates {
        fn changed(&self, value: usize);
    }
}
use declarations::UpdatesSignalsExt;

#[eventful(declarations::Updates, shard = DynamicShard)]
struct GenericListener<T: AsRef<str> = String, const N: usize = 2>
where
    T: Clone,
{
    name: T,
    values: RefCell<[usize; N]>,
}

impl<T: AsRef<str> + Clone, const N: usize> declarations::Updates for GenericListener<T, N> {
    fn changed(&self, value: usize) {
        self.values.borrow_mut().fill(value);
    }
}

#[test]
fn generic_instances_deliver_events_across_shards() {
    let source_shard = std_rt::Shard::new("generic-source");
    let target_shard = std_rt::Shard::new("generic-target");
    let source = source_shard.bind(|bind| {
        bind(GenericListener::<String> {
            name: "source".into(),
            values: RefCell::new([0; 2]),
            events: Default::default(),
        })
        .to_handle()
    });
    // The target's generic field is non-Send and constructed on its own shard.
    let target = target_shard.bind(|bind| {
        bind(GenericListener::<Rc<str>, 3> {
            name: Rc::from("target"),
            values: RefCell::new([0; 3]),
            events: Default::default(),
        })
        .to_handle()
    });
    source.changed().connect(&target);
    futures::executor::block_on(source.deferred_upgrade_in_shard(async move |source| {
        source.events.changed().tracked().emit(42).await
    }))
    .unwrap();
    let values = futures::executor::block_on(target.deferred_upgrade_in_shard(async |value| {
        assert_eq!(value.name.as_ref(), "target");
        *value.values.borrow()
    }));
    assert_eq!(values, [42; 3]);
    source_shard.join().unwrap();
    target_shard.join().unwrap();
}

#[sharded(shard = DynamicShard)]
mod empty_interfaces {
    use super::*;

    #[eventful]
    pub struct Borrowed<'a, T: ?Sized = str> {
        pub value: &'a T,
    }

    #[eventful]
    pub struct Unit<const N: usize = 4>;

    #[test]
    fn empty_interfaces_preserve_lifetimes_unsized_parameters_and_const_defaults() {
        fn assert_eventful<T: Eventful + HasEvents<T::EventSetType>>(_: &T) {}

        let text = String::from("borrowed");
        let borrowed = Borrowed::<str> {
            value: text.as_str(),
            events: Default::default(),
        };
        assert_eventful(&borrowed);
        assert_eq!(borrowed.value, "borrowed");
        assert_eventful(&Unit::<4> {
            events: Default::default(),
        });
    }
}
