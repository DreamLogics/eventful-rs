//! Builder routing, ownership, and capability contracts.
use eventful_rs::*;
use futures::executor::block_on;
use std::sync::{Arc, Mutex};

// Labels intentionally implement neither Clone nor Debug.
struct Topic(u8);
impl EventLabel for Topic {
    fn matches(&self, emitted: &Self) -> bool {
        self.0 == emitted.0
    }
}
#[events]
trait Updates {
    fn plain(&self, callback: usize);
    #[with_label(Topic)]
    fn changed(&self, callback: usize);
}
#[eventful(Updates, shard = DynamicShard)]
struct Source;

#[test]
fn standalone_callbacks_route_filter_and_complete_without_a_receiver() {
    let shard = std_rt::Shard::new("callbacks");
    let source = Source {
        events: Default::default(),
    };
    let seen = Arc::new(Mutex::new(Vec::new()));
    let owner = block_on(shard.handle().bind_async(|bind| {
        let _source = bind(Source {
            events: Default::default(),
        });
        std::thread::current().id()
    }))
    .unwrap();
    let output = seen.clone();
    let selected = source
        .events
        .changed()
        .signal()
        .on_shard(&shard.handle())
        .labelled(Topic(1))
        .connect(move |value| {
            assert_eq!(std::thread::current().id(), owner);
            output.lock().unwrap().push(value);
        });
    let output = seen.clone();
    let wildcard = source
        .events
        .changed()
        .signal()
        .on_shard(&shard.handle())
        .connect(move |value| output.lock().unwrap().push(value * 10));
    block_on(source.events.changed().tracked().labelled(Topic(2)).emit(2)).unwrap();
    block_on(source.events.changed().labelled(Topic(1)).tracked().emit(3)).unwrap();
    assert_eq!(*seen.lock().unwrap(), [20, 3, 30]);
    wildcard.disconnect();
    selected.disconnect();
    let output = seen.clone();
    let guard = source
        .events
        .plain()
        .signal()
        .on_shard(&shard.handle())
        .connect(move |value| output.lock().unwrap().push(value))
        .scoped();
    // The returned observer neither borrows the source nor defers submission.
    let completion = source.events.plain().tracked().emit(4);
    drop(completion);
    block_on(shard.handle().try_invoke_tracked(|| {})).unwrap();
    drop(guard);
    block_on(source.events.plain().tracked().emit(5)).unwrap();
    assert_eq!(*seen.lock().unwrap(), [20, 3, 30, 4]);
    shard.join().unwrap();
}

#[test]
fn tracked_standalone_callbacks_report_panics_and_closed_shards() {
    let shard = std_rt::Shard::new("callback-errors");
    let source = Source {
        events: Default::default(),
    };
    let panic = source
        .events
        .plain()
        .signal()
        .on_shard(&shard.handle())
        .connect(|_| panic!("callback"));
    assert_eq!(
        block_on(source.events.plain().tracked().emit(1)),
        Err(DeliveryError::Panicked)
    );
    panic.disconnect();
    source
        .events
        .changed()
        .signal()
        .labelled(Topic(1))
        .on_shard(&shard.handle())
        .connect(|_| {});
    shard.join().unwrap();
    assert_eq!(
        block_on(source.events.changed().labelled(Topic(2)).tracked().emit(1)),
        Ok(())
    );
    assert_eq!(
        block_on(source.events.changed().labelled(Topic(1)).tracked().emit(1)),
        Err(DeliveryError::Closed)
    );
    source.events.changed().labelled(Topic(1)).emit(1); // Untracked closed delivery is skipped.
}

#[test]
fn completion_future_does_not_retain_emission_access() {
    fn send_static(_: impl Future<Output = Result<(), DeliveryError>> + Send + 'static) {}
    let source = Source {
        events: Default::default(),
    };
    let completion = source.events.plain().tracked().emit(0);
    drop(source);
    send_static(completion);
}

mod paths {
    use super::*;
    #[derive(Clone)]
    pub(super) struct Payload;
    pub(super) struct Label;
    impl EventLabel for Label {
        fn matches(&self, _: &Self) -> bool {
            true
        }
    }
    mod interface {
        #[eventful_rs::events]
        pub(super) trait Messages {
            #[with_label(super::Label)]
            fn changed(&self, payload: super::Payload);
        }
        #[eventful_rs::eventful(Messages, shard = eventful_rs::DynamicShard)]
        pub(super) struct Source;
        impl Source {
            pub(super) fn check() {
                let source = Self {
                    events: Default::default(),
                };
                source
                    .events
                    .changed()
                    .labelled(super::Label)
                    .emit(super::Payload);
            }
        }
    }
    #[test]
    fn relative_paths_and_restricted_visibility_survive_generated_module() {
        interface::Source::check();
    }
}
