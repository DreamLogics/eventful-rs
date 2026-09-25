//! API contracts from the Rust API Guidelines that need compile/runtime coverage.
use eventful_rs::*;
use futures::executor::block_on;
use std::{cell::Cell, fmt::Debug};

#[events]
trait Updates {
    fn changed(&self, value: usize);
    #[cfg(any())]
    #[with_label(MissingLabel)]
    fn absent(&self, value: MissingPayload);
    #[cfg_attr(all(), cfg_attr(all(), cfg(any())))]
    fn also_absent(&self, value: MissingPayload);
}

#[eventful(Updates, shard = DynamicShard)]
struct State {
    value: Cell<usize>,
}
impl Updates for State {
    fn changed(&self, value: usize) {
        self.value.set(value);
    }
}

#[asynchronize]
impl State {
    #[cfg(any())]
    #[asynced]
    fn absent(&self) -> MissingPayload {
        unreachable!()
    }
    #[cfg_attr(all(), cfg(any()))]
    #[action]
    fn also_absent(&self, value: MissingPayload) {
        let _ = value;
    }
    #[asynced]
    fn read(&self) -> usize {
        self.value.get()
    }
}

#[events]
#[cfg(any())]
trait MissingInterface {
    fn missing(&self, value: MissingPayload);
}
#[eventful(shard = DynamicShard)]
#[cfg_attr(all(), cfg(any()))]
struct MissingState;
#[asynchronize]
#[cfg(any())]
impl MissingState {
    #[asynced]
    fn missing(&self) -> MissingPayload {
        unreachable!()
    }
}

fn debug<T: Debug>(value: &T) {
    assert!(!format!("{value:?}").is_empty());
}

#[test]
fn conditional_methods_do_not_leak_into_generated_items() {
    let shard = shard::Shard::new("cfg-methods");
    let state = shard.bind(|bind| {
        bind(State {
            value: Cell::new(0),
            events: Default::default(),
        })
        .to_handle()
    });
    let _group = state.connect(&state).scoped();
    block_on(state.emit_changed_tracked(7)).unwrap();
    assert_eq!(block_on(state.read()), 7);
    debug(&shard);
    debug(&state);
    debug(&state.downgrade());
    debug(&state.join(&state).unwrap());
    debug(state.events());
    debug(state.changed());
    shard.join().unwrap();
}

#[test]
fn event_and_connections_do_not_require_debug_payloads() {
    #[derive(Clone)]
    struct Payload;
    let event = Event::<Payload>::default();
    debug(&event);
    let connection = event.add_connection(|_| {});
    debug(&connection);
    let scoped = connection.clone().scoped();
    debug(&scoped);
    let group: ConnectionGroup = [connection].into_iter().collect();
    debug(&group);
    debug(&group.scoped());
    debug(&task::TaskCancellationToken::new());
}

#[test]
fn local_binding_is_fallible_and_preserves_application_methods() {
    declare_shard!(Main, runtime = main);
    file_scope!(shard = Main);
    #[eventful]
    struct Local;
    impl Local {
        fn connect(&self) -> usize {
            42
        }
    }
    let value = Local {
        events: Default::default(),
    };
    assert!(matches!(
        ShardRc::try_bind(value),
        Err(InvokeError::WrongShard)
    ));
    Main::shard().run_main(|| async {
        let local = ShardRc::try_bind(Local {
            events: Default::default(),
        })
        .unwrap();
        assert_eq!(local.connect(), 42); // No inherent smart-pointer method shadows T.
        debug(&local);
        debug(&local.to_handle());
        let _subscriptions = ShardRc::connect(&local, &local).scoped();
    });
}

mod qualified {
    use super::*;
    #[eventful(shard = DynamicShard)]
    pub(super) struct Value;
    impl Value {
        pub(super) fn new() -> Self {
            Self {
                events: Default::default(),
            }
        }
    }
}
#[asynchronize(pub(crate))]
impl qualified::Value {
    #[asynced]
    fn answer(&self) -> usize {
        42
    }
}

#[test]
fn qualified_types_and_restricted_wrapper_visibility_work() {
    let shard = shard::Shard::new("qualified-impl");
    let value = shard.bind(|bind| bind(qualified::Value::new()).to_handle());
    assert_eq!(block_on(value.answer()), 42);
    shard.join().unwrap();
}

// A doc attribute before nested cfg_attr must not leave stray implementations.
declare_shard!(
    /// An unavailable backend must not be referenced when the marker is disabled.
    #[cfg_attr(all(), cfg_attr(all(), cfg(any())))]
    Disabled, runtime = slint
);

#[events]
#[allow(dead_code)]
trait RawNames {
    fn r#type(&self, value: usize);
}

#[test]
fn runtime_types_have_thread_safe_debuggable_handles() {
    fn shareable<T: Send + Sync + Debug>() {}
    fn debuggable<T: Debug>() {}
    shareable::<ShardRcHandle<State>>();
    shareable::<ShardWeakHandle<State>>();
    shareable::<ShardEventHandle>();
    shareable::<InvokeError>();
    shareable::<ShardError>();
    debuggable::<local::LocalShard>();
    #[cfg(feature = "tokio")]
    {
        debuggable::<tokio::TokioShard>();
        debuggable::<tokio_local::TokioLocalShard>();
    }
    #[cfg(feature = "slint")]
    debuggable::<slint::SlintShard>();
}
