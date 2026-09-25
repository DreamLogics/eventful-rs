use eventful_rs::*;
use futures::executor::block_on;
use std::{
    rc::Rc,
    sync::atomic::{AtomicBool, Ordering},
};

declare_shard!(pub Worker, runtime = std);
declare_shard!(pub Other, runtime = std);

#[scope(shard = super::Worker)]
mod models {
    #[eventful_rs::eventful]
    pub struct Local {
        pub value: std::rc::Rc<usize>,
        pub owner: std::thread::ThreadId,
    }
    impl Local {
        pub fn new() -> Self {
            Self {
                value: std::rc::Rc::new(42),
                owner: std::thread::current().id(),
                events: Default::default(),
            }
        }
    }
    #[eventful_rs::eventful(shard = super::Other)]
    pub struct Override;
    impl Override {
        pub fn new() -> Self {
            Self {
                events: Default::default(),
            }
        }
    }

    pub mod nested {
        #[eventful_rs::eventful]
        pub struct Inherited;
    }
    #[eventful_rs::scope(shard = crate::Other)]
    pub mod overridden {
        #[eventful_rs::eventful]
        pub struct Selected;
    }
}

#[test]
fn named_affinity_scopes_factories_and_runtime_checks() {
    fn on_worker<T: Eventful<Shard = Worker>>() {}
    fn on_other<T: Eventful<Shard = Other>>() {}
    on_worker::<models::Local>();
    on_worker::<models::nested::Inherited>();
    on_other::<models::Override>();
    on_other::<models::overridden::Selected>();

    let caller = std::thread::current().id();
    let state = block_on(models::Local::spawn(models::Local::new)).unwrap();
    assert_eq!(
        block_on(
            Worker::handle().try_deferred_invoke(state.clone(), async move |s| {
                assert_ne!(s.owner, caller);
                assert_eq!(s.owner, std::thread::current().id());
                assert_eq!(Rc::strong_count(&s.value), 1);
                // Local conversion remains available for non-Send values.
                let local: ShardRc<models::Local> = models::Local::new().into();
                assert_eq!(*local.value, 42);
                *s.value
            })
        ),
        Ok(42)
    );

    let second = block_on(Worker::bind_async(|bind| {
        bind(models::Local::new()).as_handle()
    }))
    .unwrap();
    assert!(state.join(&second).is_some());
    let other = block_on(models::Override::spawn(models::Override::new)).unwrap();
    assert!(state.join(&other).is_none());

    static CALLED: AtomicBool = AtomicBool::new(false);
    let result = block_on(Other::handle().bind_async(|bind| {
        CALLED.store(true, Ordering::SeqCst);
        bind(models::Local::new()).as_handle()
    }));
    assert!(matches!(result, Err(InvokeError::WrongShard)));
    assert!(!CALLED.load(Ordering::SeqCst));
    assert!(
        std::panic::catch_unwind(|| {
            Other::shard().bind(|bind| bind(models::Local::new()).as_handle());
        })
        .is_err()
    );
    // Also reject a mismatched bind when already executing on its destination.
    assert_eq!(
        block_on(
            Other::handle().try_deferred_invoke(other.clone(), async |_| {
                Other::shard().bind(|bind| bind(models::Local::new()).as_handle());
            })
        ),
        Err(InvokeError::Panicked)
    );

    assert!(matches!(
        block_on(models::Local::spawn(|| panic!("factory panic"))),
        Err(InvokeError::Panicked)
    ));
    Worker::shard().join().unwrap();
    assert!(matches!(
        block_on(models::Local::spawn(models::Local::new)),
        Err(InvokeError::Closed)
    ));
    Other::shard().join().unwrap();
}

#[cfg(feature = "tokio")]
#[test]
fn named_tokio_main_constructs_non_send_values_on_its_owner() {
    declare_shard!(Main, runtime = tokio_main);
    #[eventful(shard = Main)]
    struct Local {
        value: Rc<usize>,
    }
    let owner = std::thread::current().id();
    Main::shard().run_main(move || async move {
        let state = Local::spawn(|| Local {
            value: Rc::new(7),
            events: Default::default(),
        })
        .await
        .unwrap();
        assert_eq!(
            Main::handle()
                .try_deferred_invoke(state, async move |s| {
                    assert_eq!(std::thread::current().id(), owner);
                    ::tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                    *s.value
                })
                .await,
            Ok(7)
        );
    });
}
