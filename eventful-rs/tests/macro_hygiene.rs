//! Generated code must not depend on unqualified prelude names, and dispatch
//! wrappers must forward signatures that use binding modes or `Self`.
use std::sync::mpsc;

mod shadowed {
    #![allow(dead_code)]
    use eventful_rs::*;
    use std::cell::Cell;

    // User items that shadow prelude names used by generated code.
    pub type Result<T> = std::result::Result<T, String>;
    pub trait Default {}
    pub trait Clone {}
    pub trait Send {}
    pub trait Sync {}
    pub trait Fn {}
    pub trait Sized {}
    pub trait Debug {}
    #[allow(unused_macros)]
    macro_rules! stringify {
        ($($tokens:tt)*) => {
            compile_error!("generated code used an unqualified stringify!")
        };
    }

    #[events]
    pub trait Hygiene {
        fn changed(&self, value: u32);
    }

    // A trailing comma is accepted after the runtime selection.
    declare_shard!(pub Worker, runtime = std,);

    #[eventful(Hygiene, shard = Worker)]
    pub struct Source {
        pub value: Cell<u32>,
    }

    impl Source {
        pub fn new(value: u32) -> Self {
            Self {
                value: Cell::new(value),
                events: ::core::default::Default::default(),
            }
        }
    }

    #[asynchronize(pub)]
    impl Source {
        const WIDTH: usize = 2;

        #[action]
        pub fn bump(&self, mut amount: u32) {
            amount += self.value.get();
            self.value.set(amount);
            self.events.changed().emit(amount);
        }

        #[action]
        #[allow(clippy::toplevel_ref_arg)]
        pub fn set_ref(&self, ref amount: u32) {
            self.value.set(*amount);
        }

        #[asynced]
        pub async fn notify(&self) -> std::result::Result<(), DeliveryError> {
            self.events.changed().tracked().emit(self.value.get()).await
        }

        #[asynced]
        pub fn duplicate(&self, extra: Option<Self>) -> Option<Self> {
            let extra = extra.map_or(0, |source| source.value.get());
            Some(Self::new(self.value.get() + extra))
        }

        #[asynced]
        pub fn width(&self, values: [u8; Self::WIDTH]) -> usize {
            values.len()
        }

        #[asynced]
        #[deprecated(note = "use bump")]
        pub fn legacy(&self) -> u32 {
            self.value.get()
        }

        #[asynced]
        #[expect(unused_variables)]
        pub fn ignore(&self, value: u32) -> u32 {
            self.value.get()
        }
    }
}

#[eventful_rs::sharded(shard = eventful_rs::DynamicShard)]
mod scoped {
    #[eventful_rs::eventful]
    pub struct Top;

    pub mod nested {
        pub mod deeper {
            #[eventful_rs::eventful]
            pub struct Deep;
        }
    }
}

#[test]
fn sharded_scopes_accept_external_crate_paths_in_nested_modules() {
    fn dynamic<T: eventful_rs::Eventful<Shard = eventful_rs::DynamicShard>>() {}
    dynamic::<scoped::Top>();
    dynamic::<scoped::nested::deeper::Deep>();
}

#[test]
fn generated_code_ignores_shadowed_prelude_names() {
    use eventful_rs::*;
    use shadowed::{HygieneSignalsExt, Source, SourceAsync, Worker};

    let source = futures::executor::block_on(Source::spawn(|| Source::new(1))).unwrap();
    let (tx, rx) = mpsc::channel();
    let _connection = source
        .changed()
        .on_shard(&<Worker as ShardBinding>::handle())
        .connect(move |value| tx.send(value).unwrap());

    source.bump(2);
    assert_eq!(rx.recv().unwrap(), 3);
    source.set_ref(5);
    futures::executor::block_on(source.notify()).unwrap();
    assert_eq!(rx.recv().unwrap(), 5);

    let copy = futures::executor::block_on(source.duplicate(Some(Source::new(10)))).unwrap();
    assert_eq!(copy.value.get(), 15);
    assert_eq!(futures::executor::block_on(source.width([1, 2])), 2);
    #[allow(deprecated)]
    let legacy = futures::executor::block_on(source.legacy());
    assert_eq!(legacy, 5);
    assert_eq!(futures::executor::block_on(source.ignore(0)), 5);

    drop(source);
    Worker::shard().join().unwrap();
}
