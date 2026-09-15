use std::thread;

use eventful_rs::events;
use eventful_rs::{EventLoop, ShardHandle, eventful, shard_std};

use crate::bar::BAR_SHARD;

shard_std!(FOO_SHARD);

#[events]
trait FooEvents {
    fn on_hello(&self, name: String);
    fn on_position(&self, x: f32, y: f32);
}

#[eventful(FooEvents)]
struct Foo {
    name: String,
}

impl Foo {
    fn new(name: String) -> Self {
        Self {
            name,
            events: Default::default(),
        }
    }

    fn say_hello(&self) {
        self.emit_on_hello(format!("Hello, world! {}", self.name));
        self.emit_on_position(12.0, 34.0);
    }
}

mod bar {

    use super::*;
    shard_std!(BAR_SHARD);

    #[eventful]
    pub struct Bar;

    impl Bar {
        pub fn new() -> Self {
            Bar {
                events: Default::default(),
            }
        }
    }

    impl FooEvents for Bar {
        fn on_hello(&self, name: String) {
            println!("Bar received on {:?}: {name}", thread::current().name());
        }

        fn on_position(&self, x: f32, y: f32) {
            println!("Bar moved to ({x}, {y}) on {:?}", thread::current().name());
        }
    }
}

fn main() {
    let source = FOO_SHARD.bind(|sharded| sharded(Foo::new("Sera".to_owned())).as_handle());
    let bar = BAR_SHARD.bind(|sharded| sharded(bar::Bar::new()).as_handle());

    source.on_hello().connect(&bar);
    source.on_position().connect(&bar);

    source.upgrade_in_shard(|source| {
        for _ in 0..3 {
            source.say_hello();
        }
    });

    FOO_SHARD.join().unwrap();
    bar::BAR_SHARD.join().unwrap();
}
