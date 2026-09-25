use std::thread;

use eventful_rs::events;
use eventful_rs::{EventLoop, ShardHandle, declare_shard, eventful};

use crate::bar::BarShard;

declare_shard!(pub FooShard, runtime = std);

#[events]
trait FooEvents {
    fn on_hello(&self, name: String);
    fn on_position(&self, x: f32, y: f32);
}

#[eventful(FooEvents, shard = FooShard)]
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
    declare_shard!(pub BarShard, runtime = std);

    #[eventful(shard = BarShard)]
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
    let source = FooShard::shard().bind(|sharded| sharded(Foo::new("Sera".to_owned())).as_handle());
    let bar = BarShard::shard().bind(|sharded| sharded(bar::Bar::new()).as_handle());

    // Subscribe the listener to every event in FooEvents.
    source.connect(&bar);

    source.upgrade_in_shard(|source| {
        for _ in 0..3 {
            source.say_hello();
        }
    });

    FooShard::shard().join().unwrap();
    bar::BarShard::shard().join().unwrap();
}
