use std::rc::Rc;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use eventful_rs::{EventLoop, EventLoopHandle, ShardHandle, eventful, shard_std};
use eventful_rs::{ShardRc, events};

use crate::bar::BAR_SHARD;

shard_std!(FOO_SHARD);

// mod expanded;

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
        self.emit_on_hello(format!("Hello, world! {}", &self.name));
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
    let foo = FOO_SHARD.bind(|sharded| sharded(Foo::new("Sera".to_owned())).as_handle());
    let bar = BAR_SHARD.bind(|sharded| sharded(bar::Bar::new()).as_handle());

    foo.on_hello().connect(&bar);
    foo.on_position().connect(&bar);

    foo.upgrade_in_shard(|foo| {
        for _ in 0..3 {
            foo.say_hello();
            thread::sleep(Duration::from_millis(100));
        }
    });

    FOO_SHARD.join();
    bar::BAR_SHARD.join();
}
