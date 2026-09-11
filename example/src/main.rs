use std::rc::Rc;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use eventful_rs::events;
use eventful_rs::{EventLoop, EventLoopHandle, eventful, shard_std};

shard_std!(FOO_SHARD);

mod expanded;

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
    use std::rc::Rc;

    use super::*;
    shard_std!(BAR_SHARD);
    #[eventful]
    pub struct Bar;

    impl Bar {
        pub fn new() -> Rc<Self> {
            Rc::new(Bar {
                events: Default::default(),
            })
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
    let foo = Rc::new(Foo::new("Sera".to_owned()));
    let bar = bar::Bar::new();

    foo.on_hello().connect(&bar);
    foo.on_position().connect(&bar);

    let emitter = foo.clone();
    FOO_SHARD.handle().invoke(move || {
        for _ in 0..3 {
            emitter.get().say_hello();
            thread::sleep(Duration::from_millis(100));
        }
    });

    bar::BAR_SHARD.join();
}
