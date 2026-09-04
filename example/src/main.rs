use eventful_rs::{EventLoop, EventLoopHandle, Shard, accept_events, events, with_events};
use std::sync::{Arc, LazyLock};
use std::thread;
use std::time::Duration;

static BAR_SHARD: LazyLock<Shard> = LazyLock::new(|| Shard::new("receiver-a"));

#[events]
trait FooEvents {
    fn on_hello(&self, name: String);
    fn on_position(&self, x: f32, y: f32);
}

#[with_events(FooEvents)]
struct Foo {
    name: String,
}

impl Foo {
    fn new(name: String) -> Self {
        Self {
            name,
            foo_events: Default::default(),
        }
    }

    fn say_hello(&self) {
        self.emit_on_hello(format!("Hello, world! {}", &self.name));
        self.emit_on_position(12.0, 34.0);
    }
}

#[accept_events(BAR_SHARD)]
struct Bar;

impl FooEvents for Bar {
    fn on_hello(&self, name: String) {
        println!("Bar received on {:?}: {name}", thread::current().name());
    }

    fn on_position(&self, x: f32, y: f32) {
        println!("Bar moved to ({x}, {y}) on {:?}", thread::current().name());
    }
}

fn main() {
    let foo = Arc::new(Foo::new("Sera".to_owned()));
    let bar = BAR_SHARD.bind(Bar);

    foo.on_hello().connect(&bar);
    foo.on_position().connect(&bar);

    let emitter = Arc::clone(&foo);
    thread::spawn(move || {
        for _ in 0..3 {
            emitter.say_hello();
            thread::sleep(Duration::from_millis(100));
        }
    })
    .join()
    .unwrap();

    // Give the independent event-loop thread time to drain for this short demo.
    thread::sleep(Duration::from_millis(100));
}
