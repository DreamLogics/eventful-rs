# eventful-rs

Is your Rust code too boring? Want to spice up your async cravings? Then let's make
things a little more eventful! Add some events to your objects, connect that spaghetti
and let the magic happen. (or make it explode, whatever works for you)

This crate provides an event and async dispatch system for Rust, focused mostly on
ergonomics rather than performance. (no, that doesn't mean performance is going to be shit)
It is intended for applications that need a simple, safe, and flexible way to handle
events and asynchronous method calls on objects that might not live on the same thread.

In concept, you have your stuff living on these little islands called shards. Each shard has it's
own thread and event loop. You can create objects on a shard, and then call methods on those objects
from other shards. The calls are queued and executed on the shard's thread, and you can await the
result of the call if you want. You can also connect events from one object to another, and when
an event is emitted, all connected objects will receive the event on their own shard's thread.

An object can use `Rc`, `Cell`, or `RefCell` internally. Other threads communicate
through a `ShardRcHandle<T>`; they never receive a reference to the object itself.
A shard owns the queue, object store, and futures for its objects.

This is an early `0.1` API. It supports a standard thread executor, Tokio runtimes,
and an optional Slint UI adapter. It is not a distributed actor system (yet?).

## Install

```toml
[dependencies]
eventful-rs = "0.1"
futures = "0.3" # Only needed if your application uses its executor directly.
```

The default feature enables Tokio. For the standard executor only:

```toml
[dependencies]
eventful-rs = { version = "0.1", default-features = false }
```

Rust 1.85 or newer is required for the core API and async closures. Optional
backends and application dependencies can require a newer compiler; the Slint
adapter is tested on current stable Rust.

## Quick start

The [annotated sharded-main example](examples/sharded-main/src/main.rs) shows a
producer on a background shard sending events to a reporter on the main-thread
shard. Run it with `cargo run -p sharded-main`.

`#[sharded_main]` drives the main shard while `main` awaits work, allowing the
reporter to process events while the producer runs on its own thread. It also
joins the background shards when the main shard finishes.

```rust
use eventful_rs::*;

mod producer {
    use std::cell::Cell;

    use eventful_rs::*;

    shard_std!(PRODUCER);

    #[events]
    pub trait ProducerEvents {
        fn on_produce(&self, item: String);
    }

    #[eventful(ProducerEvents)]
    pub struct Producer {
        count: Cell<usize>,
    }

    #[asynchronize]
    impl Producer {
        pub fn new() -> ShardRcHandle<Self> {
            Self {
                events: Default::default(),
                count: Cell::new(0),
            }
            .into()
        }

        // This method produces items and emits events for each produced item.
        // The `#[asynced]` attribute allows this method to be called asynchronously,
        // even though it is not an async function itself. All that is made async is the
        // wrapper that calls this method, so it can be called from an async context without blocking.
        // This version doesn't track the event emissions, meaning there is no guarantee that the events
        // have been processed by the listeners before this method returns.

        #[asynced]
        pub fn produce(&self, count: usize) -> Vec<String> {
            for i in 0..count {
                let item = format!("Item {}", i);
                self.emit_on_produce(item.clone());
            }
            self.count.update(|c| c + count);
            (0..count).map(|i| format!("Item {}", i)).collect()
        }

        // This is the same as the previous method, but it tracks the event emissions.
        // It uses `futures::future::join_all` to wait for all event emissions to complete
        // before returning. This ensures that all events have been processed by the listeners
        // before this method returns.

        #[asynced]
        pub async fn produce_tracked(&self, count: usize) -> Vec<String> {
            let tracking = (0..count).map(|i| {
                let item = format!("Item {}", i);
                self.emit_on_produce_tracked(item.clone())
            });
            futures::future::join_all(tracking).await;

            self.count.update(|c| c + count);
            (0..count).map(|i| format!("Item {}", i)).collect()
        }

        // An alternative to asynced wrappers is to use the `#[action]` attribute,
        // it functions more or less the same as `#[asynced]`, but it doesn't return anything,
        // thus making it a bit more lightweight. Also, the wrapper is just a normal function,
        // so it can be called from any context, not just async contexts.
        // This is useful for methods that don't need to return a value,
        // but still need to be called asynchronously.

        #[action]
        pub fn reset_count(&self) {
            self.count.set(0);
        }

        // Pretty much anything can be made asynced (or an action), as long as the arguments and
        // return values are Send + 'static.

        #[asynced]
        pub fn get_count(&self) -> usize {
            self.count.get()
        }

        // Normal methods won't be available from outside the shard, but can be called like normal
        // from within the same shard.

        pub fn internal_report(&self) -> String {
            format!("Produced {} items", self.count.get())
        }
    }
}

#[eventful]
struct ProductionReporter {}

impl ProductionReporter {
    pub fn new() -> ShardRcHandle<Self> {
        Self {
            events: Default::default(),
        }
        .into()
    }
}

impl producer::ProducerEvents for ProductionReporter {
    fn on_produce(&self, item: String) {
        println!("Produced: {}", item);
    }
}

#[sharded_main]
async fn main() {
    use producer::*;

    // create our objects
    let producer = Producer::new();
    let reporter = ProductionReporter::new();

    // connect the reporter to the producer's events
    producer.on_produce().connect(&reporter);

    // produce some items
    // you will notice that this call may return before the reporter has
    // finished processing all events, because we are not tracking if all
    // callbacks have been called before returning
    let produced = producer.produce(12).await;
    println!("Produced items untracked: {:?}", produced);

    // produce some items, but this time we will track the event emissions
    // this means that this call will not return until all callbacks have been called
    let produced_tracked = producer.produce_tracked(12).await;
    println!("Produced items tracked: {:?}", produced_tracked);

    // we can also turn our handle back into a reference to the object
    // the closure will run on the shard/thread this handle's object lives in
    producer.upgrade_in_shard(|producer| {
        // this will run on the producer shard/thread
        println!("Report: {}", producer.internal_report());
    });

    // using an asynced method wrapper is often more convenient than
    // upgrading a handle, but does require an async context
    let count = producer.get_count().await;
    println!("Count from async call: {}", count);

    // we can also join two handles, but they must be on the same shard
    // this allows us to upgrade both handles at the same time,
    // and run a closure on the shard/thread they live in
    let another_producer = Producer::new();
    let joined = producer.join(&another_producer).expect("same shard");

    joined.upgrade_in_shard(|(producer1, producer2)| {
        // this will run on the producer shard/thread
        println!(
            "Report from joined handles: {}",
            producer1.internal_report()
        );
        println!(
            "Report from joined handles: {}",
            producer2.internal_report()
        );
    });

    // there might be instances where no async context is available,
    // luckily asynced action methods do not require one
    fn sync_context(producer: ShardRcHandle<Producer>) {
        // we can also call an action method from a sync context
        producer.reset_count();
    }
    sync_context(producer.clone());
}
```

### What to use when

- **Choose where objects live.** `shard_std!(PRODUCER)` supplies the producer
  module's default shard. `#[sharded_main]` supplies the main-thread default used
  by `ProductionReporter`. Their constructors return `ShardRcHandle<Self>` via
  `.into()`, binding each object to its default shard. `#[eventful]` adds the
  `events` field and object traits.
- **Connect objects through events.** `#[events]` defines `ProducerEvents`;
  `#[eventful(ProducerEvents)]` lets `Producer` emit them. Implementing that trait
  on `ProductionReporter` supplies the callbacks. `connect(&reporter)` arranges
  for those callbacks to run on the reporter's shard.
- **Await a method result.** `#[asynchronize]` generates handle methods for the
  annotated `impl`. `#[asynced]` makes the handle wrapper async, even for an
  ordinary method such as `produce` or `get_count`. The original method runs on
  the object's shard, and awaiting the wrapper returns its result.
- **Wait for event listeners too.** Awaiting `produce` only waits for the producer
  method; its untracked events may still be pending. `produce_tracked` awaits
  `join_all` over the tracked emissions, so every delivery has completed or
  reported a failure before it returns. The example discards those results;
  inspect the returned `Result<(), DeliveryError>` values when failures matter.
- **Queue work from synchronous code.** `#[action]` generates a normal handle
  method that queues work and returns immediately. `reset_count()` needs no
  `.await` or async caller; returning from it does not mean the reset has run.
- **Call ordinary methods on the object.** `internal_report` has no generated
  handle wrapper. `upgrade_in_shard` queues a closure that receives `&Producer`
  on the producer's shard, where it can call that method directly. The reference
  stays inside the callback; it is not returned to the calling thread.
- **Work with several objects together.** `producer.join(&another_producer)`
  checks that both handles share a shard. The resulting group upgrades them in
  one callback, as a tuple of references, while keeping the original handles
  usable.

The dispatch arguments and results must be `Send + 'static`; the objects
can use `Cell`, `RefCell`, and `Rc` internally. The example's `Cell<usize>` is
`Send`, so its constructor can use `.into()`. For non-`Send` values such as `Rc`,
construct inside a `bind` factory and return `.as_handle()`. See
[Async callers and construction](#async-callers-and-construction) for the blocking
behavior of `.into()` and the `bind_async` alternative for Tokio callers.

## Joining handles

Use `foo.join(&bar)` to group strong handles on the same shard. It returns
`Some(JoinedHandles<...>)` when their shard IDs match, otherwise `None`.

```rust
if let Some(joined) = foo.join(&bar) {
    joined.upgrade_in_shard(|(foo, bar)| {
        foo.something();
        bar.something();
    });
}
```

Joins borrow their inputs and clone the handles only when their shard IDs match.
The returned group owns its handles. Chain `.join(&baz)` to extend the flat tuple
to as many as eight handles. Objects can have different types, and the same handle
can appear more than once.

Joined handles also provide `upgrade_in_shard_async`, `deferred_upgrade_in_shard`,
and `try_deferred_upgrade_in_shard`, with callbacks taking a tuple of references.
All objects are resolved in one job on their shared shard and kept alive for the
callback. Async callbacks can interleave with other jobs while suspended.

Joining checks shard identity, not whether the shard is running. Like single
handle upgrades, fire-and-forget callbacks are skipped after shutdown; the
`try_deferred` method reports `InvokeError::Closed`. This API supports
`ShardRcHandle<T>` using the built-in `ShardEventHandle` shared by the backends.

## Typed events

Declare an event interface with `#[events]`, attach it to a source with
`#[eventful(InterfaceName)]`, and implement the interface on a destination object.
The generated `source.event_name().connect(&target)` queues delivery on the
destination's shard. Arguments must be owned, `Clone + Send + 'static` values.
Interfaces need not be `Send` or `Sync`; their objects remain thread-affine.

```rust
use eventful_rs::*;
shard_std!(MESSAGES);

#[events]
trait Messages { fn message(&self, text: String); }

#[eventful(Messages)]
struct Sender;

#[eventful]
struct Receiver;
impl Messages for Receiver {
    fn message(&self, text: String) { println!("{text}"); }
}

fn main() {
    let source = MESSAGES.bind(|bind| bind(Sender { events: Default::default() }).as_handle());
    let target = MESSAGES.bind(|bind| bind(Receiver { events: Default::default() }).as_handle());
    source.message().connect(&target);
    source.message().emit("hello".to_owned());
    MESSAGES.join().unwrap();
}
```

Inside a source object, call `self.emit_message(text)`. Event traits in another
module expose `MessagesSignalsExt` and `MessagesEmittersExt`; import these extension
traits where their methods are used.

Connections use weak targets. They do not keep destination objects or event sets
alive. Deliveries to missing or stopped targets are ignored. Connections remain in
the source's connection list until that source event set is dropped; there is not
yet a disconnect API.

Use `source.message().emit_tracked(text).await` (or
`self.emit_message_tracked(text).await`) to wait for handler completion. Dispatch
starts when the method is called, and dropping the future does not cancel queued
shard deliveries. The future waits for every connection in the emission snapshot,
then returns `Ok(())` or the first `DeliveryError` in connection order. Tracked
delivery reports stopped shards, missing weak targets, handler panics, and shutdown
cancellation. An event with no connections succeeds. Ordinary `emit` continues to
ignore missing or stopped targets.

For manually constructed `Event` values, `add_connection` tracks completion of the
callback itself. Use `add_tracked_connection` to supply separate ordinary and
tracked dispatch callbacks when completion needs to be observed asynchronously.

## Actions and return values

Put `#[asynchronize]` on a non-generic inherent `impl`:

| Attribute    | Original method                    | Method on a handle                      |
| ------------ | ---------------------------------- | --------------------------------------- |
| `#[action]`  | Synchronous or async, returns `()` | Queues work and returns immediately     |
| `#[asynced]` | Synchronous or async, returns `R`  | Returns an async operation yielding `R` |

Generated methods require `&self`. Arguments captured for dispatch and returned
values must be `Send + 'static`. The original method keeps its synchronous or
async signature; these attributes control the wrapper on the handle. For an
async action, the generated wrapper arranges for the shard to await the method.
The caller does not await the action, and its return does not signal completion.
`#[asynced]` methods submit when their generated future is polled, as ordinary
async methods do. Awaiting them waits for the method's result, including any
tracked emissions it explicitly awaits; it does not track ordinary emissions.

Unannotated methods, such as `internal_report` in the example, remain ordinary
object methods. Use `upgrade_in_shard` to call them in a queued callback with a
local object reference. That helper also works from synchronous code and returns
without waiting. Use `deferred_upgrade_in_shard` when the callback needs to return
a result to the caller.

For explicit errors rather than a panic on cancellation, use
`handle.try_deferred_invoke(object_handle, async |object| { /* result */ })`.
It submits immediately and returns a future yielding `Result<R, InvokeError>`.
Errors distinguish closed shards, wrong-shard handles, missing objects, callback
panics, and cancellation. Dropping the receiver does not cancel accepted work.

The legacy object-handle helpers and generated `#[asynced]` methods still return
`R`, so they panic if no result can be delivered. `try_invoke*` and `try_spawn`
report admission errors. Targeted fire-and-forget helpers ignore unavailable
targets; use the fallible methods when that distinction matters.

## Choosing a backend

| Declaration               | Execution thread          | Driver                       |
| ------------------------- | ------------------------- | ---------------------------- |
| `shard_std!(NAME)`        | Dedicated OS thread       | Executor-independent futures |
| `shard_tokio!(NAME)`      | Dedicated OS thread       | Tokio current-thread runtime |
| `shard_main!(NAME)`       | Thread calling `run_main` | Executor-independent futures |
| `shard_tokio_main!(NAME)` | Thread calling `run_main` | Tokio current-thread runtime |
| `shard_slint!(NAME)`      | Slint UI thread           | Slint's local executor       |

All backends use the same thread-safe handle implementation and common queue.
Backend handle names remain available as aliases. A runtime shard ID check prevents
using an object's ID against a different shard's store.

`#[sharded_main] async fn main()` declares and drives a standard main-thread shard,
then joins background shards. `#[sharded_main(tokio)]` selects the Tokio variant.
The original main return type is preserved. Calling-thread shards must be driven
on their constructing thread and may run only once. Their main future and result
can be non-`Send`.

Tokio shards provide timers and I/O drivers. A standard shard can await
executor-independent futures such as `futures::channel::oneshot`, but cannot
provide a Tokio reactor by itself. Futures are scheduled by their wakers; the
engine does not busy-poll pending work.

For Slint, enable the `slint` feature and configure a Slint backend/renderer in your
application. The adapter does not select a GUI backend for you. Construct it on the
UI thread and keep Slint's event loop running to execute work. `join()` is a no-op
for calling-thread and UI shards; use `request_shutdown()` and let their driver
finish before stopping the host event loop. On Slint, await
`shard.shutdown_async()` before calling `slint::quit_event_loop()`.

## Async callers and construction

Synchronous cross-thread `bind()` blocks until the factory finishes. In a Tokio
context, use `shard.bind_async(factory).await?`; blocking binding is rejected there.
On the destination thread, `bind()` executes directly and supports reentrant binding.
Never synchronously wait for work that needs the thread you are blocking.

The convenience `let handle: ShardRcHandle<T> = value.into()` is available when
`T: Send` and has a default shard. It may block across threads. Use an explicit
factory with `bind_async` from Tokio, and with `bind` for non-`Send` object creation.
This avoids constructing runtime-dependent resources on the wrong thread.

## Ordering, ownership, and shutdown

- Submissions enter one queue. Each operation starts in admission order.
  Synchronous callbacks run to completion; async operations may interleave and
  complete out of order after yielding. Await results to express dependencies.
- Do not hold a `RefCell` borrow across an await if another operation can borrow
  that cell. Thread affinity prevents cross-thread access, not async reentrancy.
- Object handles keep objects alive while the shard is active. Weak handles do
  not. Garbage collection checks idle stores periodically and retires objects
  outside the store borrow so destructors can safely reenter during collection.
- The queue is unbounded and has no backpressure. Producers must bound their own
  outstanding work. This is unsuitable for unrestricted untrusted submissions.
- `request_shutdown()` closes admission immediately. `join()` waits synchronously;
  `join_async().await` waits without blocking a Tokio worker. Neither may join its
  own shard. Repeated and concurrent joins observe the same outcome.
- The driver dispatches accepted messages, then gives outstanding futures a five
  second grace period. `try_new(name, grace)` customizes this for background shards.
  Unfinished futures are canceled. This is not a hard wall-clock shutdown deadline:
  a blocking callback or a non-yielding poll cannot be interrupted.
- Child tasks launched directly with Tokio or Slint APIs are outside the shard's
  accounting. Await them or submit through the shard handle if they must drain.
  Tokio blocking-pool jobs can also delay runtime destruction.
- Caught unwinding panics preserve the loop but do not roll back object mutations.
  Abort-on-panic builds and panicking destructors cannot provide this isolation.
- Static shards are not dropped at process exit. Join them explicitly, or use
  `join_all_shards()` after producers finish. From an external Tokio runtime use
  `join_all_shards_async().await`. Joining shards one-by-one is not a distributed
  quiescence protocol: finish cross-shard workflows before shutting them down.

The crate forbids unsafe code. `ShardRc<T>` stays non-`Send` and non-`Sync`; moving
the safe handle never moves its `Rc<T>` or object store. A weak handle's `events`
field is a `Weak` pointer, not an owning `Arc`.

## Examples and development

From the workspace root:

```sh
cargo run -p no-main-shard
cargo run -p sharded-main
cargo run -p example_tokio
cargo test --workspace
cargo test -p eventful-rs --no-default-features
cargo check -p eventful-rs --all-features
cargo test -p eventful-rs --all-features --test slint
```

The primary `sharded-main` example demonstrates generated method wrappers,
tracked and untracked events, local callbacks, and joined handles. The
`no-main-shard` example uses a normal synchronous `main`, explicit `bind`
factories, and callbacks on two background shards.

The HTTP example runs a local HTTP server and needs no external website. Tests
include Rust e2e tests in each example crate, using `assert_cmd` to check the real
binary's output and exit status with a 20-second timeout. They run as part of
`cargo test --workspace`; no separate test runner is required. Tests
cover dispatch, timer progress, typed results, cancellation, panic handling,
thread-affine destruction, weak ownership, macro diagnostics, and the executable
examples. See the workspace `RELEASING.md` for package verification and release
order. Generic event traits/structs, tuple structs, and generic annotated impls are
currently rejected with explicit diagnostics.

## License

MIT. See `LICENSE`.
