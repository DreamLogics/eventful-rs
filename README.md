# eventful-rs

Is your Rust code too boring? Want to spice up your async cravings? Then let's make
things a little more eventful! Add some events to your structs, connect that spaghetti
and let the magic happen. (or make it explode, whatever works for you)

This crate provides an event and async dispatch system for Rust, focused mostly on
ergonomics rather than performance. (no, that doesn't mean performance is going to be shit)
It is intended for applications that need a simple, safe, and flexible way to handle
events and asynchronous method calls on values that might not live on the same thread.

In concept, you have your stuff living on these little islands called shards. Each shard has its
own thread and event loop. You can create values on a shard, and then call methods on those values
from other shards. The calls are queued and executed on the shard's thread, and you can await the
result of the call if you want. You can also connect events from a producer to a listener, and when
an event is emitted, all connected listeners will receive the event on their own shard's thread.

A value can use `Rc`, `Cell`, or `RefCell` internally. Other threads communicate
through a `ShardRcHandle<T>`; they never receive a reference to the value itself.
A shard owns the queue, value store, and futures for its values.

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

`#[sharded_main(Main)]` drives the main shard while `main` awaits work, allowing the
reporter to process events while the producer runs on its own thread. It also
joins the background shards when the main shard finishes.

```rust
eventful_rs::declare_shard!(pub Main, runtime = main);
use eventful_rs::*;

#[scope(shard = ProducerShard)]
mod producer {
    use std::cell::Cell;

    use eventful_rs::*;

    declare_shard!(pub ProducerShard, runtime = std);

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
        pub async fn new() -> Result<ShardRcHandle<Self>, InvokeError> {
            Self::spawn(|| Self {
                events: Default::default(),
                count: Cell::new(0),
            })
            .await
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

#[eventful(shard = Main)]
struct ProductionReporter {}

impl ProductionReporter {
    pub async fn new() -> Result<ShardRcHandle<Self>, InvokeError> {
        Self::spawn(|| Self {
            events: Default::default(),
        })
        .await
    }
}

impl producer::ProducerEvents for ProductionReporter {
    fn on_produce(&self, item: String) {
        println!("Produced: {}", item);
    }
}

#[sharded_main(Main)]
async fn main() {
    use producer::*;

    // create the producer and reporter
    let producer = Producer::new().await.unwrap();
    let reporter = ProductionReporter::new().await.unwrap();

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

    // borrow the producer through its handle
    // the closure runs on the producer's shard thread
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
    let another_producer = Producer::new().await.unwrap();
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

- **Choose where values live.** `declare_shard!(pub ProducerShard, runtime = std)`
  declares a named shard. `#[scope(shard = ProducerShard)]` selects it for the
  producer module. The reporter explicitly selects `Main`, declared with
  `runtime = main` and driven by `#[sharded_main(Main)]`. Constructors use
  `Self::spawn(factory).await` to create values on the shard identified by their type.
- **Connect producers to listeners.** `#[events]` defines `ProducerEvents`;
  `#[eventful(ProducerEvents)]` lets `Producer` emit them. Implementing that trait
  on `ProductionReporter` supplies the callbacks. `connect(&reporter)` arranges
  for those callbacks to run on the reporter's shard.
- **Await a method result.** `#[asynchronize]` generates handle methods for the
  annotated `impl`. `#[asynced]` makes the handle wrapper async, even for an
  ordinary method such as `produce` or `get_count`. The original method runs on
  the value's shard, and awaiting the wrapper returns its result.
- **Wait for event listeners too.** Awaiting `produce` only waits for the producer
  method; its untracked events may still be pending. `produce_tracked` awaits
  `join_all` over the tracked emissions, so every delivery has completed or
  reported a failure before it returns. The example discards those results;
  inspect the returned `Result<(), DeliveryError>` values when failures matter.
- **Queue work from synchronous code.** `#[action]` generates a normal handle
  method that queues work and returns immediately. `reset_count()` needs no
  `.await` or async caller; returning from it does not mean the reset has run.
- **Call ordinary methods on the producer.** `internal_report` has no generated
  handle wrapper. `upgrade_in_shard` queues a closure that receives `&Producer`
  on the producer's shard, where it can call that method directly. The reference
  stays inside the callback; it is not returned to the calling thread.
- **Work with several values together.** `producer.join(&another_producer)`
  checks that both handles share a shard. The resulting group upgrades them in
  one callback, as a tuple of references, while keeping the original handles
  usable.

Dispatch arguments, factory captures, and returned results must be `Send + 'static`.
The eventful value itself need not be `Send`: construct `Rc`, `Cell`, and `RefCell`
inside the `spawn` factory on the owning shard.

## Declaring and selecting shards

Declaration creates a unique marker type and a lazy singleton. Several declarations
can coexist in one module. Selection is explicit and independent of declaration:

```rust
use eventful_rs::*;
declare_shard!(pub Worker, runtime = std);
declare_shard!(pub Ui, runtime = main);

#[scope(shard = super::Worker)]
mod models {
    use eventful_rs::*;
    #[eventful]
    pub struct Counter;

    #[eventful(shard = super::Ui)]
    pub struct View;
}
```

`<models::Counter as Eventful>::Shard` is `Worker`; `View::Shard` is `Ui`.
Scope paths resolve inside the annotated module, so use `super::Worker` for a
marker declared in its parent. Selection extends to nested inline modules;
per-type selections and nested `#[scope]` attributes override it. External module
files must select their own shards. Types without a selection fail to compile.
Scope only processes directly written struct/module items, not declarations
emitted by another macro or hidden in `cfg_attr`.

`Worker::shard()` exposes the backend for driving and shutdown. Initialize
calling-thread and Slint markers on their owner thread before submitting work
from other threads. Named singleton shards cannot restart after shutdown.

`Worker::bind_async(...)` requires `T: Eventful<Shard = Worker>` at compile time.
Binding through a backend or an erased `ShardEventHandle` checks the designated
shard ID at runtime: `bind_async` returns `InvokeError::WrongShard`, and synchronous
`bind` panics, before running the factory. A named type cannot be rebound elsewhere.

For values deliberately placed on different runtime-created shards, select
`#[eventful(shard = DynamicShard)]` (or put it on a scope) and use each backend's
`bind` / `bind_async`. Their handles carry the actual shard identity.
`DynamicShard` has no inferred destination and does not support `T::spawn` or
`.into()` conversions. Manual `Eventful` implementations likewise specify
`type Shard = Marker` or `type Shard = DynamicShard`.

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
to as many as eight handles. Values can have different types, and the same handle
can appear more than once.

Joined handles also provide `upgrade_in_shard_async`, `deferred_upgrade_in_shard`,
and `try_deferred_upgrade_in_shard`, with callbacks taking a tuple of references.
All values are resolved in one job on their shared shard and kept alive for the
callback. Async callbacks can interleave with other jobs while suspended.

Joining checks shard identity, not whether the shard is running. Like single
handle upgrades, fire-and-forget callbacks are skipped after shutdown; the
`try_deferred` method reports `InvokeError::Closed`. This API supports
`ShardRcHandle<T>` using the built-in `ShardEventHandle` shared by the backends.

## Typed events

Declare an event interface with `#[events]`, attach it to a source with
`#[eventful(InterfaceName)]`, and implement the interface for a listener type.
The generated `source.event_name().connect(&target)` queues delivery on the
destination's shard. Arguments must be owned, `Clone + Send + 'static` values.
Interfaces need not be `Send` or `Sync`; listener values remain thread-affine.
Concrete argument types such as `Vec<String>` are supported:
`fn some_event(&self, values: Vec<String>);`. The restriction on generics applies
to declared trait or method parameters such as `fn some_event<T>(...)`, not to
concrete container types. Every event method requires an `&self` receiver.

```rust
use eventful_rs::*;
declare_shard!(pub MessagesShard, runtime = std);

#[events]
trait Messages { fn message(&self, text: String); }

#[eventful(Messages, shard = MessagesShard)]
struct Sender;

#[eventful(shard = MessagesShard)]
struct Receiver;
impl Messages for Receiver {
    fn message(&self, text: String) { println!("{text}"); }
}

fn main() {
    let source = MessagesShard::shard().bind(|bind| bind(Sender { events: Default::default() }).as_handle());
    let target = MessagesShard::shard().bind(|bind| bind(Receiver { events: Default::default() }).as_handle());
    source.message().connect(&target);
    source.message().emit("hello".to_owned());
    MessagesShard::shard().join().unwrap();
}
```

Inside a method on the source type, call `self.emit_message(text)`. Event traits in another
module expose `MessagesSignalsExt` and `MessagesEmittersExt`; import these extension
traits where their methods are used.

Connections use weak targets. They do not keep listeners or event sets
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
methods on shard-local values. Use `upgrade_in_shard` to call them in a queued
callback with a reference to the value. That helper also works from synchronous
code and returns without waiting. Use `deferred_upgrade_in_shard` when the callback needs to return
a result to the caller.

For explicit errors rather than a panic on cancellation, use
`handle.try_deferred_invoke(value_handle, async |value| { /* result */ })`.
It submits immediately and returns a future yielding `Result<R, InvokeError>`.
Errors distinguish closed shards, wrong-shard handles, missing values, callback
panics, and cancellation. Dropping the receiver does not cancel accepted work.

The legacy handle helpers and generated `#[asynced]` methods still return
`R`, so they panic if no result can be delivered. `try_invoke*` and `try_spawn`
report admission errors. Targeted fire-and-forget helpers ignore unavailable
targets; use the fallible methods when that distinction matters.

## Choosing a backend

| Declaration               | Execution thread          | Driver                       |
| ------------------------- | ------------------------- | ---------------------------- |
| `declare_shard!(Name, runtime = std)`        | Dedicated OS thread       | Executor-independent futures |
| `declare_shard!(Name, runtime = tokio)`      | Dedicated OS thread       | Tokio current-thread runtime |
| `declare_shard!(Name, runtime = main)`       | Thread calling `run_main` | Executor-independent futures |
| `declare_shard!(Name, runtime = tokio_main)` | Thread calling `run_main` | Tokio current-thread runtime |
| `declare_shard!(Name, runtime = slint)`      | Slint UI thread           | Slint's local executor       |

All backends use the same thread-safe handle implementation and common queue.
Backend handle names remain available as aliases. A runtime shard ID check prevents
using a value's ID against a different shard's store.

`#[sharded_main(Main)] async fn main()` drives an explicitly declared main-thread
marker, then joins background shards. Declare `Main` with `runtime = main` or
`runtime = tokio_main` to choose its executor. It does not implicitly select a
shard for surrounding types.
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
`T: Send` and `T::Shard: ShardBinding`. It may block across threads. Prefer
`T::spawn(factory).await?` for inferred construction, including from Tokio and for
non-`Send` values. `spawn` submits immediately; dropping its future does not cancel
accepted work. Factory panics become `InvokeError::Panicked`, and submissions to
stopped shards return `InvokeError::Closed`. The destination loop must be running.

`let local: ShardRc<T> = value.into()` remains available inside the designated
shard's execution context and does not require `T: Send`. It panics outside that
context. `ShardRc<T>` cannot be returned across threads; return `.as_handle()`
from a binding factory instead.

## Ordering, ownership, and shutdown

- Submissions enter one queue. Each operation starts in admission order.
  Synchronous callbacks run to completion; async operations may interleave and
  complete out of order after yielding. Await results to express dependencies.
- Do not hold a `RefCell` borrow across an await if another operation can borrow
  that cell. Thread affinity prevents cross-thread access, not async reentrancy.
- Strong handles keep values alive while the shard is active. Weak handles do
  not. Garbage collection checks idle stores periodically and retires values
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
- Caught unwinding panics preserve the loop but do not roll back value mutations.
  Abort-on-panic builds and panicking destructors cannot provide this isolation.
- Static shards are not dropped at process exit. Join them explicitly, or use
  `join_all_shards()` after producers finish. From an external Tokio runtime use
  `join_all_shards_async().await`. Joining shards one-by-one is not a distributed
  quiescence protocol: finish cross-shard workflows before shutting them down.

The crate forbids unsafe code. `ShardRc<T>` stays non-`Send` and non-`Sync`; moving
the safe handle never moves its `Rc<T>` or value store. A weak handle's `events`
field is a `Weak` pointer, not an owning `Arc`.

## Examples and development

From the workspace root:

```sh
cargo run -p no-main-shard-example
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
