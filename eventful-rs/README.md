# eventful-rs

Typed events and asynchronous method calls for objects that stay on one thread.

An object can use `Rc`, `Cell`, or `RefCell` internally. Other threads communicate
through a `ShardRcHandle<T>`; they never receive a reference to the object itself.
A shard owns the queue, object store, and futures for its objects.

This is an early `0.1` API. It supports a standard thread executor, Tokio runtimes,
and an optional Slint UI adapter. It is not a distributed actor system.

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

```rust
use eventful_rs::*;
use std::cell::Cell;

shard_std!(COUNTERS);

#[eventful]
struct Counter {
    value: Cell<u32>,
}

#[asynchronize]
impl Counter {
    #[asynced]
    fn add(&self, amount: u32) -> u32 {
        let value = self.value.get() + amount;
        self.value.set(value);
        value
    }
}

fn main() {
    let counter = COUNTERS.bind(|bind| {
        bind(Counter { value: Cell::new(0), events: Default::default() }).as_handle()
    });
    assert_eq!(futures::executor::block_on(counter.add(3)), 3);
    COUNTERS.join().unwrap();
}
```

`#[eventful]` adds an `events` field and implements the object traits. The shard
macro supplies the default destination for the module. Construct values inside
`bind` to keep even non-`Send` objects on their owner thread. The returned value
must be `Send`; return `.as_handle()`, not a `ShardRc<T>`.

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

## Actions and return values

Put `#[asynchronize]` on a non-generic inherent `impl`:

| Attribute    | Original method                    | Method on a handle                      |
| ------------ | ---------------------------------- | --------------------------------------- |
| `#[action]`  | Synchronous or async, returns `()` | Queues work and returns immediately     |
| `#[asynced]` | Synchronous or async, returns `R`  | Returns an async operation yielding `R` |

Generated methods require `&self`. Arguments captured for dispatch and returned
values must be `Send + 'static`. Async actions must be awaited inside the shard;
they are not completed when the caller's action method returns. `#[asynced]`
methods submit when their generated future is polled, as ordinary async methods do.

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
cargo run -p basics-example
cargo run -p sharded-main
cargo run -p example_tokio
cargo test --workspace
cargo test -p eventful-rs --no-default-features
cargo check -p eventful-rs --all-features
cargo test -p eventful-rs --all-features --test slint
```

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
