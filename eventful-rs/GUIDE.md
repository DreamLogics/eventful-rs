# eventful-rs

Typed events and asynchronous method calls for values that live on one thread.
A **shard** owns an event loop and its **shard-local values**: instances of
[`Eventful`] types bound to that shard. They may use `Rc`, `Cell`, and `RefCell`;
other threads communicate through [`ShardRcHandle<T>`].

## Quick start

A background inventory sends stock updates to a main-thread display. Tracked
emission makes receipt part of the operation's result.

```rust
use eventful_rs::*;
use std::cell::Cell;

declare_shard!(pub Main, runtime = main);
declare_shard!(pub InventoryShard, runtime = std);

#[events]
trait StockEvents {
    fn stock_changed(&self, remaining: usize);
}

#[eventful(StockEvents, shard = InventoryShard)]
struct Inventory { remaining: Cell<usize> }

#[asynchronize]
impl Inventory {
    #[asynced]
    async fn reserve(&self, quantity: usize) -> Result<bool, DeliveryError> {
        let Some(remaining) = self.remaining.get().checked_sub(quantity) else {
            return Ok(false);
        };
        self.remaining.set(remaining);
        self.emit_stock_changed_tracked(remaining).await?;
        Ok(true)
    }
}

#[eventful(shard = Main)]
struct Display;
impl StockEvents for Display {
    fn stock_changed(&self, remaining: usize) {
        println!("{remaining} items available");
    }
}

#[sharded_main(Main)]
async fn main() -> Result<(), DeliveryError> {
    let inventory = Inventory::spawn(|| Inventory {
        remaining: Cell::new(10), events: Default::default(),
    }).await?;
    let display = Display::spawn(|| Display { events: Default::default() }).await?;
    let _subscription = inventory.stock_changed().connect(&display).scoped();
    assert!(inventory.reserve(3).await?);
    Ok(())
}
```

## Selecting a shard

[`declare_shard!`] creates a named, lazily initialized singleton. Choose affinity
per type with `#[eventful(Events, shard = Worker)]`, per inline module with
`#[scope(shard = Worker)]`, or per source file with `file_scope!(shard = Worker);`.
An explicit type selection overrides a scope; a scope overrides the file default.
Scope paths resolve inside the annotated module. Nested inline modules inherit
scopes; external files must select their own shard. Normal Rust imports apply to
file defaults, so a glob import can also import the generated alias.

[`Eventful::spawn`] constructs on the selected shard. Only the factory's captures
must be `Send`, not the constructed value. It submits immediately; its returned
future observes completion. The destination event loop must be running.
[`ShardBinding::bind_async`] can create multiple values of the same affinity in
one factory. Use [`DynamicShard`] and a backend's `bind_async` to select a shard
at runtime instead of a singleton; type-inferred `spawn` is unavailable for it.

Synchronous [`EventLoop::bind`] runs directly on the owner thread and blocks when
called from another thread. Cross-thread blocking is rejected inside Tokio.
`ShardRc::try_bind(value)` binds locally and returns `InvokeError::WrongShard`
outside the selected shard's context. Use `local.to_handle()` (with [`Sharded`]
in scope) to clone an owned remote handle. Prefer factory construction when
working across threads.

## Calling methods

Apply [`macro@asynchronize`] to an inherent implementation, then annotate methods.
The generated extension trait is private by default; use `#[asynchronize(pub)]`
or `#[asynchronize(pub(crate))]` when callers in other modules need it in scope:

| API                                                | Submission                               | Completion                                            |
| -------------------------------------------------- | ---------------------------------------- | ----------------------------------------------------- |
| `#[asynced]`                                       | When the handle's async method is polled | Returns the method result; panics on dispatch failure |
| `#[action]`                                        | Immediately, even from synchronous code  | Returns immediately; method must return `()`          |
| [`ShardHandle::upgrade_in_shard`]                  | Immediately                              | Runs a closure with `&T`; no result                   |
| [`ShardHandle::deferred_upgrade_in_shard`]         | When polled                              | Awaits a closure's result; panics on failure          |
| [`ShardWeakHandle::try_deferred_upgrade_in_shard`] | Immediately                              | Returns `Result<R, InvokeError>`                      |

Original methods can be synchronous or async. Arguments, captured state, and
returned values crossing threads must be `Send + 'static`. The callback's future
and its `&T` stay on the shard. Ordinary methods remain accessible through local
references. A strong handle can use `.downgrade()` to access fallible dispatch.

[`ShardRcHandle::join`] groups two to eight strong handles from the same shard,
returning `None` for mismatched shards. Chained joins form a flat tuple; use a
single callback to access the values together. This does not serialize an async
callback across suspension points.

## Events and connections

[`macro@events`] defines a synchronous listener trait. Attach it to a producer
with [`macro@eventful`] and implement the trait on listeners. Arguments must be
`Clone + Send + 'static`. Event interfaces and eventful structs cannot be generic;
concrete arguments such as `Vec<String>` are supported.

- `source.changed().connect(&listener)` subscribes to one signal.
- `handle.connect(&listener)` subscribes to the whole interface; for a local
  reference, use `ShardRc::connect(&source, &listener)`.
- `emit_changed(args)` dispatches without waiting; `emit_changed_tracked(args)`
  submits immediately and returns a future observing all selected handlers.

Awaiting a method does **not** wait for its untracked emissions. Tracked delivery
waits for every outcome and returns the first error in connection order. It does
not track work independently spawned by a handler. Dropping a tracking future
does not cancel accepted shard deliveries. [`DeliveryError`] distinguishes closed
shards, missing values, wrong shards, unwinding panics, and cancellation.

Connections hold listeners weakly: keep a strong listener handle alive. Dropping
[`Connection`] or [`ConnectionGroup`] leaves subscriptions active. Use
`disconnect()` or `.scoped()` for cleanup; dropping any scoped clone disconnects
the subscription. An emission uses a snapshot, so already selected deliveries
may still run after disconnection. Bulk registration/removal is per signal,
not atomic across the interface.

[`ShardRc::connect`] additionally ties the subscriptions to the source value's
lifetime. Remote [`ShardRcHandle::connect`] uses explicitly managed subscriptions.
A weak source's `connect` returns `None` if its event storage has expired.

### Labelled routing

Add `#[with_label(LabelType)]` to an event method. Implement [`EventLabel`] with
`subscription.matches(&emitted)`; a subscription can represent a topic, mask,
range, or other application rule. The label is routing metadata, not a handler
argument, and need not implement `Clone` or `Eq`.

```rust
use eventful_rs::*;
struct Topic(&'static str);
impl EventLabel for Topic {
    fn matches(&self, emitted: &Self) -> bool { self.0 == emitted.0 }
}
#[events]
trait Updates {
    #[with_label(Topic)]
    fn changed(&self, message: String);
}
```

Use `signal.connect_labelled(&listener, Topic("orders"))` and
`emit_changed(Topic("orders"), message)`. Ordinary and bulk connections are
wildcards. Matching scans subscriptions on the emitting thread before cloning
payloads or scheduling deliveries. It must be fast and safe for concurrent calls.
Tracked emission reports matching panics and continues other deliveries;
untracked emission propagates them.

## Backends and features

| Runtime in `declare_shard!` | Backend                                                   | Feature           |
| --------------------------- | --------------------------------------------------------- | ----------------- |
| `std`                       | [`shard::Shard`], dedicated thread                        | None              |
| `main`                      | [`local::LocalShard`], calling thread                     | None              |
| `tokio`                     | `tokio::TokioShard`, dedicated thread with timers and I/O | `tokio` (default) |
| `tokio_main`                | `tokio_local::TokioLocalShard`, calling thread with Tokio | `tokio`           |
| `slint`                     | `slint::SlintShard`, application UI loop                  | `slint`           |

Disable default features for executor-independent futures only. Core and Tokio
support Rust 1.85; Slint 1.18 requires Rust 1.92 or newer. An application must
select its own Slint backend and renderer.

Initialize main/UI markers on their owner thread before cross-thread access.
Calling-thread shards run once via `run_main` or `run_event_loop`.
[`macro@sharded_main`] drives a declared `main` or `tokio_main` shard and joins
background shards after the main future returns. It preserves the return type.
Standard shards do not supply a Tokio reactor.

## Ordering, lifetimes, and shutdown

Accepted operations start in queue order. Synchronous callbacks finish before
the next starts; async callbacks may interleave after yielding. Do not hold a
`RefCell` borrow across an await if other jobs may borrow it. Queues are unbounded:
applications should limit outstanding work.

Strong handles retain values while the shard runs. Garbage collection periodically
retires unused values on their owner thread. Shutdown destroys the store even if
handles remain. Local references stay on their owner thread; use handles for cross-thread access.

`request_shutdown()` rejects new work immediately and queues shutdown behind
accepted jobs. Outstanding futures then get a five-second grace period
(customizable with background `try_new`). Unfinished futures are dropped on the
owner thread. Blocking code and non-yielding polls cannot be interrupted.

Join background shards using [`EventLoop::join`] from synchronous code, or their
`join_async` from an external Tokio runtime. Neither can join its own shard.
[`join_all_shards`] joins registered backgrounds; with Tokio,
`join_all_shards_async` avoids blocking workers. Finish cross-shard workflows
before joining: joining is not a protocol for detecting global inactivity.
Named singleton shards cannot restart and are not dropped at process exit.

For calling-thread and UI shards, `join()` is a no-op. Drive shutdown before
stopping the host loop. In Slint, await `shutdown_async()` before quitting Slint.
Child tasks spawned directly through Tokio or Slint are outside shard accounting.
Caught unwinding panics do not roll back mutations; aborting panics are not caught.

## Examples

The repository's [example index](https://github.com/DreamLogics/eventful-rs/tree/main/examples)
maps runnable scenarios to features, including HTTP I/O, routed notifications,
subscription lifetimes, joined handles, and synchronous integration.
