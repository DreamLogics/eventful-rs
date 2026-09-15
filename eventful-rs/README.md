# eventful-rs

Thread-affine objects, typed events, and asynchronous method dispatch for Rust.
Objects live on shards that own their queues, stores, and futures. Objects can
use `Rc`, `Cell`, and `RefCell` internally; thread-safe `ShardRcHandle<T>` values
let other threads queue work on the owning shard.

Start with the [annotated producer/reporter example and guide](https://github.com/DreamLogics/eventful-rs#quick-start).
The producer lives on a background shard and emits events to a reporter on the
main-thread shard. It demonstrates when to use each API:

- `shard_std!(PRODUCER)` declares a background shard and the module's default
  destination. `#[sharded_main]` drives the main-thread shard while the async
  main function awaits work, then joins background shards when it finishes.
- `#[eventful]` adds an `events` field and object traits. Constructors can return
  `ShardRcHandle<Self>` using `.into()` for `Send` objects with a default shard.
  That conversion may block across threads; use `bind_async` from Tokio and
  construct non-`Send` objects inside `bind` or `bind_async` factories.
- `#[events]` defines a listener interface. Attach it to a source with
  `#[eventful(ProducerEvents)]`, implement it on a listener, and use
  `producer.on_produce().connect(&reporter)` to dispatch on the listener's shard.
- `#[asynchronize]` generates handle wrappers. `#[asynced]` makes a wrapper async
  even when the original method is synchronous. Awaiting it returns the method's
  result; untracked events emitted by that method may still be pending.
- Tracked emissions return futures that observe handler completion and errors.
  The example's `produce_tracked` awaits them with `join_all`. It waits for every
  delivery outcome but discards the results; inspect them to handle failures.
- `#[action]` generates a normal handle method that queues work and returns
  immediately. Calling `reset_count()` needs no async context or `.await`.
- `upgrade_in_shard` queues a closure with a local object reference so it can
  call ordinary methods such as `internal_report`. The reference stays on the
  owning shard. Use `deferred_upgrade_in_shard` to await a callback result.
- `foo.join(&bar)` borrows strong handles and returns an owned `JoinedHandles`
  group if their shard IDs match, otherwise `None`. Chain `.join(&baz)` for flat
  tuples of up to eight handles, then upgrade them together in one callback.

Dispatch arguments and results must be `Send + 'static`. Async callbacks can
interleave with other work while suspended. Connections use weak targets;
keep listener handles alive while their objects are needed.

The default feature enables Tokio. Standard thread, calling-thread, and optional
Slint backends share the same handle implementation. See the guide for backend
selection, construction, delivery errors, and shutdown behavior.
