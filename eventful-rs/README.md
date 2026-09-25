# eventful-rs

Thread-affine values, typed events, and asynchronous method dispatch for Rust.
Values live on shards that own their queues, stores, and futures. Values can
use `Rc`, `Cell`, and `RefCell` internally; thread-safe `ShardRcHandle<T>` values
let other threads queue work on the owning shard.

Start with the [annotated producer/reporter example and guide](https://github.com/DreamLogics/eventful-rs#quick-start).
The producer lives on a background shard and emits events to a reporter on the
main-thread shard. It demonstrates when to use each API:

- `declare_shard!(pub Worker, runtime = std)` declares a marker and a lazy singleton.
  `#[scope(shard = super::Worker)]` selects it for an inline module; paths resolve
  inside that module. `#[eventful(shard = Worker)]` selects a shard for one type.
  Explicit type selections and nested scopes override inherited selections.
- `Eventful::Shard` records the destination in the type. `T::spawn(factory).await?`
  constructs a value there and returns `ShardRcHandle<T>`. The factory's captures
  must be `Send`; the value may contain `Rc` or other non-`Send` state.
- `#[sharded_main(Main)]` drives a marker declared with `runtime = main` or
  `runtime = tokio_main`, then joins background shards. It supplies no implicit
  default. Use `Marker::shard()` for backend lifecycle operations.
- `Marker::bind_async` enforces matching affinity at compile time. Backend/handle
  binding checks it at runtime. `DynamicShard` explicitly permits runtime selection
  through `bind` / `bind_async`, without type-inferred `spawn` or `.into()`.
- `ShardRc<T>::from(value)` binds locally without requiring `Send`; it panics
  outside the designated shard. `ShardRcHandle<T>::from(value)` requires `Send`
  and can block across threads. Prefer factory construction from async code.
- `#[events]` defines a listener interface. Attach it to a source with
  `#[eventful(ProducerEvents, shard = Worker)]`, implement it on a listener, and use
  `producer.on_produce().connect(&reporter)` to dispatch on the listener's shard.
- `#[asynchronize]` generates handle wrappers. `#[asynced]` makes a wrapper async
  even when the original method is synchronous. Awaiting it returns the method's
  result; untracked events emitted by that method may still be pending.
- Tracked emissions return futures that observe handler completion and errors.
  The example's `produce_tracked` awaits them with `join_all`. It waits for every
  delivery outcome but discards the results; inspect them to handle failures.
- `#[action]` generates a normal handle method that queues work and returns
  immediately. Calling `reset_count()` needs no async context or `.await`.
- `upgrade_in_shard` queues a closure with a reference to a shard-local value
  so it can call ordinary methods such as `internal_report`. The reference stays on the
  owning shard. Use `deferred_upgrade_in_shard` to await a callback result.
- `foo.join(&bar)` borrows strong handles and returns an owned `JoinedHandles`
  group if their shard IDs match, otherwise `None`. Chain `.join(&baz)` for flat
  tuples of up to eight handles, then upgrade them together in one callback.

Dispatch arguments and results must be `Send + 'static`. Async callbacks can
interleave with other work while suspended. Connections use weak targets;
keep listener handles alive while the listeners are needed.

The default feature enables Tokio. Standard thread, calling-thread, and optional
Slint backends share the same handle implementation. See the guide for backend
selection, construction, delivery errors, and shutdown behavior.

Shard selections are mandatory. Scope inheritance covers directly written structs
and nested inline modules; external module files select their own shards. Main/UI
markers must be initialized on their owner thread before cross-thread submission.
The destination loop must run to complete `spawn`. Named shards cannot restart.

The old `shard_std!`, `shard_tokio!`, `shard_main!`, `shard_tokio_main!`,
`shard_slint!`, and `use_shard!` macros are replaced by declaration and selection.
`Eventful::EventLoopHandleType` and `default_handle()` are replaced by `Shard`;
backend handles still share `ShardEventHandle`.
