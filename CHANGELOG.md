# Changelog

## Unreleased

- Align the public API with the Rust API Guidelines: add unconditional `Debug`
  implementations for runtime handles, make identity and event storage read-only,
  seal the internal dispatch contract, and mark error enums non-exhaustive.
- Preserve conditional compilation and visibility in generated items and support
  renamed runtime dependencies. Add regression coverage for these macro contracts.

### API guideline migration (breaking)

- Replace `as_handle()` with `to_handle()` and import `Sharded`.
- Replace local `ShardRc::from(value)` / `.into()` with `ShardRc::try_bind(value)?`.
  Replace remote handle conversions with `T::spawn(factory).await?`.
- Use `ShardRc::connect(&local, &listener)` for local bulk subscriptions.
- Read backend identity through `shard_id()` and events through `events()`;
  access generated signals through methods instead of event-set fields.
  Weak `events()` now returns `Option<Arc<_>>`.
- Export generated dispatch traits explicitly with `#[asynchronize(pub)]` or a
  restricted visibility when they must be imported from another module.
- Match `InvokeError` and `ShardError` with a fallback arm for future variants.

- Update the optional Slint backend to Slint 1.18 (locked to 1.18.1).

- Preserve event-interface visibility on generated extension traits so private
  payloads are not exposed through public tracked-emission interfaces.

- Prevent connection removal from running captured destructors under the event
  mutex, avoiding deadlocks when destructors reenter the same signal.
- Add a tested API guide, complete public/private doc comments, generated API
  documentation, realistic examples, and CI/package/MSRV release checks.
- Separate signal storage, backend contracts, lifecycle, and macro expansion
  modules. Make the implementation-only `ShardRcId` and `ShardRcStore` private
  and remove the unused store removal method before the initial release.
- Keep Rust 1.85 compatibility for the core/default library; document Slint's
  higher compiler requirement. Await tracked responses in the HTTP example.

- Tie local `ShardRc::connect` subscriptions to the underlying value's lifetime,
  allowing constructors to discard connection tokens. Remote-handle and individual
  signal connections retain their explicitly managed lifetimes.

- Add `connect(&listener)` to shard references and handles for subscribing to all
  events, with `ConnectionGroup` disconnection and scoped cleanup. Weak sources
  return `None` when their event set has expired.

- Add `file_scope!(shard = Marker)` for file-level defaults without an inline
  module wrapper; per-type and enclosing scope selections take precedence.

- Add named shard markers with `declare_shard!`, explicit `#[scope]` selection,
  per-type overrides, and inferred `Eventful::spawn` factory construction.
- Enforce named affinity during binding; retain runtime-selected values through
  explicit `DynamicShard` affinity.
- Preserve non-Send local `ShardRc` conversion and shared runtime handle aliases.

### Shard API migration (breaking)

| Previous API | Replacement |
| --- | --- |
| `shard_std!(WORKER)` and backend variants | `declare_shard!(pub Worker, runtime = std)`; choose `tokio`, `main`, `tokio_main`, or `slint` as needed |
| Implicit module defaults / `use_shard!(WORKER)` | `file_scope!(shard = Worker)` for a file, `#[scope(shard = super::Worker)]` on an inline module, or `#[eventful(shard = Worker)]` per type |
| `Eventful::EventLoopHandleType` / `default_handle()` | `Eventful::Shard`; obtain a named handle with `ShardBinding::handle()` |
| `#[sharded_main]` / `#[sharded_main(tokio)]` | Declare a main marker, then `#[sharded_main(Main)]` |
| `WORKER.join()` and other backend operations | `Worker::shard().join()` |
| Manual binding to a default destination | Prefer `T::spawn(factory).await?`; typed `Worker::bind_async` remains available |
| One eventful type bound to arbitrary instances | Explicit `type Shard = DynamicShard` or `#[eventful(shard = DynamicShard)]` |

`bind` and `bind_async` remain useful for runtime-selected shards and compound
factories. Local binding uses the fallible `ShardRc::try_bind`; cross-thread
construction uses a factory.
Runtime shard-ID checks and `join` checks remain necessary for erased handles and
dynamic values. Backend handle aliases remain available.

## 0.1.0 — release candidate

- Share queueing, wake-driven execution, error handling, and shutdown across backends.
- Replace unsafe shared RefCell contexts and raw main-result pointers with thread-local stores.
- Complete the Tokio main-thread adapter and repair the Slint macro and adapter.
- Keep value lookup borrows out of callbacks and awaits; destroy collected values outside store borrows.
- Reject wrong-shard value IDs and retain values while local Rc references are live.
- Make weak handles weak with respect to event sets as well as values.
- Add async binding/joining, fallible invocation helpers, and startup/shutdown error propagation.
- Remove unnecessary Send/Sync requirements on event receiver traits.
- Fix generated closure captures and add explicit unsupported-input diagnostics.
- Update every original example; make the HTTP example deterministic and offline.
- Add tests, CI, crate metadata, README, and release instructions.

### Migration

- Rename `InvokeError::ObjectMissing` to `InvokeError::ValueMissing`.

- Cross-thread binding from Tokio must use `bind_async`; same-shard binding remains direct.
- Backend handle types are aliases of a shared handle; shard IDs enforce destination identity.
- Weak-handle `events` is a Weak pointer. Use a strong handle to subscribe or emit through its event set.
- Generic annotated structs/impls and event traits are explicitly unsupported in this release.
- Handle From conversion requires a Send value; use a factory for non-Send values.
- ShardError sources now require Send + Sync for asynchronous joining.

- `EventLoopHandle::deferred_invoke` returns `BoxFuture<'static, R>` so its result can escape a temporary handle on Rust 1.85 without implicit lifetime capture.
- Runtime handle IDs are read through `EventLoopHandle::shard_id()` instead of a mutable public field.
