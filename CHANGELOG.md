# Changelog

## Unreleased

- Add named shard markers with `declare_shard!`, explicit `#[scope]` selection,
  per-type overrides, and inferred `Eventful::spawn` factory construction.
- Enforce named affinity during binding; retain runtime-selected values through
  explicit `DynamicShard` affinity.
- Preserve non-Send local `ShardRc` conversion and shared runtime handle aliases.

### Shard API migration (breaking)

| Previous API | Replacement |
| --- | --- |
| `shard_std!(WORKER)` and backend variants | `declare_shard!(pub Worker, runtime = std)`; choose `tokio`, `main`, `tokio_main`, or `slint` as needed |
| Implicit module defaults / `use_shard!(WORKER)` | `#[scope(shard = super::Worker)]` on an inline module, or `#[eventful(shard = Worker)]` per type |
| `Eventful::EventLoopHandleType` / `default_handle()` | `Eventful::Shard`; obtain a named handle with `ShardBinding::handle()` |
| `#[sharded_main]` / `#[sharded_main(tokio)]` | Declare a main marker, then `#[sharded_main(Main)]` |
| `WORKER.join()` and other backend operations | `Worker::shard().join()` |
| Manual binding to a default destination | Prefer `T::spawn(factory).await?`; typed `Worker::bind_async` remains available |
| One eventful type bound to arbitrary instances | Explicit `type Shard = DynamicShard` or `#[eventful(shard = DynamicShard)]` |

`bind` and `bind_async` remain useful for runtime-selected shards and compound
factories. The `From` conversions remain convenience APIs with distinct bounds:
local `ShardRc` conversion needs no `Send`; cross-thread handle conversion does.
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
