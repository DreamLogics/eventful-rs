# Changelog

## Unreleased

- Shards now collect a value as soon as its last strong reference is released and
  in-flight callbacks on it finish, instead of polling every 100 ms. Idle shards,
  including the Slint UI thread, no longer wake periodically. Weak receivers whose
  strong handles were all dropped now expire promptly rather than up to 100 ms later.
- Emissions share the subscription list instead of copying it, and the last
  matching subscriber receives the original payload instead of a clone.
- Fixed unbounded growth of the shard registry for joined shards, and of
  value-owned connection lists after repeated connect/disconnect.
- **Breaking:** removed `Eventful::as_rc_handle`. Use `ShardRc::try_from_ref(&value)`
  for a local reference or `ShardRcHandle::try_from_ref(&value)` for a remote handle.
  Both return `Result<_, InvokeError>`: `WrongShard` off the owner thread and
  `ValueMissing` for a value that is not bound. Lookup no longer scans the store.
- Documentation: corrected stale module names, links, and `ShardRc::connect_to`
  guidance; the guide's previously ignored snippets now run as doctests.

## 0.2.0 - 2026-10-01

- Added `#[slint_events(component = Ui)]` to generate a typed callback bridge.
  Store the generated `InterfaceBridge` in a wrapper and use `connect_to` to
  deliver Slint callbacks as queued events without strong ownership cycles.

- Added local `ShardWeak` references and `weak_callback` / `weak_callback_or_else`
  helpers for callbacks that must not retain their owner.
- Added `connect_to` on local references and strong remote handles.
- Bulk connections from empty event sources now fail at compile time, including
  interfaces whose signals are all disabled by conditional compilation. Receivers
  may still have no outgoing events.
- Added `Eventful::bind_local` as the synchronous local counterpart to `spawn`.

- Fixed an issue with the Slint shard runtime, where one couldn't create a ShardRc prior to the Slint event-loop being started.
- Event interfaces now receive a defaulted role parameter. Use `.role::<Role>().connect(...)` for
  distinct receiver implementations, including bulk and labelled connections.
- Added `.with_receiver(...).connect(...)` for methods and capturing callbacks
  dispatched on the receiver's shard, without implementing the event interface.
- Fixed the Slint module declaration after the runtime module rename.
- Renamed shard runtime modules with more fitting names, also makes it less likely there will be name collisions.

### Breaking API migration

Emission and subscription access are now separate capabilities. The generated
private module holds the implementation, and re-exports the public API using
the event interface's visibility. `#[events]` interfaces belong at module scope.

| Before                                                    | 0.2                                                                 |
| --------------------------------------------------------- | ------------------------------------------------------------------- |
| `self.emit_changed(value)`                                | `self.events.changed().emit(value)`                                 |
| `self.emit_changed_tracked(value)`                        | `self.events.changed().tracked().emit(value)`                       |
| Labelled emission with a leading label argument           | `self.events.changed().labelled(label).emit(value)`                 |
| `signal.connect_fn(&receiver, callback)`                  | `signal.with_receiver(&receiver).connect(callback)`                 |
| `signal.connect_labelled(&receiver, label)`               | `signal.labelled(label).connect(&receiver)`                         |
| `signal.connect_labelled_fn(&receiver, label, callback)`  | `signal.labelled(label).with_receiver(&receiver).connect(callback)` |
| `signal.connect_as::<Role, _>(&receiver)`                 | `signal.role::<Role>().connect(&receiver)`                          |
| `signal.connect_labelled_as::<Role, _>(&receiver, label)` | `signal.labelled(label).role::<Role>().connect(&receiver)`          |
| `handle.connect_as::<Role, _>(&receiver)`                 | `handle.role::<Role>().connect(&receiver)`                          |
| `ShardRc::connect_as::<Role, _>(&local, &receiver)`       | `ShardRc::role::<Role>(&local).connect(&receiver)`                  |
| `events.connect_events_as::<Role, _>(&receiver)`          | `events.role::<Role>().connect(&receiver)`                          |

- Standalone callbacks use `signal.on_shard(&shard.handle()).connect(callback)`.
  Labels compose before or after shard selection. Captures require
  `Send + Sync + 'static`, and callbacks are always queued.
- Remove imports of generated `InterfaceEmittersExt` traits. Emission builders
  are inherent methods on the source's `events` field, which is no longer an
  `Arc<EventSet>`. `events: Default::default()` remains valid construction syntax.
- `HasEvents` and handles expose only subscription storage. Remote emission must
  call a source method or dispatch to the source with `deferred_upgrade_in_shard`.
  Subscription views have no emission methods, including after source destruction.
- For self-subscriptions, use `self.events.changed().signal()`. An emission set
  is not cloneable; emission builders borrow it. Subscription views can be retained.
- `.tracked().emit(...)` submits immediately and returns a completion observer
  that does not borrow the source. Dropping the observer does not cancel delivery.
- Plain connection tokens still do not disconnect on drop. Scoped guards and
  source-owned local bulk connections retain their existing cleanup behavior.
  Standalone callbacks have no weak receiver and retain captures until removed.
- Slint bridges expose signals and `.role::<Role>().connect(...)`; their installed
  callbacks own emission access internally. Use the component's callback invocation
  to simulate a UI event. Retained subscription storage does not keep forwarding alive.
- The low-level `Event` API remains available for explicitly owned raw events.

## 0.1.2 - 2026-09-28

- Added lookup of sharded rc handles from a reference.
- Renamed `file_scope!` and `#[scope]` macros to `use_shard!` and `#[sharded]`.
- Removed unnecessary restriction of preventing generic structs to be eventful.
- Rework the getting-started guide to make it a little more useful, full rewrite still in progress.
- Disable Tokio by default. Applications using the Tokio adapters must now enable
  `features = ["tokio"]` on their `eventful-rs` dependency.

## 0.1.1 - 2026-09-26

- Dual-license both crates under MIT OR Apache-2.0 and explicitly credit Sanne
  Ladage alongside contributors in copyright notices and author metadata.

- Add a manually triggered release workflow with changelog/version validation,
  full CI checks, dependency-ordered publishing, commit tags, and curated GitHub
  release notes. Support retrying partial releases from the same commit.

- Align the public API with the Rust API Guidelines: add unconditional `Debug`
  implementations for runtime handles, make identity and event storage read-only,
  seal the internal dispatch contract, and mark error enums non-exhaustive.
- Preserve conditional compilation and visibility in generated items and support
  renamed runtime dependencies. Add regression coverage for these macro contracts.

## 0.1.0 - 2026-09-25

- Initial release of `eventful-rs` and `eventful-rs-macros` crates. The library provides
  thread-affine state, typed events, method dispatch, explicit lifetimes, and runtime
  choice for Rust applications.
