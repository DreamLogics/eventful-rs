# Changelog

## 0.1.3 - tbd

- Added local `ShardWeak` references and `weak_callback` / `weak_callback_or_else`
  helpers for callbacks that must not retain their owner.
- Added `connect_to` on local references and strong remote handles.
- Bulk connections from empty event sources now fail at compile time, including
  interfaces whose signals are all disabled by conditional compilation. Receivers
  may still have no outgoing events.
- Added `Eventful::bind_local` as the synchronous local counterpart to `spawn`.

- Fixed an issue with the Slint shard runtime, where one couldn't create a ShardRc prior to the Slint event-loop being started.
- Event interfaces now receive a defaulted role parameter. Use `connect_as` for
  distinct receiver implementations, including bulk and labelled connections.
- Added `connect_fn` and `connect_labelled_fn` for methods and capturing callbacks
  dispatched on the receiver's shard, without implementing the event interface.
- Fixed the Slint module declaration after the runtime module rename.
- Renamed shard runtime modules with more fitting names, also makes it less likely there will be name collisions.

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
