[![crates.io](https://img.shields.io/crates/v/eventful-rs.svg)](https://crates.io/crates/eventful-rs)
[![docs.rs](https://docs.rs/eventful-rs/badge.svg)](https://docs.rs/eventful-rs)
[![license](https://img.shields.io/crates/l/eventful-rs.svg)](#license)

# eventful-rs

Is your Rust code too boring? Want to spice up your async cravings? Then let's make
things a little more eventful! Add some events to your structs, connect that spaghetti
and let the magic happen. (or make it explode, whatever works for you)

This crate provides an event and async dispatch system for Rust, with a focus on ergonomics.
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
```

Rust 1.85+ for the core and default Tokio backend. Use `default-features = false`
for the standard executor only; the optional `slint` adapter requires Rust 1.92+.

## What it offers

- **Thread-affine state:** keep `Rc`, `Cell`, and `RefCell` on their owning shard.
- **Typed events:** connect listeners individually or by interface, with optional
  application-defined routing labels and tracked delivery.
- **Method dispatch:** await results or queue actions through generated handle methods.
- **Explicit lifetimes:** strong/weak handles, scoped connections, and coordinated shutdown.
- **Runtime choice:** dedicated threads, a main-thread loop, Tokio, or a Slint UI loop.

See the [complete quick start and API documentation](https://docs.rs/eventful-rs)
for the type definitions, delivery semantics, and lifecycle rules.

## Examples

| Run from this repository             | Demonstrates                                              |
| ------------------------------------ | --------------------------------------------------------- |
| `cargo run -p sharded-main`          | Batch processing, tracked events, actions, joined handles |
| `cargo run -p no-main-shard-example` | A synchronous application with background shards          |
| `cargo run -p connect-all`           | Whole-interface subscriptions and scoped cleanup          |
| `cargo run -p targeted-events`       | Topic routing with wildcard observers                     |
| `cargo run -p example_tokio`         | HTTP I/O on Tokio with a main-thread listener             |

See [the example index](https://github.com/DreamLogics/eventful-rs/tree/main/examples)
for coverage and [the development guide](https://github.com/DreamLogics/eventful-rs/blob/main/DEVELOPMENT.md)
for development checks. Queues are unbounded and async operations may interleave;
this library provides thread affinity, not transactional isolation.

## License

Copyright (c) 2026 Sanne Ladage and eventful-rs contributors.

Licensed under either [MIT](https://github.com/DreamLogics/eventful-rs/blob/main/LICENSE-MIT)
or [Apache-2.0](https://github.com/DreamLogics/eventful-rs/blob/main/LICENSE-APACHE), at your option.

Contributions are accepted under these same dual-license terms unless explicitly
agreed otherwise.
