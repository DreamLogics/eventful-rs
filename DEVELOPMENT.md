# Development

## Source map

- `event.rs` and `connection.rs`: signals, routing, delivery, subscription lifetime.
- `eventful.rs` and `binding.rs`: value construction and shard affinity.
- `shard_handle.rs`, `shard_handle/joined.rs`, `shard_handle/store.rs`: local/remote
  references, grouped access, and private ownership storage.
- `event_loop.rs`, `engine.rs`, `engine/driver.rs`: backend contracts, submission,
  and scheduling/draining.
- `background.rs`, `main_loop.rs`, `registry.rs`: shared lifecycle implementations.
  Public backend modules (`shard`, `local`, `tokio`, `tokio_local`, `slint`) are thin adapters.
- `task.rs`: detached blocking work and cooperative cancellation.
- `eventful-rs-macros/src`: public entry points in `lib.rs`, expansion logic grouped
  into `events`, `dispatch`, `affinity`, `declaration`, and `entry`; `attributes`
  handles conditional compilation.

See [the changelog](CHANGELOG.md) for migration instructions.
