# Development

## Source map

Runtime crate paths are relative to `eventful-rs/src`.

- `event.rs` and `connection.rs`: signals, routing, delivery, subscription lifetime.
- `eventful.rs` and `binding.rs`: value construction and shard affinity.
- `shard_handle.rs`, `shard_handle/joined.rs`, `shard_handle/store.rs`: local/remote
  references, grouped access, and private ownership storage.
- `event_loop.rs`, `engine.rs`, `engine/driver.rs`: backend contracts, submission,
  and scheduling/draining.
- `background.rs`, `main_loop.rs`, `registry.rs`: shared lifecycle implementations.
  Public backend modules (`std_rt`, `local_rt`, `tokio_rt`, `tokio_local_rt`, `slint_rt`)
  are thin adapters.
- `task.rs`: detached blocking work and cooperative cancellation.
- `eventful-rs-macros/src`: public entry points in `lib.rs`, expansion logic grouped
  into `events`, `slint_events`, `dispatch`, `affinity`, `declaration`, and `entry`;
  `attributes` handles conditional compilation.

## Checks

`./scripts/check.sh` runs everything CI runs: formatting, tests with each feature
set, Clippy, documentation, and packaging. It also requires `README.md` and
`eventful-rs/README.md` to be identical, so edit both together.

Compiler-diagnostic snapshots in `eventful-rs/tests/ui` are pinned to the toolchain
named in `scripts/check-ui.sh` (install it with `rustup toolchain install <version>
--profile minimal`). Set `SKIP_UI_TESTS=1` to skip them locally.

The crate-level documentation is `eventful-rs/GUIDE.md`; its code blocks run as
doctests.

See [the changelog](CHANGELOG.md) for migration instructions.
