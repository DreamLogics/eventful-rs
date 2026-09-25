# Development and release

On Debian/Ubuntu, install the native dependencies for the optional Slint backend
before running the all-feature checks:

```sh
sudo apt-get update
sudo apt-get install --yes --no-install-recommends pkg-config libfontconfig1-dev
```

Slint's font discovery dependency uses `pkg-config` to locate `fontconfig.pc`.
The core/default library and MSRV checks do not need these packages.

Run `./scripts/check.sh` on stable Rust. It checks formatting, all example binaries,
core/default/all-feature tests, public and private API documentation, Clippy,
rustdoc links, and both packaged crates. Generated documentation is at
`target/doc/eventful_rs/index.html`; use `cargo doc -p eventful-rs --all-features
--no-deps --open` to browse it. Build artifacts remain untracked.

Check the supported compiler independently:

```sh
cargo +1.85.0 check -p eventful-rs --lib --locked
cargo +1.85.0 check -p eventful-rs --lib --no-default-features --locked
```

The MSRV applies to the core/default library. Slint 1.17 requires Rust 1.92+;
workspace examples and test dependencies are validated on stable Rust.

## Publish

1. Set the workspace version and matching macro dependency version, finalize the
   changelog, and confirm repository/license metadata. Keep the root and package
   README identical; the detailed guide lives in `eventful-rs/GUIDE.md`.
2. Commit the release and run `./scripts/check.sh`. Packaging requires a clean
   working tree. During review, `cargo package -p eventful-rs-macros -p eventful-rs
   --locked --allow-dirty` verifies the current files without publishing.
3. With crates.io credentials configured, publish in dependency order:

   ```sh
   cargo publish -p eventful-rs-macros --locked
   cargo publish -p eventful-rs --locked
   ```

   Wait until the macro version is available in the registry before publishing
   the runtime. Inspect each command's result before continuing.
4. Verify both crates.io pages and the docs.rs all-feature build, then tag the
   release. Package verification alone does not check name ownership or credentials.

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
  into `events`, `dispatch`, `affinity`, and `entry`.
