# Runnable examples

Run these commands from the workspace root. Every binary has an integration test
with a timeout; the HTTP example serves its own response on localhost.

| Command | Scenario and capabilities |
| --- | --- |
| `cargo run -p sharded-main` | Product import worker with main-thread progress, factory construction, file affinity, async methods, actions, tracked delivery, and joined handles |
| `cargo run -p no-main-shard-example` | Synchronous entry point, blocking construction, untracked events, and explicit producer-before-listener shutdown |
| `cargo run -p connect-all` | Batch reporting with whole-interface connections, explicit disconnect, scoped cleanup, and persistent subscriptions |
| `cargo run -p targeted-events` | Business notifications with topic masks, overlapping subscriptions, wildcard observers, local values, and inline module scope |
| `cargo run -p example_tokio` | HTTP client on a Tokio shard, local mutable state, response delivery to a main-thread listener, and error propagation |

The [API guide](../eventful-rs/GUIDE.md) covers lifecycle and failure semantics.
Runnable doc tests also demonstrate runtime-selected `DynamicShard` binding and
cooperative blocking tasks. The [Slint integration test](../eventful-rs/tests/slint.rs)
uses a headless host to check UI-thread ownership and draining before quit; a GUI
application must select its own renderer/backend. There is no standalone GUI demo.

For failure paths beyond these happy-path scenarios, see the library integration
tests: `runtime.rs` (cancellation, panics, shutdown), `labelled_events.rs` (routing),
`owned_connections.rs` (source-owned subscriptions), and `joined_handles.rs`.
