# eventful-rs-macros

Implementation of the procedural macros re-exported by `eventful-rs`. Depend on `eventful-rs` in applications.

`#[sharded(shard = path::Marker)]` selects an affinity for directly written eventful
structs in an inline module and its nested inline modules. Paths resolve inside
the annotated module. `#[eventful(Events, shard = Marker)]` overrides the scope;
omit `Events` to generate an empty event set. Without an explicit or enclosing selection, `#[eventful]` uses the alias
created by `use_shard!(shard = Marker);` in the current module. A missing
`__EventfulFileShard` error means a selection is needed. External module files select their own affinity.

`#[sharded_main(Main)]` drives an explicitly declared main-thread marker; declaration
uses the runtime crate's `declare_shard!` macro. It does not select a scope default.

Declare `#[events]` interfaces at module scope. Generated implementation details
live in a private module; public signal builders, emission sets, and signal
extension traits retain the interface visibility. Source methods emit through
`self.events.changed().tracked().emit(value)`. Handles expose subscription-only
builders, including `.labelled(label).on_shard(&handle).connect(callback)`.
There is no emitter extension trait.

`#[events]` also implements `ConnectEvents<Listener>` for its generated event set,
enabling `source.connect_to(&listener)` on shard references and handles. Bulk connections
use the same weak targets, delivery paths, and listener bounds as individual signals.

`#[slint_events(component = Ui)]` adds a generated `InterfaceBridge` to an event
interface. Construct it with `InterfaceBridge::new(&ui)` and retain it in the
wrapper; `bridge.connect_to(&wrapper)` forwards the listed Slint callbacks as
queued events. It supports callbacks without return values and installs weak
forwarding closures, so the component and wrapper do not retain each other.

## License

Copyright (c) 2026 Sanne Ladage and eventful-rs contributors.

Licensed under either [MIT](https://github.com/DreamLogics/eventful-rs/blob/main/LICENSE-MIT)
or [Apache-2.0](https://github.com/DreamLogics/eventful-rs/blob/main/LICENSE-APACHE), at your option.

Contributions are accepted under these same dual-license terms unless explicitly
agreed otherwise.
