# eventful-rs-macros

Implementation of the procedural macros re-exported by `eventful-rs`. Depend on `eventful-rs` in applications.

`#[scope(shard = path::Marker)]` selects an affinity for directly written eventful
structs in an inline module and its nested inline modules. Paths resolve inside
the annotated module. `#[eventful(Events, shard = Marker)]` overrides the scope;
omit `Events` to generate an empty event set. Without an explicit or enclosing selection, `#[eventful]` uses the alias
created by `file_scope!(shard = Marker);` in the current module. A missing
`__EventfulFileShard` error means a selection is needed. External module files select their own affinity.

`#[sharded_main(Main)]` drives an explicitly declared main-thread marker; declaration
uses the runtime crate's `declare_shard!` macro. It does not select a scope default.

`#[events]` also implements `ConnectEvents<Listener>` for its generated event set,
enabling `source.connect(&listener)` on shard references and handles. Bulk connections
use the same weak targets, delivery paths, and listener bounds as individual signals.

## License

Copyright (c) 2026 Sanne Ladage and eventful-rs contributors.

Licensed under either [MIT](https://github.com/DreamLogics/eventful-rs/blob/main/LICENSE-MIT)
or [Apache-2.0](https://github.com/DreamLogics/eventful-rs/blob/main/LICENSE-APACHE), at your option.

Contributions are accepted under these same dual-license terms unless explicitly
agreed otherwise.
