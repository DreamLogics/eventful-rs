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
