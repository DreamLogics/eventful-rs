# Connecting all events

Run from the workspace root:

```sh
cargo run -p connect-all
```

The [example](src/main.rs) puts a producer and reporter on separate shards.
`producer.connect(&reporter)` subscribes to both `BatchEvents::item` and
`BatchEvents::finished` in one call. The reporter implements the full interface.

It demonstrates three connection lifetimes:

- `connections.disconnect()` removes all subscriptions in the group.
- `connect(...).scoped()` disconnects when its guard leaves scope.
- Dropping a plain group keeps its subscriptions active.

Tracked emissions wait for listener completion before checking totals, so the
example needs no sleeps. Keep the reporter's handle alive: subscriptions use weak
targets. `#[sharded_main(Main)]` handles driving the main shard and joining the
background shards when the example finishes.

These calls use a remote `ShardRcHandle`. When called on a local `ShardRc`,
`connect` additionally ties the group to the underlying value: constructors can
call `window.connect(&manager);` and discard the token, and all subscriptions
are removed when that window value is destroyed. Clones and in-flight callbacks
keep the value alive; retaining its event set alone does not retain subscriptions.
