# eventful-rs

This guide shows how to use eventful-rs to have struct values talk to each other across different threads (shards),
either by calling their methods via a dispatcher, or through events. Either experiment on your own, or follow the [tutorial](#tutorial-import-products-and-display-progress) to build a small product importer with progress display.

- [Quick start: set up your project](#quick-start)
- [Tutorial: import products and display progress](#tutorial-import-products-and-display-progress)
- [Using this in your project](#using-this-in-your-project)
- [When you need another runtime](#when-you-need-another-runtime)
- [Where to go next](#where-to-go-next)
- [Glossary](#glossary)

## Quick start

Create a Rust project (or use an existing one):

```sh
cargo new product-import
cd product-import
```

Add eventful-rs under `[dependencies]` in `Cargo.toml`:

```toml
[dependencies]
eventful-rs = "0.1"
```

By default a built-in thread runtime is included with basic async support, though it is advised to use tokio if you intend to work with more fancy async stuff.

## Tutorial: import products and display progress

We'll build a small command-line application that imports a list of products.
The importer will clean up product names, skip empty entries, and report each
accepted product to a display. Importing runs on a worker thread; the display
handles progress on the main thread, as it would in a UI application.

The importer should know how to process products without knowing anything about
the display. We'll give it a progress event and connect a listener to that event.
Calling the importer through a handle will run its method on the worker;
eventful-rs will deliver each progress update on the display's thread.

By the end, we'll be able to start an import, display its progress, and wait for
both the work and its updates to finish. The work itself is deliberately small
so we can focus on how the pieces communicate. First we'll walk through those
pieces, then put them together in a [complete program](#the-complete-program).

### 1. Choose where the work runs

A **shard** is an event loop and the values it owns on one thread. We'll use two:
one on the main thread for displaying progress, and one on a background thread
for importing products.

```rust
use eventful_rs::*;

declare_shard!(pub Main, runtime = main);
declare_shard!(pub Worker, runtime = std);
```

These declarations give us names we can use when assigning a struct to a thread.
The worker starts when first used. Later, `#[sharded_main(Main)]` will drive the
main thread's event loop while our application waits for the import to finish.

### 2. Give the importer a progress event

An event describes what happened. Define it as a trait with `#[events]`:

```rust
use eventful_rs::*;

#[events]
trait ImportEvents {
    fn imported(&self, name: String);
}
```

A listener implements this trait to decide what to do with each product name.
The importer doesn't need to know which listeners are connected.

In the complete program, `#[eventful(ImportEvents, shard = Worker)]` attaches
these events to `Importer` and assigns it to the worker. Its `Cell<usize>` counts
products across calls. That state stays on the worker; no mutex is needed to
access it there.

### 3. Make the import callable from the main thread

Write the import logic as an ordinary method. `#[asynchronize]` on the `impl`
and `#[asynced]` on the method also make it callable through the importer's
handle:

```text
let count = importer.import(rows).await?;
```

Here `importer` is a handle, not a reference to the worker's object. The call
sends `rows` to the worker, runs the method there, and brings its result back.
While the main thread awaits that result, its event loop can handle progress
updates.

For each accepted row, our method calls
`self.emit_imported_tracked(name).await?`. The `#[eventful]` macro generates this
method from the event trait. It delivers the event to connected listeners and
waits for their handlers to finish. This lets our final "Done" message mean that
all progress has been displayed too.

### 4. Connect a display and run the application

`Progress` lives on `Main` and implements `ImportEvents` by printing each product
name. We create both objects with `spawn`, which runs a constructor on the
object's chosen thread and returns a handle.

Then we connect them:

```text
let _subscription = importer.imported().connect(&progress).scoped();
```

The connection delivers `imported` events to `progress` on the main thread.
Keeping the scoped subscription in a variable keeps the connection active;
dropping it disconnects the listener. Keep the `progress` handle alive too:
a connection does not own its listener.

### The complete program

```rust
use eventful_rs::*;
use std::cell::Cell;

declare_shard!(pub Main, runtime = main);
declare_shard!(pub Worker, runtime = std);

#[events]
trait ImportEvents {
    fn imported(&self, name: String);
}

#[eventful(ImportEvents, shard = Worker)]
struct Importer {
    total: Cell<usize>,
}

#[asynchronize]
impl Importer {
    #[asynced]
    async fn import(&self, rows: Vec<String>) -> Result<usize, DeliveryError> {
        let mut count = 0;
        for row in rows {
            let name = row.trim();
            if name.is_empty() {
                continue;
            }
            self.total.set(self.total.get() + 1);
            self.emit_imported_tracked(name.to_owned()).await?;
            count += 1;
        }
        Ok(count)
    }

    #[asynced]
    fn total(&self) -> usize {
        self.total.get()
    }
}

#[eventful(shard = Main)]
struct Progress;

impl ImportEvents for Progress {
    fn imported(&self, name: String) {
        println!("Imported: {name}");
    }
}

#[sharded_main(Main)]
async fn main() -> Result<(), DeliveryError> {
    let importer = Importer::spawn(|| Importer {
        total: Cell::new(0),
        events: Default::default(),
    }).await?;
    let progress = Progress::spawn(|| Progress {
        events: Default::default(),
    }).await?;

    let _subscription = importer.imported().connect(&progress).scoped();
    let count = importer.import(vec![" Apples ".into(), "".into(), "Pears".into()]).await?;
    println!("Done: {count} products imported");
    assert_eq!(importer.total().await, 2);
    Ok(())
}
```

Run it with `cargo run`:

```text
Imported: Apples
Imported: Pears
Done: 2 products imported
```

The `events: Default::default()` fields initialize the event storage added by
`#[eventful]`, including on listeners. `#[sharded_main]` runs the main event loop
and joins background shards after `main` returns.

Notice that `total` is a synchronous method on `Importer`, but calling it through
the handle is asynchronous: even reading the counter has to happen on its owning
thread. You can add another import call and the counter will retain its value.

You now have a worker with its own state, a main-thread listener, and a connection
between them. To add another consumer, implement `ImportEvents` on another
`#[eventful]` type, create it, and connect it to the same signal. The importer
needs no changes.

## Using this in your project

Start by deciding which values need to share a thread. Several types can use the
same shard; you don't need a thread per object. Assign each with
`#[eventful(shard = Worker)]`, adding it's event trait if it produces events.
For a module of related types, [`macro@scope`] can set their shard together.

Use `spawn` to construct values where they belong. Its closure can build `Rc`,
`Cell`, and `RefCell` state on that thread. Captures sent into the closure must be
`Send`, but the constructed value doesn't have to be. The returned
[`ShardRcHandle`] can be cloned and passed to other threads.

Mark methods you want to call through handles with `#[asynced]`. When splitting
code into modules, use `#[asynchronize(pub)]` and import the generated extension
trait at the call site. If a command needs no result, `#[action]` queues it
immediately, including from synchronous code.

Use events when other parts of the application need to react to a change.
`emit_imported(name)` queues progress without waiting for listeners;
`emit_imported_tracked(name).await?` waits for their handlers. In our example,
waiting for each update keeps the producer paced by the display. A delivery
failure returns a [`DeliveryError`]; it does not undo the counter change or any
handler that already ran.

A few rules matter as your application grows:

- **Keep handles and subscriptions alive.** A strong handle keeps a value alive
  while its shard runs. Connections hold listeners weakly. Use `.scoped()` for
  cleanup on drop; a plain [`Connection`] stays connected until disconnected.
- **Allow for async interleaving.** A shard starts queued calls in order, but
  other calls may run when an async method awaits. Don't keep a `RefCell` borrow
  across an await if another call might access it.
- **Limit outstanding work.** Queues are unbounded. Awaiting each call or tracked
  event is one way to avoid submitting work faster than it can be handled.
- **Finish workflows before shutting down.** Complete the calls and deliveries
  your application needs before returning from `main`. For manual lifecycle
  management, see [`EventLoop`] and [`join_all_shards`].

## When you need another runtime

The example uses `std` for a background thread and `main` for the calling thread.
They can run async methods, but don't provide Tokio's timers or I/O reactor.
If your worker uses Tokio-based networking or timers, enable the adapter:

```toml
[dependencies]
eventful-rs = { version = "0.1", features = ["tokio"] }
```

Then select `runtime = tokio` for that worker. Use `runtime = tokio_main` if the
main thread also needs Tokio. Tokio is optional and disabled by default.

For a Slint application, enable `slint` and use `runtime = slint` to deliver
callbacks through the UI loop. The application selects its Slint backend and
renderer. Initialize the shard on the UI thread, and await `shutdown_async()`
before quitting the UI loop. See the [Slint module](https://docs.rs/eventful-rs/latest/eventful_rs/slint/)
for integration details.

## Where to go next

The [runnable examples](https://github.com/DreamLogics/eventful-rs/tree/main/examples)
build on these ideas:

- **`sharded-main`** expands the importer into modules and adds actions and access
  to multiple values in one call.
- **`no-main-shard-example`** integrates workers into a synchronous application.
- **`connect-all`** connects an entire event interface and manages subscriptions.
- **`targeted-events`** routes notifications by topic using [`EventLabel`].
- **`example_tokio`** performs HTTP I/O on a worker and reports to the main thread.

For more control, see [`ShardRcHandle::join`] for accessing several values on one
shard, [`DynamicShard`] for selecting a shard at runtime, and [`DeliveryError`]
for tracked delivery outcomes.

## Glossary

| Concept                   | Meaning                                                                                                                                                                                                                     |
| ------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Shard**                 | An event loop and the values it owns on one thread. Several objects can share a shard.                                                                                                                                      |
| **Event loop**            | Runs queued calls and event handlers on the shard's thread, and polls their async work.                                                                                                                                     |
| **Shard affinity**        | The choice of shard for a type, such as `shard = Worker`. It determines where its values are created and their methods run.                                                                                                 |
| **Eventful value**        | An object managed by a shard, usually declared with `#[eventful]`. It can produce events, listen to them, or simply expose methods.                                                                                         |
| **Handle**                | A way to communicate with an object on its shard without accessing the object directly. A strong [`ShardRcHandle`] can cross threads and keeps the object alive while the shard runs; a weak handle does not keep it alive. |
| **Event interface**       | A trait declared with `#[events]`. Its methods describe the notifications a producer can emit and a listener can handle.                                                                                                    |
| **Signal**                | One event on a particular producer, such as `importer.imported()`. Connect listeners to it to receive that notification.                                                                                                    |
| **Listener**              | An eventful value that implements an event interface. Its handlers run on its own shard.                                                                                                                                    |
| **Connection**            | A subscription linking a producer's signal to a listener. It holds the listener weakly, so the listener also needs a live strong handle.                                                                                    |
| **Scoped subscription**   | A connection wrapped with `.scoped()` so dropping it disconnects the listener. Dropping a plain connection does not disconnect it.                                                                                          |
| **Tracked emission**      | An event emission that returns a future for the selected handlers' outcomes. Awaiting it waits for those handlers, but not for work they independently spawn.                                                               |
| **Async method dispatch** | Calling a method through a handle with `#[asynced]`. Polling the call queues it on the object's shard; awaiting it obtains the method's result.                                                                             |
| **Action**                | A method marked `#[action]` that a handle queues immediately without waiting for a result. The method must return `()`.                                                                                                     |
| **Runtime backend**       | The implementation that drives a shard, such as the standard thread runtime, Tokio, or Slint's UI loop.                                                                                                                     |
