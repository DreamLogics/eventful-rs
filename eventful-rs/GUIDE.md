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
eventful-rs = "0.2"
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
# fn main() {}
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
`self.events.imported().tracked().emit(name).await?`. The `events` field exposes emission builders
for the declared event interface. It delivers the event to connected listeners and
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
            self.events.imported().tracked().emit(name.to_owned()).await?;
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
For a module of related types, [`macro@sharded`] can set their shard together.

Use `spawn` to construct values where they belong. Its closure can build `Rc`,
`Cell`, and `RefCell` state on that thread. Captures sent into the closure must be
`Send`, but the constructed value doesn't have to be. The returned
[`ShardRcHandle`] can be cloned and passed to other threads.

Mark methods you want to call through handles with `#[asynced]`. When splitting
code into modules, use `#[asynchronize(pub)]` and import the generated extension
trait at the call site. If a command needs no result, `#[action]` queues it
immediately, including from synchronous code.

Use events when other parts of the application need to react to a change.
`self.events.imported().emit(name)` queues progress without waiting for listeners;
`self.events.imported().tracked().emit(name).await?` waits for their handlers. In our example,
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
eventful-rs = { version = "0.2", features = ["tokio"] }
```

Then select `runtime = tokio` for that worker. Use `runtime = tokio_main` if the
main thread also needs Tokio. Tokio is optional and disabled by default.

For a Slint application, enable `slint` and use `runtime = slint` to deliver
callbacks through the UI loop. The application selects its Slint backend and
renderer. Initialize the shard on the UI thread, and await `shutdown_async()`
before quitting the UI loop. See the [Slint module](https://docs.rs/eventful-rs/latest/eventful_rs/slint/)
for integration details.

### Local values and UI callbacks

`Self::spawn(factory)` queues construction and returns a future yielding a strong
remote handle. `Self::bind_local(value)` binds immediately on the named shard's
thread and returns `Result<ShardRc<Self>, InvokeError>`. It keeps the usual explicit
struct initialization, including `events: Default::default()`.

Connect a local source with `source.connect_to(&receiver)`. The direction is always
source to receiver; the source must declare outgoing events. The receiver only
needs to implement their handlers. `ShardRc::connect_to(&source, &receiver)` is
available if the wrapped value has a method with the same name. The existing
`ShardRc::connect` remains supported.

Use `ShardRc::downgrade(&window)` for a local weak reference and `.upgrade()` to
recover an optional local strong reference synchronously. These references cannot
cross threads. Existing `ShardWeakHandle` remains the weak handle for queued access.
The store collects unused values periodically, so weak upgrades can succeed until
collection occurs. A local strong reference can also keep a value alive after
shard shutdown; upgrading it does not restart event delivery.

For callbacks owned by the UI, capture a weak reference instead of cloning the
window into its own callback:

```rust,ignore
let cancel = window.weak_callback(|window, ()| {
    window.events.on_close().emit();
    window.hide().unwrap();
});
window.ui.on_cancel(move || cancel(()));

let edit = window.weak_callback(|window, id| window.edit_paragraph(id));
window.ui.on_edit_paragraph(edit);
```

`weak_callback` skips calls once the value is destroyed. It takes one argument;
use `()` for zero arguments or a tuple adapter for multiple arguments. These
callbacks run synchronously on the calling thread without queuing. For callbacks
returning a value, `weak_callback_or_else(handler, fallback)` requires an explicit
fallback closure receiving the same argument. Avoid capturing a strong reference
to the owner inside either closure, which would reintroduce the cycle.

### Bridging Slint callbacks

Use `#[slint_events(component = ComponentType)]` on an explicit callback interface.
It generates the usual event traits and a `<TraitName>Bridge` source. Callback
names map to Slint setters: `edit_paragraph` installs `on_edit_paragraph`.

```rust,no_run
# #[cfg(feature = "slint")]
# mod example {
use eventful_rs::*;
slint::slint! {
    export component EditorUi inherits Window {
        callback save();
        callback edit-paragraph(string);
    }
}
declare_shard!(UiShard, runtime = slint);

#[slint_events(component = EditorUi)]
trait UiActions {
    fn save(&self);
    fn edit_paragraph(&self, id: slint::SharedString);
}

#[eventful(shard = UiShard)]
struct Editor {
    ui: EditorUi,
    ui_events: UiActionsBridge,
}
impl UiActions for Editor {
    fn save(&self) { /* save the document */ }
    fn edit_paragraph(&self, id: slint::SharedString) { /* open the editor */ }
}

# fn run() -> Result<(), Box<dyn std::error::Error>> {
let ui = EditorUi::new()?;
let ui_events = UiActionsBridge::new(&ui);
let editor = Editor::bind_local(Editor {
    ui, ui_events, events: Default::default(),
})?;
editor.ui_events.connect_to(&editor);
// Retain editor in your application and run the Slint event loop.
# Ok(())
# }
# }
# fn main() {}
```

The bridge is local to the UI thread and retains no component or receiver.
Store it in the wrapper so it lives as long as the wrapper. Slint callbacks hold
only a weak reference to the bridge; dropping the bridge stops forwarding even
if something else retains its event storage. Already queued events can still run.

Installation **replaces** the handlers for the listed callbacks. Later `on_*`
registrations replace the bridge's handlers in turn. Dropping a bridge does not
clear or restore handlers, so it cannot accidentally remove a newer registration.
Unlisted callbacks are unaffected.

Delivery is queued, including delivery to a wrapper on the same UI shard. Handlers
run after the original Slint callback returns. For async work, a handler can queue
an existing asynchronous handle method or start a Slint local task.

Callback argument types must match Slint's generated signatures and implement
`Clone + Send + 'static`, just like ordinary event payloads. For example, use
`slint::SharedString` for a Slint `string` and convert it in the receiver if needed.
Payload conversion and local-only payloads require manual callback wiring for now.
Callbacks with return values and labelled events are not supported by this bridge;
callbacks needing an immediate result must remain synchronous handlers.

Individual signal connections, receiver callbacks, and role selection
are also available through the generated subscription API. A bridge with no enabled
callbacks cannot be bulk-connected. Conditional callback declarations propagate
to the registrations as well as the event interface.

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


## Distinguishing sources and connecting methods

`#[events]` adds a defaulted role parameter to the listener trait. The producer
and its event storage do not need a role. Select the receiver implementation when
making a connection:

```rust
use eventful_rs::*;
#[events]
trait PushButtonEvents {
    fn on_clicked(&self);
}
struct ButtonA;
struct ButtonB;
#[eventful(PushButtonEvents, shard = DynamicShard)]
struct PushButton;
#[eventful(shard = DynamicShard)]
struct Dialog;
impl PushButtonEvents<ButtonA> for Dialog {
    fn on_clicked(&self) { self.pressed_a(); }
}
impl PushButtonEvents<ButtonB> for Dialog {
    fn on_clicked(&self) { self.pressed_b(); }
}
impl Dialog {
    fn pressed_a(&self) {}
    fn pressed_b(&self) {}
}
fn wire(a: &ShardRcHandle<PushButton>, b: &ShardRcHandle<PushButton>, dialog: &ShardRcHandle<Dialog>) {
    a.on_clicked().role::<ButtonA>().connect(dialog);
    b.on_clicked().role::<ButtonB>().connect(dialog);
}
# fn main() {}
```

`impl PushButtonEvents for Dialog` and ordinary `.connect(dialog)` select the
role `()`. Named roles need no values or marker traits. They describe the role of
a connection, not an intrinsic identity of the button.

For all events in an interface, use `source.role::<ButtonA>().connect(dialog)` on
a strong handle, or `source.events().role::<ButtonA>().connect(dialog)`.
Weak handles return `None` if their event storage has expired.
`ShardRc::role::<ButtonA>(&local_source).connect(dialog)` additionally ties the
subscriptions to the source value's lifetime, just like `ShardRc::connect`.

To connect a method directly, use
`a.on_clicked().with_receiver(dialog).connect(Dialog::pressed_a)`. A capturing closure works
as well: `a.on_clicked().with_receiver(dialog).connect(move |d| d.pressed_a())`.
Callbacks receive `&Dialog` followed by the event arguments and return `()`.
They need no event-interface implementation, but still satisfy any extra receiver
bound declared with `#[events(ExtraTrait)]`.

Callback captures must be `Send + Sync + 'static`; the receiver may contain
`Rc`, `Cell`, or `RefCell`, since callbacks run on its own shard. The connection
holds the receiver weakly. Explicitly capturing a strong handle in a closure
retains that handle in the usual way.

Labelled signals support `.labelled(label).role::<ButtonA>().connect(dialog)`
and `.labelled(label).with_receiver(dialog).connect(callback)`. Labels filter emissions;
roles select the receiver implementation. Unlabelled connections to labelled
signals receive every emission.

All these methods return the existing connection tokens or groups. Dropping a
plain token leaves the subscription active; use `disconnect()` or retain a
`scoped()` guard for cleanup. Tracked emission waits for selected callbacks to
finish and reports dispatch failures or panics.


## Emission and subscription builders (0.2)

The private `events` field on an eventful source owns emission access. Emit from
source methods with `self.events.changed().emit(value)`; add `.tracked()` to
observe completion and `.labelled(label)` for labelled events. A labelled
emission must select a label. Label and tracking selection can appear in either
order. Emission submits immediately; dropping its completion future does not
cancel delivery, and the future does not borrow the source.

Remote handles and `HasEvents::events()` expose subscription-only views. To
request an emission remotely, call a source action or dispatch onto the source:

```ignore
handle.deferred_upgrade_in_shard(async |source| {
    source.events.changed().tracked().emit(value).await
}).await?;
```

For standalone callbacks, select a shard without creating a receiver:

```ignore
source.changed().on_shard(&worker.handle()).connect(move |value| {
    // Runs on worker, including when the emitter is already on worker.
});
source.changed().labelled(topic).on_shard(&worker.handle()).connect(callback);
```

Captures require `Send + Sync + 'static`. There is no weak receiver to expire;
captures remain registered until disconnected or their subscription storage is
released. Use `.scoped()` on the returned connection for automatic cleanup.
The event interface's extra receiver bounds apply to receiver connections, not
to standalone callbacks. Callbacks return `()`; independently spawned work is
not included in tracked completion.

A source can subscribe to its own events through
`self.events.changed().signal().connect(&receiver)`. This conversion preserves
an already selected label. Subscription access cannot be converted back into
emission access. Weak handles expose `events() -> Option<Arc<EventSet>>`; upgrade
that view before choosing a signal. Retaining a subscription view does not keep
the source value alive.

Generated implementation details live in a private module. Declare `#[events]`
interfaces at module scope, including inside inline modules, so their payloads,
labels, and receiver bounds can be resolved from that module. Public types and
signal extension traits are re-exported with the interface's visibility. There
is no generated emitter extension trait. The low-level `Event` type remains
available for applications explicitly owning raw event storage.
