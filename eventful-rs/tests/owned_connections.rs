use eventful_rs::*;
use futures::{channel::oneshot, executor::block_on};
use std::{
    rc::Rc,
    sync::{Arc, Mutex},
};

declare_shard!(pub WindowShard, runtime = main);

#[events]
trait WindowEvents {
    fn save(&self, name: String);
}
#[eventful(WindowEvents, shard = WindowShard)]
struct AppWindow {
    ui: Rc<()>,
    dropped: Option<oneshot::Sender<()>>,
}
impl AppWindow {
    fn new(doc_man: ShardRcHandle<DocumentManager>, dropped: oneshot::Sender<()>) -> ShardRc<Self> {
        let w: ShardRc<Self> = ShardRc::try_bind(Self {
            ui: Rc::new(()),
            dropped: Some(dropped),
            events: Default::default(),
        })
        .unwrap();
        ShardRc::connect(&w, &doc_man);
        w
    }
}
impl Drop for AppWindow {
    fn drop(&mut self) {
        if let Some(tx) = self.dropped.take() {
            let _ = tx.send(());
        }
    }
}
#[eventful(shard = DynamicShard)]
struct DocumentManager {
    saved: Arc<Mutex<Vec<String>>>,
}
impl WindowEvents for DocumentManager {
    fn save(&self, name: String) {
        self.saved.lock().unwrap().push(name);
    }
}

#[test]
fn constructor_connections_follow_the_value_not_the_local_wrapper_or_event_set() {
    let docs = shard::Shard::new("documents");
    let saved = Arc::new(Mutex::new(Vec::new()));
    let output = saved.clone();
    let manager = docs.bind(move |bind| {
        bind(DocumentManager {
            saved: output,
            events: Default::default(),
        })
        .to_handle()
    });
    WindowShard::shard().run_main(move || async move {
        let (tx, dropped) = oneshot::channel();
        let window = AppWindow::new(manager.clone(), tx);
        assert_eq!(Rc::strong_count(&window.ui), 1);
        let clone = window.clone();
        let handle = window.to_handle();
        let events = window.events().clone(); // Retaining signals must not retain subscriptions past T's destruction.
        drop(window);
        events
            .save()
            .emit_tracked("local clone".into())
            .await
            .unwrap();
        drop(clone);
        events
            .save()
            .emit_tracked("remote handle".into())
            .await
            .unwrap();
        // Ensure the source's stored value still resolves for remote callbacks.
        assert_eq!(
            handle
                .deferred_upgrade_in_shard(async |w| Rc::strong_count(&w.ui))
                .await,
            1
        );
        let (started, ready) = oneshot::channel();
        let (resume, gate) = oneshot::channel();
        let pending =
            WindowShard::handle().try_deferred_invoke(handle.downgrade(), async move |w| {
                started.send(()).unwrap();
                gate.await.unwrap();
                assert_eq!(Rc::strong_count(&w.ui), 1);
            });
        ready.await.unwrap();
        drop(handle);
        events
            .save()
            .emit_tracked("in-flight callback".into())
            .await
            .unwrap();
        resume.send(()).unwrap();
        pending.await.unwrap();
        dropped.await.unwrap(); // Wait for actual shard collection, without a timing assumption.
        events
            .save()
            .emit_tracked("after destruction".into())
            .await
            .unwrap();
        assert_eq!(
            *saved.lock().unwrap(),
            ["local clone", "remote handle", "in-flight callback"]
        );
    });
    docs.join().unwrap();
}

#[test]
fn ownership_survives_store_shutdown_but_not_the_last_local_reference() {
    let docs = shard::Shard::new("shutdown-documents");
    let saved = Arc::new(Mutex::new(Vec::new()));
    let output = saved.clone();
    let manager = docs.bind(move |bind| {
        bind(DocumentManager {
            saved: output,
            events: Default::default(),
        })
        .to_handle()
    });
    declare_shard!(ShutdownShard, runtime = main);
    #[eventful(WindowEvents, shard = ShutdownShard)]
    struct LocalWindow;
    let destination = manager.clone();
    let (window, token) = ShutdownShard::shard().run_main(move || async move {
        let window: ShardRc<LocalWindow> = ShardRc::try_bind(LocalWindow {
            events: Default::default(),
        })
        .unwrap();
        let token = ShardRc::connect(&window, &destination);
        (window, token)
    });
    // The store has gone, but a local Rc still owns the value and its guards.
    let events = window.events().clone();
    block_on(events.save().emit_tracked("after shutdown".into())).unwrap();
    assert_eq!(*saved.lock().unwrap(), ["after shutdown"]);
    drop(window);
    // An externally retained event set AND connection token do not extend
    // the lifetime of a value-owned subscription.
    block_on(events.save().emit_tracked("after drop".into())).unwrap();
    assert_eq!(*saved.lock().unwrap(), ["after shutdown"]);
    token.disconnect(); // Harmless after automatic cleanup.
    docs.join().unwrap();
}
