#![cfg(feature = "slint")]
use eventful_rs::{
    EventLoop, EventLoopHandle, Eventful, HasEvents, InvokeError, ShardBinding, ShardRc,
};
use slint::platform::{EventLoopProxy, Platform, WindowAdapter};
use std::{cell::RefCell, rc::Rc, sync::mpsc, time::Duration};

eventful_rs::declare_shard!(pub Ui, runtime = slint);
#[eventful_rs::eventful(shard = Ui)]
struct UiState {
    value: Rc<usize>,
}

#[eventful_rs::events]
trait Updates {
    fn changed(&self, value: usize);
}
#[eventful_rs::eventful(Updates, shard = Ui)]
struct Source;
#[eventful_rs::eventful(shard = Ui)]
struct Listener {
    received: RefCell<Vec<usize>>,
    owner: std::thread::ThreadId,
}
impl Updates for Listener {
    fn changed(&self, value: usize) {
        assert_eq!(std::thread::current().id(), self.owner);
        self.received.borrow_mut().push(value);
    }
}

type LocalCallback = Box<dyn Fn(usize)>;

/// Set when the detached root is destroyed.
static ROOT_DROPPED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

#[eventful_rs::eventful(shard = Ui)]
struct Root;
impl Drop for Root {
    fn drop(&mut self) {
        ROOT_DROPPED.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

#[eventful_rs::eventful(shard = Ui)]
struct CallbackOwner {
    callback: RefCell<Option<LocalCallback>>,
    value: std::cell::Cell<usize>,
}

slint::slint! {
    #[edition = "1.19"]
    export component BridgeTestUi inherits Window {
        callback save();
        callback edit-paragraph(string, int);
    }
}

slint::slint! {
    #[edition = "1.19"]
    export component WeakCallbackUi inherits Window {
        callback save();
        callback rename(string);
        callback key-pressed(string, bool, bool);
        callback wide(int, int, int, int, int, int, int, int, int, int, int, int, int);
        callback scale(int, int) -> int;
        callback count() -> int;
    }
}

#[eventful_rs::eventful(shard = Ui)]
struct CallbackApp {
    log: Rc<RefCell<Vec<String>>>,
    factor: i32,
}
impl CallbackApp {
    fn record(&self, entry: String) {
        self.log.borrow_mut().push(entry);
    }
}

/// Wait until the shard has collected the value behind `weak`.
async fn wait_for_collection<T>(weak: &eventful_rs::ShardWeak<T>)
where
    T: Eventful + HasEvents<T::EventSetType> + 'static,
{
    let collected = async {
        while weak.upgrade().is_some() {
            futures_timer::Delay::new(Duration::from_millis(10)).await;
        }
    };
    futures::pin_mut!(collected);
    let deadline = futures_timer::Delay::new(Duration::from_secs(2));
    assert!(matches!(
        futures::future::select(collected, deadline).await,
        futures::future::Either::Left(_)
    ));
}

/// The callback macros go straight into generated `on_*` setters of any arity.
async fn weak_callback_macros_fit_slint_setters() {
    use eventful_rs::{weak_callback, weak_callback_or_else};
    let log = Rc::new(RefCell::new(Vec::new()));
    let app = CallbackApp::bind_local(CallbackApp {
        log: log.clone(),
        factor: 3,
        events: Default::default(),
    })
    .unwrap();
    let ui = WeakCallbackUi::new().unwrap();
    ui.on_save(weak_callback!(app, |app| app.record("save".into())));
    ui.on_rename(weak_callback!(app, |app, name| app.record(format!("rename {name}"))));
    // Annotations and `_` patterns are accepted.
    ui.on_key_pressed(weak_callback!(app, |app, text, ctrl: bool, _| {
        app.record(format!("key {text} {ctrl}"))
    }));
    ui.on_wide(weak_callback!(app, |app,
                                    a,
                                    b,
                                    c,
                                    d,
                                    e,
                                    f,
                                    g,
                                    h,
                                    i,
                                    j,
                                    k,
                                    l,
                                    m| {
        let sum = a + b + c + d + e + f + g + h + i + j + k + l + m;
        app.record(format!("wide {sum}"));
    }));
    ui.on_scale(weak_callback_or_else!(
        app,
        |app, value, offset| value * app.factor + offset,
        |value, _| -value,
    ));
    ui.on_count(weak_callback_or_else!(
        app,
        |app: &ShardRc<CallbackApp>| app.log.borrow().len() as i32,
        || -1
    ));

    ui.invoke_save();
    ui.invoke_rename("draft".into());
    ui.invoke_key_pressed("k".into(), true, false);
    ui.invoke_wide(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13);
    assert_eq!(ui.invoke_scale(4, 1), 13);
    assert_eq!(ui.invoke_count(), 4);
    assert_eq!(
        *log.borrow(),
        ["save", "rename draft", "key k true", "wide 91"]
    );

    // The callbacks hold no strong reference, so the value is still collected.
    let weak = ShardRc::downgrade(&app);
    drop(app);
    wait_for_collection(&weak).await;
    ui.invoke_save();
    ui.invoke_rename("ignored".into());
    ui.invoke_key_pressed("x".into(), false, false);
    ui.invoke_wide(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1);
    assert_eq!(log.borrow().len(), 4); // Expired callbacks are skipped.
    assert_eq!(ui.invoke_scale(4, 1), -4);
    assert_eq!(ui.invoke_count(), -1);
}

#[eventful_rs::slint_events(component = BridgeTestUi)]
trait UiActions {
    fn save(&self);
    fn edit_paragraph(&self, id: slint::SharedString, index: i32);
    #[cfg(any())]
    fn nonexistent(&self, missing: MissingType);
}

#[eventful_rs::eventful(shard = Ui)]
struct BridgedWindow {
    ui: BridgeTestUi,
    bridge: UiActionsBridge,
    calls: RefCell<Vec<String>>,
}
impl UiActions for BridgedWindow {
    fn save(&self) {
        self.calls.borrow_mut().push("save".into());
    }
    fn edit_paragraph(&self, id: slint::SharedString, index: i32) {
        self.calls.borrow_mut().push(format!("{id}:{index}"));
    }
}

enum Message {
    Invoke(Box<dyn FnOnce() + Send>),
    Quit,
}
struct Proxy(mpsc::Sender<Message>);
impl EventLoopProxy for Proxy {
    fn quit_event_loop(&self) -> Result<(), slint::EventLoopError> {
        self.0
            .send(Message::Quit)
            .map_err(|_| slint::EventLoopError::EventLoopTerminated)
    }
    fn invoke_from_event_loop(
        &self,
        event: Box<dyn FnOnce() + Send>,
    ) -> Result<(), slint::EventLoopError> {
        self.0
            .send(Message::Invoke(event))
            .map_err(|_| slint::EventLoopError::EventLoopTerminated)
    }
}
struct Headless {
    tx: mpsc::Sender<Message>,
    rx: mpsc::Receiver<Message>,
}
impl Platform for Headless {
    fn create_window_adapter(&self) -> Result<Rc<dyn WindowAdapter>, slint::PlatformError> {
        Ok(i_slint_renderer_software::MinimalSoftwareWindow::new(
            i_slint_renderer_software::RepaintBufferType::NewBuffer,
        ))
    }
    fn new_event_loop_proxy(&self) -> Option<Box<dyn EventLoopProxy>> {
        Some(Box::new(Proxy(self.tx.clone())))
    }
    fn run_event_loop(&self) -> Result<(), slint::PlatformError> {
        while let Message::Invoke(f) = self
            .rx
            .recv_timeout(Duration::from_secs(5))
            .expect("Slint driver stalled")
        {
            f();
        }
        Ok(())
    }
}
#[test]
fn slint_runs_non_send_work_on_ui_thread_and_drains_before_quit() {
    let (tx, rx) = mpsc::channel();
    slint::platform::set_platform(Box::new(Headless { tx, rx })).unwrap();
    // Lazy initialization must make binding available before the driver is polled.
    let local = ShardRc::try_bind(UiState {
        value: Rc::new(13),
        events: Default::default(),
    })
    .unwrap();
    assert_eq!(*local.value, 13);
    // Local tasks may also be spawned before the Slint loop polls the driver.
    let (task_done, task_thread) = futures::channel::oneshot::channel();
    let early = Ui::spawn_local(async move {
        let _ = task_done.send(std::thread::current().id());
    })
    .unwrap();
    // A detached root lives until shutdown, which destroys it on the UI thread.
    let root = ShardRc::detach(
        ShardRc::try_bind(Root {
            events: Default::default(),
        })
        .unwrap(),
    );
    let endless = Ui::spawn_local(futures::future::pending::<()>())
        .unwrap()
        .detach();
    let shard = Ui::shard();
    std::thread::spawn(|| {
        assert!(matches!(
            ShardRc::try_bind(UiState {
                value: Rc::new(0),
                events: Default::default(),
            }),
            Err(InvokeError::WrongShard)
        ));
    })
    .join()
    .unwrap();
    let state = UiState::spawn(|| UiState {
        value: Rc::new(11),
        events: Default::default(),
    });
    let handle = shard.handle();
    let owner = std::thread::current().id();
    let source = ShardRc::try_bind(Source {
        events: Default::default(),
    })
    .unwrap();
    let listener = ShardRc::try_bind(Listener {
        received: RefCell::default(),
        owner,
        events: Default::default(),
    })
    .unwrap();
    // A plain connection token may be dropped; the source owns the connection.
    drop(source.connect_to(&listener));
    source.events.changed().emit(1);
    assert!(listener.received.borrow().is_empty()); // Delivery is queued.
    let (done, result) = futures::channel::oneshot::channel();
    std::thread::spawn(move || {
        handle.invoke_async(async move || {
            let local = Rc::new(7);
            futures_timer::Delay::new(Duration::from_millis(5)).await;
            assert_eq!(*local, 7);
            done.send(std::thread::current().id()).unwrap();
        })
    })
    .join()
    .unwrap();
    slint::spawn_local(async move {
        source.events.changed().tracked().emit(2).await.unwrap();
        assert_eq!(*listener.received.borrow(), [1, 2]);
        source.events.changed().emit(3);
        source.events.changed().tracked().emit(4).await.unwrap();
        assert_eq!(*listener.received.borrow(), [1, 2, 3, 4]);
        // Generated Slint callbacks forward to a wrapper on the same UI shard.
        let ui = BridgeTestUi::new().unwrap();
        let bridge = UiActionsBridge::new(&ui);
        let window = BridgedWindow::bind_local(BridgedWindow {
            ui,
            bridge,
            calls: RefCell::default(),
            events: Default::default(),
        })
        .unwrap();
        window.bridge.connect_to(&window);
        window.ui.invoke_save();
        window.ui.invoke_edit_paragraph("paragraph".into(), 7);
        assert!(window.calls.borrow().is_empty());
        // A tracked event on the same receiver observes the preceding queued calls.
        window.ui.invoke_save();
        Ui::shard()
            .handle()
            .try_invoke_tracked(|| {})
            .await
            .unwrap();
        assert_eq!(*window.calls.borrow(), ["save", "paragraph:7", "save"]);

        // Reinstalling replaces the callbacks. Dropping the old bridge must not
        // clear newer handlers, and retaining old event storage must not keep it active.
        let ui = BridgeTestUi::new().unwrap();
        let old_bridge = UiActionsBridge::new(&ui);
        old_bridge.connect_to(&window);
        let retained_events = old_bridge.events().clone();
        let new_bridge = UiActionsBridge::new(&ui);
        new_bridge.connect_to(&window);
        drop(old_bridge);
        ui.invoke_save();
        ui.invoke_save();
        Ui::shard()
            .handle()
            .try_invoke_tracked(|| {})
            .await
            .unwrap();
        assert_eq!(window.calls.borrow().len(), 5);
        let retained_new_events = new_bridge.events().clone();
        drop(new_bridge);
        ui.invoke_save(); // Neither retained event set keeps forwarding alive.
        window.ui.invoke_save();
        Ui::shard()
            .handle()
            .try_invoke_tracked(|| {})
            .await
            .unwrap();
        assert_eq!(window.calls.borrow().len(), 6);
        drop((retained_events, retained_new_events));

        let weak_window = ShardRc::downgrade(&window);
        drop(window); // UI -> callback -> bridge -> receiver must not form a cycle.
        let collect_window = async {
            while weak_window.upgrade().is_some() {
                futures_timer::Delay::new(Duration::from_millis(10)).await;
            }
        };
        futures::pin_mut!(collect_window);
        assert!(matches!(
            futures::future::select(
                collect_window,
                futures_timer::Delay::new(Duration::from_secs(2))
            )
            .await,
            futures::future::Either::Left(_)
        ));

        let callback_owner = CallbackOwner::bind_local(CallbackOwner {
            callback: RefCell::default(),
            value: std::cell::Cell::new(0),
            events: Default::default(),
        })
        .unwrap();
        let weak = ShardRc::downgrade(&callback_owner);
        let callback = callback_owner.weak_callback(|owner, value| owner.value.set(value));
        callback(23);
        assert_eq!(weak.upgrade().unwrap().value.get(), 23);
        let result_callback =
            callback_owner.weak_callback_or_else(|owner, ()| owner.value.get(), |()| 99);
        assert_eq!(result_callback(()), 23);
        callback_owner.callback.replace(Some(Box::new(
            callback_owner.weak_callback(|_, _: usize| {}),
        )));
        drop(callback_owner);
        // The stored callback must not create a cycle. Collection releases the owner.
        let collected = async {
            while weak.upgrade().is_some() {
                futures_timer::Delay::new(Duration::from_millis(10)).await;
            }
        };
        futures::pin_mut!(collected);
        let deadline = futures_timer::Delay::new(Duration::from_secs(2));
        assert!(matches!(
            futures::future::select(collected, deadline).await,
            futures::future::Either::Left(_)
        ));
        callback(42); // Expired unit callbacks are skipped.
        assert_eq!(result_callback(()), 99);
        assert!(weak.clone().upgrade().is_none());
        weak_callback_macros_fit_slint_setters().await;
        let bound = ShardRc::try_bind(UiState {
            value: Rc::new(17),
            events: Default::default(),
        })
        .unwrap();
        assert_eq!(*bound.value, 17);
        assert_eq!(result.await.unwrap(), owner);
        let state = state.await.unwrap();
        assert_eq!(
            shard
                .handle()
                .try_deferred_invoke(state, async move |s| {
                    assert_eq!(std::thread::current().id(), owner);
                    *s.value
                })
                .await,
            Ok(11)
        );
        assert_eq!(task_thread.await.unwrap(), owner);
        assert!(early.is_finished());
        // Shutdown aborts endless local tasks instead of waiting for the grace period.
        let stopping = std::time::Instant::now();
        shard.shutdown_async().await.unwrap();
        assert!(stopping.elapsed() < Duration::from_secs(1));
        assert!(endless.is_finished());
        assert!(ROOT_DROPPED.load(std::sync::atomic::Ordering::SeqCst));
        assert_eq!(root.try_local().err(), Some(InvokeError::WrongShard));
        assert_eq!(Ui::spawn_local(async {}).err(), Some(InvokeError::Closed));
        assert!(matches!(
            ShardRc::try_bind(UiState {
                value: Rc::new(0),
                events: Default::default(),
            }),
            Err(InvokeError::WrongShard)
        ));
        slint::quit_event_loop().unwrap();
    })
    .unwrap();
    slint::run_event_loop_until_quit().unwrap();
}
