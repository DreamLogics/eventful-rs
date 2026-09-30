#![cfg(feature = "slint")]
use eventful_rs::{EventLoop, EventLoopHandle, Eventful, InvokeError, ShardRc};
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

#[eventful_rs::eventful(shard = Ui)]
struct CallbackOwner {
    callback: RefCell<Option<LocalCallback>>,
    value: std::cell::Cell<usize>,
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
        Err(slint::PlatformError::Unsupported)
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
    source.emit_changed(1);
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
        source.emit_changed_tracked(2).await.unwrap();
        assert_eq!(*listener.received.borrow(), [1, 2]);
        source.emit_changed(3);
        source.emit_changed_tracked(4).await.unwrap();
        assert_eq!(*listener.received.borrow(), [1, 2, 3, 4]);
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
        shard.shutdown_async().await.unwrap();
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
