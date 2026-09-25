#![cfg(feature = "slint")]
use eventful_rs::{EventLoop, EventLoopHandle, Eventful};
use slint::platform::{EventLoopProxy, Platform, WindowAdapter};
use std::{rc::Rc, sync::mpsc, time::Duration};

eventful_rs::declare_shard!(pub Ui, runtime = slint);
#[eventful_rs::eventful(shard = Ui)]
struct UiState {
    value: Rc<usize>,
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
    let shard = Ui::shard();
    let state = UiState::spawn(|| UiState {
        value: Rc::new(11),
        events: Default::default(),
    });
    let handle = shard.handle();
    let owner = std::thread::current().id();
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
        slint::quit_event_loop().unwrap();
    })
    .unwrap();
    slint::run_event_loop_until_quit().unwrap();
}
