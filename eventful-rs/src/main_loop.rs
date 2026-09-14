use crate::{
    background::Runtime,
    engine::{self, ContextGuard, Receiver, ShardEventHandle},
};
use futures::FutureExt;
use std::{
    cell::RefCell, future::Future, panic::AssertUnwindSafe, rc::Rc, sync::Mutex, thread,
    time::Duration,
};

pub(crate) struct MainLoop {
    pub(crate) handle: ShardEventHandle,
    pub(crate) owner: thread::ThreadId,
    rx: Mutex<Option<Receiver>>,
    runtime: Runtime,
}
impl MainLoop {
    pub(crate) fn new(runtime: Runtime) -> Self {
        let (handle, rx) = ShardEventHandle::channel();
        Self {
            handle,
            rx: Mutex::new(Some(rx)),
            owner: thread::current().id(),
            runtime,
        }
    }
    pub(crate) fn run_main<F, RF, R>(&self, main: F) -> R
    where
        F: FnOnce() -> RF + 'static,
        RF: Future<Output = R> + 'static,
        R: 'static,
    {
        let result = Rc::new(RefCell::new(None));
        let output = result.clone();
        let handle = self.handle.clone();
        let main = Box::pin(async move {
            let value = AssertUnwindSafe(async move { main().await })
                .catch_unwind()
                .await;
            *output.borrow_mut() = Some(value);
            handle.request_shutdown();
        });
        self.run(Some(main));
        let value = result
            .borrow_mut()
            .take()
            .expect("main canceled before completion");
        match value {
            Ok(value) => value,
            Err(panic) => std::panic::resume_unwind(panic),
        }
    }
    pub(crate) fn run_event_loop(&self) {
        self.run(None);
    }
    fn run(&self, initial: Option<engine::LocalFuture>) {
        assert_eq!(
            thread::current().id(),
            self.owner,
            "run_main must run on the creating thread"
        );
        engine::assert_not_async("run_main cannot nest inside Tokio");
        let rx = self
            .rx
            .lock()
            .unwrap()
            .take()
            .expect("run_main may only be called once");
        let _ctx = ContextGuard::new(self.handle.shard_id);
        let drive = engine::drive(rx, self.handle.clone(), Duration::from_secs(5), initial);
        match self.runtime {
            Runtime::Standard => futures::executor::block_on(drive),
            #[cfg(feature = "tokio")]
            Runtime::Tokio => {
                let rt = ::tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("failed to create Tokio runtime");
                let local = ::tokio::task::LocalSet::new();
                rt.block_on(local.run_until(drive));
                drop(local);
                drop(rt);
            }
        }
    }
}
impl Drop for MainLoop {
    fn drop(&mut self) {
        self.handle.request_shutdown();
    }
}
