use eventful_rs::*;
use futures::{channel::oneshot, executor::block_on};
use std::{
    cell::RefCell,
    rc::Rc,
    sync::{Arc, mpsc},
    time::Duration,
};

struct Object {
    value: RefCell<usize>,
    local: Rc<()>,
    events: Arc<()>,
    dropped: Option<mpsc::Sender<std::thread::ThreadId>>,
}
impl Object {
    fn new() -> Self {
        Self {
            value: RefCell::new(0),
            local: Rc::new(()),
            events: Arc::new(()),
            dropped: None,
        }
    }
}
impl Eventful for Object {
    type EventSetType = ();
    type EventLoopHandleType = ShardEventHandle;
}
impl HasEvents<()> for Object {
    fn events(&self) -> &Arc<()> {
        &self.events
    }
}
impl Drop for Object {
    fn drop(&mut self) {
        if let Some(tx) = &self.dropped {
            let _ = tx.send(std::thread::current().id());
        }
    }
}

fn exercise(shard: &impl EventLoop<HandleType = ShardEventHandle>) {
    let handle = shard.handle();
    let object = shard.bind(|bind| bind(Object::new()).as_handle());
    let (go, ready) = oneshot::channel();
    let first = handle.try_deferred_invoke(object.clone(), async move |o| {
        ready.await.unwrap();
        *o.value.borrow_mut() += 1;
        assert_eq!(Rc::strong_count(&o.local), 1);
        std::thread::current().id()
    });
    handle.invoke(move || {
        go.send(()).unwrap();
    });
    let owner = block_on(first).unwrap();
    assert_ne!(owner, std::thread::current().id());
    assert_eq!(
        block_on(handle.try_deferred_invoke(object.clone(), async |o| *o.value.borrow())),
        Ok(1)
    );
    let panicked = handle.try_deferred_invoke(object.clone(), async |_| -> usize {
        panic!("expected panic")
    });
    assert_eq!(block_on(panicked), Err(InvokeError::Panicked));
    assert_eq!(
        block_on(handle.try_deferred_invoke(object, async |_| 42)),
        Ok(42)
    );
}

#[test]
fn standard_concurrency_results_and_panic_isolation() {
    let shard = shard::Shard::new("standard-test");
    exercise(&shard);
    shard.join().unwrap();
}
#[cfg(feature = "tokio")]
#[test]
fn tokio_concurrency_results_and_panic_isolation() {
    let shard = tokio::TokioShard::new("tokio-test");
    exercise(&shard);
    shard.join().unwrap();
}
#[test]
fn wrong_shard_never_resolves_colliding_object_id() {
    let a = shard::Shard::new("a");
    let b = shard::Shard::new("b");
    let object = a.bind(|bind| bind(Object::new()).as_handle());
    let _other = b.bind(|bind| bind(Object::new()).as_handle());
    assert_eq!(
        block_on(b.handle().try_deferred_invoke(object, async |_| 1)),
        Err(InvokeError::WrongShard)
    );
    a.join().unwrap();
    b.join().unwrap();
}
#[test]
fn shutdown_cancels_pending_results_rejects_submissions_and_is_repeatable() {
    let shard = shard::Shard::try_new("cancel", Duration::from_millis(20)).unwrap();
    let object = shard.bind(|bind| bind(Object::new()).as_handle());
    let pending = shard
        .handle()
        .try_deferred_invoke(object, async |_| std::future::pending::<()>().await);
    shard.join().unwrap();
    shard.join().unwrap();
    assert_eq!(block_on(pending), Err(InvokeError::Canceled));
    assert_eq!(shard.handle().try_invoke(|| {}), Err(InvokeError::Closed));
}
#[test]
fn final_object_drop_occurs_on_owner_even_when_handle_drops_elsewhere() {
    let shard = shard::Shard::new("drop-owner");
    let (tx, rx) = mpsc::channel();
    let object = shard.bind(move |bind| {
        let mut object = Object::new();
        object.dropped = Some(tx);
        bind(object).as_handle()
    });
    let owner = block_on(
        shard
            .handle()
            .try_deferred_invoke(object.clone(), async |_| std::thread::current().id()),
    )
    .unwrap();
    let weak = object.downgrade();
    std::thread::spawn(move || drop(object)).join().unwrap();
    assert_eq!(rx.recv_timeout(Duration::from_secs(3)).unwrap(), owner);
    assert_eq!(
        block_on(shard.handle().try_deferred_invoke(weak, async |_| 1)),
        Err(InvokeError::ObjectMissing)
    );
    shard.join().unwrap();
}
#[test]
fn local_main_accepts_non_send_future_and_result() {
    let shard = local::LocalShard::new();
    let value = shard.run_main(|| async { Rc::new(17) });
    assert_eq!(*value, 17);
}
#[test]
fn local_main_propagates_panic_without_hanging() {
    let shard = local::LocalShard::new();
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            shard.run_main(|| async { panic!("main panic") });
        }))
        .is_err()
    );
}
#[cfg(feature = "tokio")]
#[test]
fn tokio_main_drives_timers_and_nested_submission() {
    let shard = tokio_local::TokioLocalShard::new("main");
    let handle = shard.handle();
    let owner = std::thread::current().id();
    shard.run_main(move || async move {
        let (tx, rx) = oneshot::channel();
        handle.invoke_async(async move || {
            let local = Rc::new(1);
            ::tokio::time::sleep(Duration::from_millis(5)).await;
            assert_eq!(*local, 1);
            tx.send(std::thread::current().id()).unwrap();
        });
        assert_eq!(rx.await.unwrap(), owner);
    });
}
#[cfg(feature = "tokio")]
#[test]
fn async_bind_and_join_from_external_tokio_runtime() {
    let rt = ::tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let shard = tokio::TokioShard::new("async-bind");
    rt.block_on(async {
        let object = shard
            .bind_async(|bind| bind(Object::new()).as_handle())
            .await
            .unwrap();
        let value = shard
            .handle()
            .try_deferred_invoke(object, async |_| {
                ::tokio::time::sleep(Duration::from_millis(5)).await;
                7
            })
            .await
            .unwrap();
        assert_eq!(value, 7);
        assert!(shard.join().is_err());
        shard.join_async().await.unwrap();
    });
}

#[test]
fn local_loop_can_stop_without_a_main_result() {
    let shard = local::LocalShard::new();
    let handle = shard.handle();
    let stop = handle.clone();
    handle.invoke(move || stop.request_shutdown());
    shard.run_event_loop();
}

#[test]
fn same_shard_nested_deferred_call_progresses() {
    let shard = shard::Shard::new("nested");
    let object = shard.bind(|bind| bind(Object::new()).as_handle());
    let handle = shard.handle();
    let nested = object.clone();
    let outer = handle.clone();
    let result = handle.try_deferred_invoke(object, async move |_| {
        outer
            .try_deferred_invoke(nested, async |_| 19)
            .await
            .unwrap()
    });
    assert_eq!(block_on(result), Ok(19));
    shard.join().unwrap();
}

#[test]
fn concurrent_joiners_wait_for_actual_completion() {
    let shard = Arc::new(shard::Shard::new("joiners"));
    let (tx, rx) = mpsc::channel();
    shard.handle().invoke(move || {
        tx.send(()).unwrap();
    });
    let threads: Vec<_> = (0..4)
        .map(|_| {
            let shard = shard.clone();
            std::thread::spawn(move || shard.join().unwrap())
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
    rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(shard.handle().is_closed());
}

#[test]
fn deferred_future_outlives_the_submitting_handle() {
    let shard = shard::Shard::new("detached-receiver");
    let object = shard.bind(|bind| bind(Object::new()).as_handle());
    let result = shard.handle().deferred_invoke(object, async |_| 23);
    assert_eq!(block_on(result), 23);
    shard.join().unwrap();
}
