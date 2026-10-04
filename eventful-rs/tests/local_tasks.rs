//! Shard-local tasks: owner-thread polling, ownership, abort, panics, and shutdown.
use eventful_rs::*;
use futures::{channel::oneshot, executor::block_on};
use std::{
    cell::{Cell, RefCell},
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

/// Give the driver a few turns; local tasks are first polled on a later turn.
async fn settle() {
    futures_timer::Delay::new(Duration::from_millis(20)).await;
}

/// Wait until a condition holds, failing after two seconds.
async fn eventually(mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(2);
    while !condition() {
        assert!(Instant::now() < deadline, "condition not reached in time");
        futures_timer::Delay::new(Duration::from_millis(2)).await;
    }
}

/// Spawn on a background shard from its own thread; report the polling thread.
fn spawn_reports_owner<S: ShardBinding>() -> (thread::ThreadId, thread::ThreadId) {
    let (tx, rx) = oneshot::channel();
    let owner = block_on(S::handle().try_deferred_invoke_local(move || {
        let local = Rc::new(Cell::new(None));
        let seen = local.clone();
        S::spawn_local(async move {
            seen.set(Some(thread::current().id()));
            let _ = tx.send(seen.get().unwrap());
        })
        .unwrap()
        .detach();
        thread::current().id()
    }));
    (owner, block_on(rx).unwrap())
}

/// Run a closure on a shard and return its result.
trait InvokeLocal {
    fn try_deferred_invoke_local<R: Send + 'static>(
        &self,
        f: impl FnOnce() -> R + Send + 'static,
    ) -> impl Future<Output = R>;
}
impl InvokeLocal for ShardEventHandle {
    async fn try_deferred_invoke_local<R: Send + 'static>(
        &self,
        f: impl FnOnce() -> R + Send + 'static,
    ) -> R {
        let (tx, rx) = oneshot::channel();
        self.try_invoke(move || {
            let _ = tx.send(f());
        })
        .unwrap();
        rx.await.unwrap()
    }
}

declare_shard!(StdWorker, runtime = std);

#[test]
fn std_tasks_run_on_the_owner_thread() {
    let (owner, polled) = spawn_reports_owner::<StdWorker>();
    assert_eq!(owner, polled);
    assert_ne!(owner, thread::current().id());
    StdWorker::shard().join().unwrap();
}

#[cfg(feature = "tokio")]
declare_shard!(TokioWorker, runtime = tokio);

#[cfg(feature = "tokio")]
#[test]
fn tokio_tasks_run_on_the_owner_thread_with_tokio_services() {
    let (owner, polled) = spawn_reports_owner::<TokioWorker>();
    assert_eq!(owner, polled);
    let (tx, rx) = oneshot::channel();
    block_on(TokioWorker::handle().try_deferred_invoke_local(move || {
        TokioWorker::spawn_local(async move {
            tokio::time::sleep(Duration::from_millis(1)).await;
            let _ = tx.send(());
        })
        .unwrap()
        .detach();
    }));
    block_on(rx).unwrap();
    TokioWorker::shard().join().unwrap();
}

declare_shard!(Main, runtime = main);

#[test]
fn main_tasks_run_on_the_calling_thread() {
    let caller = thread::current().id();
    Main::shard().run_main(move || async move {
        let (tx, rx) = oneshot::channel();
        let task = Main::spawn_local(async move {
            let _ = tx.send(thread::current().id());
        })
        .unwrap();
        assert_eq!(rx.await.unwrap(), caller);
        assert!(task.is_finished());
    });
}

#[cfg(feature = "tokio")]
declare_shard!(TokioMain, runtime = tokio_main);

#[cfg(feature = "tokio")]
#[test]
fn tokio_main_tasks_can_use_tokio_timers() {
    let caller = thread::current().id();
    TokioMain::shard().run_main(move || async move {
        let (tx, rx) = oneshot::channel();
        TokioMain::spawn_local(async move {
            tokio::time::sleep(Duration::from_millis(1)).await;
            let _ = tx.send(thread::current().id());
        })
        .unwrap()
        .detach();
        assert_eq!(rx.await.unwrap(), caller);
    });
}

declare_shard!(StateShard, runtime = std);

#[eventful(shard = StateShard)]
struct Store {
    items: Rc<RefCell<Vec<u32>>>,
}

#[asynchronize]
impl Store {
    #[action]
    fn fill_in_background(&self) {
        let items = self.items.clone();
        let this = ShardRc::try_from_ref(self).unwrap();
        ShardRc::spawn_local(&this, async move {
            items.borrow_mut().push(1);
            settle().await;
            items.borrow_mut().push(2);
        })
        .unwrap()
        .detach();
    }

    #[asynced]
    fn items(&self) -> Vec<u32> {
        self.items.borrow().clone()
    }
}

#[test]
fn task_state_is_visible_to_later_dispatched_calls() {
    let store = block_on(Store::spawn(|| Store {
        items: Rc::default(),
        events: Default::default(),
    }))
    .unwrap();
    store.fill_in_background();
    block_on(async {
        let deadline = Instant::now() + Duration::from_secs(2);
        while store.items().await != [1, 2] {
            assert!(Instant::now() < deadline);
            settle().await;
        }
    });
    drop(store);
    StateShard::shard().join().unwrap();
}

declare_shard!(Closing, runtime = std);
declare_shard!(Elsewhere, runtime = std);

#[test]
fn spawning_off_shard_or_after_shutdown_fails() {
    assert_eq!(
        Closing::spawn_local(async {}).err(),
        Some(InvokeError::WrongShard)
    );
    // Another shard's thread is not this shard's context either.
    let other = block_on(
        Elsewhere::handle().try_deferred_invoke_local(|| Closing::spawn_local(async {}).err()),
    );
    assert_eq!(other, Some(InvokeError::WrongShard));
    // Work drained after shutdown begins can no longer spawn.
    let (tx, rx) = std::sync::mpsc::channel();
    Closing::handle()
        .try_invoke_async(async move || {
            settle().await;
            let _ = tx.send(Closing::spawn_local(async {}).err());
        })
        .unwrap();
    Closing::handle().request_shutdown();
    assert_eq!(rx.recv().unwrap(), Some(InvokeError::Closed));
    Closing::shard().join().unwrap();
    Elsewhere::shard().join().unwrap();
}

declare_shard!(AbortMain, runtime = main);

#[test]
fn dropping_aborts_detach_keeps_running_and_handles_abort() {
    AbortMain::shard().run_main(|| async {
        let ticker = |count: Rc<Cell<u32>>| async move {
            loop {
                count.set(count.get() + 1);
                futures_timer::Delay::new(Duration::from_millis(1)).await;
            }
        };

        let dropped = Rc::new(Cell::new(0));
        let task = AbortMain::spawn_local(ticker(dropped.clone())).unwrap();
        eventually(|| dropped.get() > 0).await;
        drop(task);
        settle().await;
        let stopped_at = dropped.get();
        settle().await;
        assert_eq!(dropped.get(), stopped_at);

        let detached = Rc::new(Cell::new(0));
        let handle = AbortMain::spawn_local(ticker(detached.clone()))
            .unwrap()
            .detach();
        eventually(|| detached.get() > 2).await;
        assert!(!handle.is_finished());
        let remote = handle.clone();
        thread::spawn(move || remote.abort()).join().unwrap();
        eventually(|| handle.is_finished()).await;
        let stopped_at = detached.get();
        settle().await;
        assert_eq!(detached.get(), stopped_at);

        let explicit = Rc::new(Cell::new(0));
        let task = AbortMain::spawn_local(ticker(explicit.clone())).unwrap();
        eventually(|| explicit.get() > 0).await;
        task.abort();
        eventually(|| task.is_finished()).await;
    });
}

declare_shard!(OwnedMain, runtime = main);

#[eventful(shard = OwnedMain)]
struct Feed {
    polls: Cell<u32>,
}

#[test]
fn owned_tasks_abort_with_their_value_and_hold_it_weakly() {
    OwnedMain::shard().run_main(|| async {
        let feed = Feed::bind_local(Feed {
            polls: Cell::new(0),
            events: Default::default(),
        })
        .unwrap();
        let weak = ShardRc::downgrade(&feed);
        let task = ShardRc::spawn_owned(&feed, |feed| async move {
            loop {
                match feed.upgrade() {
                    Some(feed) => feed.polls.set(feed.polls.get() + 1),
                    None => return,
                }
                futures_timer::Delay::new(Duration::from_millis(1)).await;
            }
        })
        .unwrap();
        eventually(|| feed.polls.get() > 2).await;
        assert!(!task.is_finished());
        // The task's weak reference does not keep the value alive.
        drop(feed);
        eventually(|| weak.upgrade().is_none()).await;
        eventually(|| task.is_finished()).await;

        // An owned task can also be aborted early through its handle.
        let feed = Feed::bind_local(Feed {
            polls: Cell::new(0),
            events: Default::default(),
        })
        .unwrap();
        let task = ShardRc::spawn_owned(&feed, |_| futures::future::pending()).unwrap();
        task.abort();
        eventually(|| task.is_finished()).await;
    });
}

declare_shard!(PanicMain, runtime = main);

#[test]
fn panicking_tasks_finish_and_the_shard_keeps_running() {
    PanicMain::shard().run_main(|| async {
        let task = PanicMain::spawn_local(async {
            settle().await;
            panic!("local task failure");
        })
        .unwrap();
        eventually(|| task.is_finished()).await;
        let (tx, rx) = oneshot::channel();
        PanicMain::spawn_local(async move {
            let _ = tx.send(());
        })
        .unwrap()
        .detach();
        rx.await.unwrap();
        assert_eq!(PanicMain::handle().try_invoke_tracked(|| {}).await, Ok(()));
    });
}

declare_shard!(Endless, runtime = std);

#[test]
fn shutdown_aborts_endless_tasks_without_waiting_for_the_grace_period() {
    let ticks = Arc::new(AtomicUsize::new(0));
    let counter = ticks.clone();
    block_on(Endless::handle().try_deferred_invoke_local(move || {
        Endless::spawn_local(async move {
            loop {
                counter.fetch_add(1, Ordering::Relaxed);
                futures_timer::Delay::new(Duration::from_millis(1)).await;
            }
        })
        .unwrap()
        .detach();
    }));
    while ticks.load(Ordering::Relaxed) == 0 {
        thread::sleep(Duration::from_millis(1));
    }
    let started = Instant::now();
    Endless::shard().join().unwrap();
    // The default grace period is five seconds.
    assert!(started.elapsed() < Duration::from_secs(1));
}

declare_shard!(BorrowMain, runtime = main);

#[test]
fn spawning_inside_a_borrow_defers_the_first_poll() {
    BorrowMain::shard().run_main(|| async {
        let state = Rc::new(RefCell::new(Vec::new()));
        let task = {
            let mut borrowed = state.borrow_mut();
            let seen = state.clone();
            let task = BorrowMain::spawn_local(async move {
                seen.borrow_mut().push("task");
            })
            .unwrap();
            borrowed.push("spawner");
            task
        };
        eventually(|| task.is_finished()).await;
        assert_eq!(*state.borrow(), ["spawner", "task"]);
    });
}

declare_shard!(ResultMain, runtime = main);

#[test]
fn awaiting_a_task_yields_its_output_or_why_it_stopped() {
    ResultMain::shard().run_main(|| async {
        // Outputs need not be Send.
        let task = ResultMain::spawn_local(async { Rc::new(41 + 1) }).unwrap();
        assert_eq!(*task.await.unwrap(), 42);

        let panicking = ResultMain::spawn_local(async {
            settle().await;
            panic!("local task failure");
        })
        .unwrap();
        assert_eq!(panicking.await, Err(InvokeError::Panicked));

        let aborted = ResultMain::spawn_local(futures::future::pending::<u32>()).unwrap();
        aborted.abort();
        assert_eq!(aborted.await, Err(InvokeError::Canceled));
    });
}

declare_shard!(ShutdownResult, runtime = std);

#[test]
fn shutdown_reports_unfinished_tasks_as_canceled() {
    let (tx, rx) = std::sync::mpsc::channel();
    let (spawned_tx, spawned) = std::sync::mpsc::channel();
    ShutdownResult::handle()
        .try_invoke_async(async move || {
            let endless = ShutdownResult::spawn_local(futures::future::pending::<()>()).unwrap();
            let _ = spawned_tx.send(());
            // The shutdown below aborts the task while this job drains.
            let _ = tx.send(endless.await);
        })
        .unwrap();
    spawned.recv().unwrap();
    ShutdownResult::handle().request_shutdown();
    assert_eq!(rx.recv().unwrap(), Err(InvokeError::Canceled));
    ShutdownResult::shard().join().unwrap();
}
