//! Queue scheduling, panic isolation, release-driven collection, and shutdown draining.
use super::{Command, LocalFuture, Receiver, ShardEventHandle, spawner, store};
use crate::shard_handle::ShardRcStore;
use futures::{FutureExt, StreamExt, future::CatchUnwind, stream::FuturesUnordered};
use std::{any::Any, cell::RefCell, panic::AssertUnwindSafe, time::Duration};

/// A local future whose unwinding panics are caught without another allocation.
type Guarded = CatchUnwind<AssertUnwindSafe<LocalFuture>>;

/// Isolate unwinding callback panics so the driver can keep processing work.
fn guarded(future: LocalFuture) -> Guarded {
    AssertUnwindSafe(future).catch_unwind()
}

/// Report a callback that unwound; the driver keeps processing work.
fn report(outcome: Result<(), Box<dyn Any + Send>>) {
    if outcome.is_err() {
        eprintln!("eventful-rs: shard callback panicked");
    }
}

/// Remove first, then drop outside the RefCell borrow: destructors may bind values.
fn collect(store: &RefCell<ShardRcStore>) {
    let retired = store.borrow_mut().take_garbage();
    drop(retired);
}

/// Release detached values newest first. Each is destroyed before the next is
/// released, so a value detached later goes before values it may depend on.
/// Values still referenced elsewhere are left to ordinary collection.
fn release_detached(store: &RefCell<ShardRcStore>) {
    loop {
        let Some(token) = store.borrow_mut().pop_detached() else {
            break;
        };
        let key = token.key();
        drop(token);
        let retired = store.borrow_mut().retire(key);
        drop(retired); // Destructors may detach or bind; never under the borrow.
    }
}

/// Recheck token-free values once callbacks that borrowed them may have finished.
fn collect_orphans(store: &RefCell<ShardRcStore>) {
    if store.borrow().has_orphans() {
        collect(store);
    }
}

/// Start jobs in admission order, poll local futures, then drain with a grace period.
/// Spawned local tasks are polled separately and aborted before the drain.
pub(crate) async fn drive(
    mut rx: Receiver,
    handle: ShardEventHandle,
    grace: Duration,
    initial: Option<LocalFuture>,
) {
    let context = store(handle.shard_id);
    let mut pending = FuturesUnordered::<Guarded>::new();
    if let Some(initial) = initial {
        pending.push(guarded(initial));
    }
    // Collection is driven by token releases rather than polling, so an idle
    // shard stays asleep.
    let signal = context.borrow().signal();
    let spawner = spawner(handle.shard_id);
    let mut local_tasks = FuturesUnordered::<Guarded>::new();
    enum Next {
        Command(Option<Command>),
        Completed,
        Collect,
        Spawned(Vec<LocalFuture>),
    }
    let mut turns = 0usize;
    loop {
        turns += 1;
        if turns == 64 {
            turns = 0;
            let mut yielded = false;
            futures::future::poll_fn(|cx| {
                if yielded {
                    std::task::Poll::Ready(())
                } else {
                    yielded = true;
                    cx.waker().wake_by_ref();
                    std::task::Poll::Pending
                }
            })
            .await;
        }
        let event = {
            let next = async {
                if pending.is_empty() {
                    futures::future::pending::<()>().await
                } else if let Some(outcome) = pending.next().await {
                    report(outcome);
                }
            }
            .fuse();
            let task = async {
                if local_tasks.is_empty() {
                    futures::future::pending::<()>().await
                } else if let Some(outcome) = local_tasks.next().await {
                    report(outcome);
                }
            }
            .fuse();
            futures::pin_mut!(next, task);
            futures::select! {
                command = rx.next().fuse() => Next::Command(command),
                _ = next => Next::Completed,
                _ = task => Next::Completed,
                _ = futures::future::poll_fn(|cx| signal.poll_candidates(cx)).fuse() => Next::Collect,
                spawned = futures::future::poll_fn(|cx| spawner.poll_queued(cx)).fuse() => Next::Spawned(spawned),
            }
        };
        match event {
            Next::Command(Some(Command::Run(run))) => {
                report(std::panic::catch_unwind(AssertUnwindSafe(|| run(&context))));
                collect_orphans(&context);
            }
            Next::Command(Some(Command::Job(job))) => {
                let mut future = guarded(job(context.clone()));
                // Start in admission order. A Pending operation then interleaves
                // with later work; a synchronous callback finishes right here.
                let polled =
                    futures::future::poll_fn(|cx| std::task::Poll::Ready(future.poll_unpin(cx)))
                        .await;
                match polled {
                    std::task::Poll::Pending => pending.push(future),
                    std::task::Poll::Ready(outcome) => {
                        report(outcome);
                        collect_orphans(&context);
                    }
                }
            }
            // Shutdown closed the queue and every accepted command has been taken.
            Next::Command(None) => break,
            Next::Completed => collect_orphans(&context),
            Next::Collect => collect(&context),
            // First polled on a later turn, never inside the spawning call.
            Next::Spawned(spawned) => local_tasks.extend(spawned.into_iter().map(guarded)),
        }
    }
    rx.close();
    // Local tasks are typically endless loops: abort them instead of letting
    // them hold the drain for the full grace period. Drop them on this thread.
    drop(spawner.close());
    drop(local_tasks);
    collect_orphans(&context);
    {
        let drain = async {
            while let Some(outcome) = pending.next().await {
                report(outcome);
            }
        }
        .fuse();
        let deadline = futures_timer::Delay::new(grace).fuse();
        futures::pin_mut!(drain, deadline);
        futures::select! { _ = drain => {}, _ = deadline => {} }
    }
    // Drop canceled futures while the context is still on its owner thread.
    drop(pending);
    release_detached(&context);
    collect(&context);
    handle.request_shutdown();
}
