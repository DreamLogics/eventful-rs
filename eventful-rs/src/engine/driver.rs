//! Queue scheduling, panic isolation, release-driven collection, and shutdown draining.
use super::{Command, LocalFuture, Receiver, ShardEventHandle, store};
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

/// Recheck token-free values once callbacks that borrowed them may have finished.
fn collect_orphans(store: &RefCell<ShardRcStore>) {
    if store.borrow().has_orphans() {
        collect(store);
    }
}

/// Start jobs in admission order, poll local futures, then drain with a grace period.
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
    enum Next {
        Command(Option<Command>),
        Completed,
        Collect,
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
            futures::pin_mut!(next);
            futures::select! {
                command = rx.next().fuse() => Next::Command(command),
                _ = next => Next::Completed,
                _ = futures::future::poll_fn(|cx| signal.poll_candidates(cx)).fuse() => Next::Collect,
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
        }
    }
    rx.close();
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
    collect(&context);
    handle.request_shutdown();
}
