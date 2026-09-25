//! Queue scheduling, panic isolation, periodic collection, and shutdown draining.
use super::{Command, LocalFuture, Receiver, ShardEventHandle, store};
use crate::shard_handle::ShardRcStore;
use futures::{FutureExt, StreamExt, stream::FuturesUnordered};
use std::{cell::RefCell, panic::AssertUnwindSafe, time::Duration};

/// Isolate unwinding callback panics so the driver can keep processing work.
fn guarded(future: LocalFuture) -> LocalFuture {
    Box::pin(async move {
        if AssertUnwindSafe(future).catch_unwind().await.is_err() {
            eprintln!("eventful-rs: shard callback panicked");
        }
    })
}

/// Remove first, then drop outside the RefCell borrow: destructors may bind values.
fn collect(store: &RefCell<ShardRcStore>) {
    let retired = store.borrow_mut().take_garbage();
    drop(retired);
}

/// Start jobs in admission order, poll local futures, then drain with a grace period.
pub(crate) async fn drive(
    mut rx: Receiver,
    handle: ShardEventHandle,
    grace: Duration,
    initial: Option<LocalFuture>,
) {
    let context = store(handle.shard_id);
    let mut pending = FuturesUnordered::<LocalFuture>::new();
    if let Some(initial) = initial {
        pending.push(guarded(initial));
    }
    let mut gc = futures_timer::Delay::new(Duration::from_millis(100)).fuse();
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
                    futures::future::pending::<Option<()>>().await
                } else {
                    pending.next().await
                }
            }
            .fuse();
            futures::pin_mut!(next);
            futures::select! {
                command = rx.next().fuse() => Next::Command(command),
                _ = next => Next::Completed,
                _ = gc => Next::Collect,
            }
        };
        match event {
            Next::Command(Some(Command::Job(job))) => {
                let ctx = context.clone();
                let mut future = guarded(Box::pin(async move {
                    job(ctx).await;
                }));
                // Start in admission order. A Pending operation then interleaves
                // with later work; a synchronous callback finishes right here.
                let polled =
                    futures::future::poll_fn(|cx| std::task::Poll::Ready(future.as_mut().poll(cx)))
                        .await;
                if polled.is_pending() {
                    pending.push(future);
                }
            }
            Next::Command(Some(Command::Stop) | None) => break,
            Next::Completed => {}
            Next::Collect => {
                collect(&context);
                gc = futures_timer::Delay::new(Duration::from_millis(100)).fuse();
            }
        }
    }
    rx.close();
    {
        let drain = async { while pending.next().await.is_some() {} }.fuse();
        let deadline = futures_timer::Delay::new(grace).fuse();
        futures::pin_mut!(drain, deadline);
        futures::select! { _ = drain => {}, _ = deadline => {} }
    }
    // Drop canceled futures while the context is still on its owner thread.
    drop(pending);
    collect(&context);
    handle.request_shutdown();
}
