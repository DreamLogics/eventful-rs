//! Non-Send futures polled by a shard's own driver, with explicit ownership.
use crate::{InvokeError, ShardEventHandle, engine::LocalFuture};
use futures::{
    FutureExt,
    channel::oneshot,
    future::{AbortHandle, Aborted},
};
use std::{
    cell::{Cell, RefCell},
    future::Future,
    panic::AssertUnwindSafe,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Waker},
};

/// Per-shard queue of spawned futures awaiting their first poll by the driver.
#[derive(Default)]
pub(crate) struct LocalSpawner {
    /// Futures spawned since the driver last took the queue.
    queue: RefCell<Vec<LocalFuture>>,
    /// The driver's waker, registered when it last checked the queue.
    waker: RefCell<Option<Waker>>,
    /// Set when the shard stops; later spawns are rejected.
    closed: Cell<bool>,
}

impl LocalSpawner {
    /// Queue a future and wake the driver; the future is not polled here.
    fn push(&self, future: LocalFuture) -> Result<(), InvokeError> {
        if self.closed.get() {
            return Err(InvokeError::Closed);
        }
        self.queue.borrow_mut().push(future);
        // Wake outside the borrow in case the waker re-enters.
        let waker = self.waker.borrow().clone();
        if let Some(waker) = waker {
            waker.wake();
        }
        Ok(())
    }

    /// Ready with every queued future; registers the driver for later spawns.
    pub(crate) fn poll_queued(&self, cx: &mut Context<'_>) -> Poll<Vec<LocalFuture>> {
        {
            let mut waker = self.waker.borrow_mut();
            if !waker.as_ref().is_some_and(|w| w.will_wake(cx.waker())) {
                *waker = Some(cx.waker().clone());
            }
        }
        let queued = std::mem::take(&mut *self.queue.borrow_mut());
        if queued.is_empty() {
            Poll::Pending
        } else {
            Poll::Ready(queued)
        }
    }

    /// Reject later spawns and return queued futures for dropping outside the borrow.
    pub(crate) fn close(&self) -> Vec<LocalFuture> {
        self.closed.set(true);
        self.waker.borrow_mut().take();
        std::mem::take(&mut *self.queue.borrow_mut())
    }
}

/// Marks a task finished when its future completes, unwinds, or is dropped.
struct Finish(Arc<AtomicBool>);
impl Finish {
    /// Mark the task finished before its outcome becomes observable.
    fn set(&self) {
        self.0.store(true, Ordering::Release);
    }
}
impl Drop for Finish {
    fn drop(&mut self) {
        self.set();
    }
}

/// Spawn a future on the handle's shard; the caller must be on that shard's thread.
pub(crate) fn spawn<F>(
    handle: &ShardEventHandle,
    future: F,
) -> Result<LocalTask<F::Output>, InvokeError>
where
    F: Future + 'static,
    F::Output: 'static,
{
    let Some(spawner) = crate::engine::local_spawner(handle.shard_id) else {
        return Err(if handle.is_closed() {
            InvokeError::Closed
        } else {
            InvokeError::WrongShard
        });
    };
    if handle.is_closed() {
        return Err(InvokeError::Closed);
    }
    let (future, abort) = futures::future::abortable(AssertUnwindSafe(future).catch_unwind());
    let finished = Arc::new(AtomicBool::new(false));
    let finish = Finish(finished.clone());
    let (tx, result) = oneshot::channel();
    spawner.push(Box::pin(async move {
        let outcome = future.await;
        finish.set();
        match outcome {
            Ok(Ok(value)) => drop(tx.send(Ok(value))),
            Ok(Err(panic)) => {
                let _ = tx.send(Err(InvokeError::Panicked));
                // Let the driver report it like any other callback panic.
                std::panic::resume_unwind(panic);
            }
            Err(Aborted) => drop(tx.send(Err(InvokeError::Canceled))),
        }
    }))?;
    Ok(LocalTask {
        handle: TaskHandle { abort, finished },
        result,
        detached: false,
    })
}

/// Owning guard for a task running on a shard's thread.
/// Dropping it aborts the task, like dropping a [`crate::ScopedConnection`]
/// disconnects. Use [`Self::detach`] to let the task run on its own.
///
/// Awaiting the guard yields the task's output, or
/// [`InvokeError::Canceled`] if it was aborted (including by shard shutdown)
/// and [`InvokeError::Panicked`] if it panicked. Only another future on the
/// same shard may await it, since the shard's driver is what makes progress.
///
/// Spawn with [`crate::ShardBinding::spawn_local`] or [`crate::ShardRc::spawn_local`];
/// [`crate::ShardRc::spawn_owned`] returns a [`TaskHandle`] directly.
#[must_use = "dropping a LocalTask aborts it; call detach() to let it run"]
pub struct LocalTask<T = ()> {
    /// Control handle shared with detached copies.
    handle: TaskHandle,
    /// The task's outcome, sent once its future completes, panics, or is aborted.
    result: oneshot::Receiver<Result<T, InvokeError>>,
    /// Set by `detach` so dropping the guard leaves the task running.
    detached: bool,
}

impl<T> std::fmt::Debug for LocalTask<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalTask")
            .field("handle", &self.handle)
            .field("detached", &self.detached)
            .finish_non_exhaustive()
    }
}

impl<T> Future for LocalTask<T> {
    type Output = Result<T, InvokeError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // A dropped sender means the shard dropped the task unfinished.
        self.result
            .poll_unpin(cx)
            .map(|result| result.unwrap_or(Err(InvokeError::Canceled)))
    }
}

impl<T> LocalTask<T> {
    /// Stop the task at its next suspension point.
    pub fn abort(&self) {
        self.handle.abort();
    }

    /// Whether the task completed, panicked, or was dropped after an abort.
    pub fn is_finished(&self) -> bool {
        self.handle.is_finished()
    }

    /// Stop owning the task. It runs until it completes, is aborted through the
    /// returned handle, or its shard shuts down. Its output is discarded.
    pub fn detach(mut self) -> TaskHandle {
        self.detached = true;
        self.handle.clone()
    }
}

impl<T> Drop for LocalTask<T> {
    fn drop(&mut self) {
        if !self.detached {
            self.handle.abort();
        }
    }
}

/// Non-owning, cloneable control handle for a local task. It can be used from
/// any thread; dropping it does not affect the task.
#[derive(Clone, Debug)]
pub struct TaskHandle {
    /// Stops the task at its next suspension point.
    abort: AbortHandle,
    /// Set once the task's future has been dropped, for any reason.
    finished: Arc<AtomicBool>,
}

impl TaskHandle {
    /// Stop the task at its next suspension point. The future is dropped on its
    /// shard's thread; a running synchronous section is never interrupted.
    pub fn abort(&self) {
        self.abort.abort();
    }

    /// Whether the task completed, panicked, or was dropped after an abort.
    /// An aborted task counts as finished once its shard has dropped it.
    pub fn is_finished(&self) -> bool {
        self.finished.load(Ordering::Acquire)
    }
}
