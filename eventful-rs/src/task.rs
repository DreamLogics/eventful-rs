//! Detached blocking work with cooperative cancellation.
//! Callbacks run on the worker thread; use shard handles to update eventful values.
/// Shared cooperative cancellation flag. Cancellation does not interrupt a running task.
pub struct TaskCancellationToken {
    /// Atomic flag shared by workers and cancellation requesters.
    cancelled: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl Clone for TaskCancellationToken {
    fn clone(&self) -> Self {
        Self {
            cancelled: self.cancelled.clone(),
        }
    }
}

impl TaskCancellationToken {
    /// Create an uncanceled token.
    pub fn new() -> Self {
        Self {
            cancelled: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// Set the cancellation flag for all clones.
    pub fn cancel(&self) {
        self.cancelled
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Read whether cancellation has been requested.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl Default for TaskCancellationToken {
    fn default() -> Self {
        Self::new()
    }
}

/// Spawn one detached OS thread and deliver a successful result on that thread.
/// The task must poll its token to stop early. `None` suppresses the callback.
/// Cancellation is checked before calling back; racing cancellation cannot revoke a callback.
/// Panics terminate this detached thread and are not returned to the caller.
///
/// ```
/// use eventful_rs::task::{run_task, TaskCancellationToken};
/// let (tx, rx) = std::sync::mpsc::channel();
/// run_task(|token| {
///     if token.is_cancelled() { return None; }
///     Some("product,quantity\napple,12".lines().skip(1).count())
/// }, move |rows| { tx.send(rows).unwrap(); }, TaskCancellationToken::new());
/// assert_eq!(rx.recv().unwrap(), 1);
/// ```
pub fn run_task<F, C, R>(task: F, callback: C, ct: TaskCancellationToken)
where
    F: FnOnce(TaskCancellationToken) -> Option<R> + Send + 'static,
    C: FnOnce(R) + Send + 'static,
    R: Send + 'static,
{
    std::thread::spawn(move || {
        let result = task(ct.clone());
        if !ct.is_cancelled() {
            if let Some(result) = result {
                callback(result);
            }
        }
    });
}
