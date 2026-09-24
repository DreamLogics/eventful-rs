pub struct TaskCancellationToken {
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
    pub fn new() -> Self {
        Self {
            cancelled: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    pub fn cancel(&self) {
        self.cancelled
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl Default for TaskCancellationToken {
    fn default() -> Self {
        Self::new()
    }
}

pub fn run_task<F, C, R>(task: F, callback: C, ct: TaskCancellationToken)
where
    F: FnOnce(TaskCancellationToken) -> Option<R> + Send + 'static,
    C: FnOnce(R) + Send + 'static,
    R: Send + 'static,
{
    std::thread::spawn(move || {
        let result = task(ct.clone());
        if let Some(result) = result
            && !ct.is_cancelled()
        {
            callback(result);
        }
    });
}
