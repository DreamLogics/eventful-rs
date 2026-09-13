use std::panic::{AssertUnwindSafe, catch_unwind};
use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    task::{Context, Poll, Wake, Waker},
};
use std::{
    sync::{Arc, Mutex, mpsc},
    thread,
};

use crate::{FutureId, LocalFuture, Task};

pub struct FutureWake<T> {
    id: FutureId,
    sender: mpsc::Sender<Task<T>>,
}

impl<T> Wake for FutureWake<T>
where
    T: 'static,
{
    fn wake(self: Arc<Self>) {
        let _ = self.sender.send(Task::Wake(self.id));
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let _ = self.sender.send(Task::Wake(self.id));
    }
}

pub fn poll_future<'a, T>(
    id: FutureId,
    futures: &mut HashMap<FutureId, LocalFuture<'a>>,
    sender: &mpsc::Sender<Task<T>>,
) where
    T: 'static,
{
    let Some(future) = futures.get_mut(&id) else {
        // A stale or duplicate wakeup.
        return;
    };

    let waker = Waker::from(Arc::new(FutureWake {
        id,
        sender: sender.clone(),
    }));

    let mut cx = Context::from_waker(&waker);

    let result = catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(&mut cx)));

    match result {
        Ok(Poll::Pending) => {}

        Ok(Poll::Ready(())) => {
            futures.remove(&id);
        }

        Err(_) => {
            // Future panicked. Remove it rather than poisoning
            // the entire event-loop thread.
            futures.remove(&id);
        }
    }
}
