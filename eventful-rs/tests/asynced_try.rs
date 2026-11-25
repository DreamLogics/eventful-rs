//! Fallible `try_` handle methods generated for `#[asynced]` methods.
use eventful_rs::*;
use futures::executor::block_on;
use std::{
    cell::Cell,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::mpsc,
    time::Duration,
};

#[eventful(shard = DynamicShard)]
struct Counter {
    value: Cell<usize>,
    dropped: Option<mpsc::Sender<()>>,
}
impl Drop for Counter {
    fn drop(&mut self) {
        if let Some(tx) = &self.dropped {
            let _ = tx.send(());
        }
    }
}
#[asynchronize]
impl Counter {
    #[asynced]
    fn count(&self) -> usize {
        self.value.get()
    }
    #[asynced]
    async fn add(&self, amount: usize) -> usize {
        self.value.set(self.value.get() + amount);
        self.value.get()
    }
    #[asynced]
    fn reset(&self) {
        self.value.set(0);
    }
    #[asynced]
    fn fail(&self) -> usize {
        panic!("expected panic")
    }
    /// Opting out leaves `try_read` free for another method.
    #[asynced(no_try)]
    fn read(&self) -> usize {
        self.value.get()
    }
    #[asynced]
    fn try_read(&self) -> Option<usize> {
        Some(self.value.get())
    }
    #[deprecated = "use count"]
    #[asynced]
    fn old_count(&self) -> usize {
        self.value.get()
    }
    // The fallible wrapper is compiled out together with the original.
    #[cfg(any())]
    #[asynced]
    fn missing(&self) -> usize {
        0
    }
}

fn counter(shard: &std_rt::Shard, dropped: Option<mpsc::Sender<()>>) -> ShardRcHandle<Counter> {
    shard.bind(|bind| {
        bind(Counter {
            value: Cell::new(0),
            dropped,
            events: Default::default(),
        })
        .to_handle()
    })
}

fn panics<F: Future>(future: F) -> bool {
    catch_unwind(AssertUnwindSafe(|| block_on(future))).is_err()
}

#[test]
fn try_methods_return_results_on_strong_and_weak_handles() {
    let shard = std_rt::Shard::new("asynced-try-ok");
    let strong = counter(&shard, None);
    let weak = strong.downgrade();
    assert_eq!(block_on(strong.try_add(2)), Ok(2));
    assert_eq!(block_on(weak.try_add(3)), Ok(5));
    assert_eq!(block_on(strong.try_count()), Ok(5));
    assert_eq!(block_on(weak.try_count()), Ok(5));
    assert_eq!(block_on(strong.read()), 5);
    assert_eq!(block_on(weak.try_read()), Some(5));
    #[allow(deprecated)]
    let old = block_on(strong.try_old_count());
    assert_eq!(old, Ok(5));
    assert_eq!(block_on(weak.try_reset()), Ok(()));
    assert_eq!(block_on(strong.count()), 0);
    shard.join().unwrap();
}

#[test]
fn try_methods_report_a_closed_shard() {
    let shard = std_rt::Shard::new("asynced-try-closed");
    let strong = counter(&shard, None);
    let weak = strong.downgrade();
    shard.join().unwrap();
    assert_eq!(block_on(strong.try_count()), Err(InvokeError::Closed));
    assert_eq!(block_on(weak.try_count()), Err(InvokeError::Closed));
    assert_eq!(block_on(strong.try_reset()), Err(InvokeError::Closed));
    assert!(panics(strong.count()));
    assert!(panics(weak.count()));
}

#[test]
fn try_methods_report_an_expired_weak_target() {
    let shard = std_rt::Shard::new("asynced-try-expired");
    let (tx, rx) = mpsc::channel();
    let strong = counter(&shard, Some(tx));
    let weak = strong.downgrade();
    drop(strong);
    rx.recv_timeout(Duration::from_secs(3)).unwrap();
    assert_eq!(block_on(weak.try_count()), Err(InvokeError::ValueMissing));
    assert_eq!(block_on(weak.try_add(1)), Err(InvokeError::ValueMissing));
    assert!(panics(weak.count()));
    shard.join().unwrap();
}

#[test]
fn try_methods_report_a_panicking_method() {
    let shard = std_rt::Shard::new("asynced-try-panic");
    let strong = counter(&shard, None);
    let weak = strong.downgrade();
    assert_eq!(block_on(strong.try_fail()), Err(InvokeError::Panicked));
    assert_eq!(block_on(weak.try_fail()), Err(InvokeError::Panicked));
    assert!(panics(strong.fail()));
    // The shard keeps running after the isolated panics.
    assert_eq!(block_on(strong.try_add(1)), Ok(1));
    shard.join().unwrap();
}

#[test]
fn shard_handle_trait_dispatches_fallibly_for_both_handles() {
    fn read<H: ShardHandle<Counter>>(handle: &H) -> Result<usize, InvokeError> {
        block_on(ShardHandle::try_deferred_upgrade_in_shard(
            handle,
            async |counter: &Counter| counter.value.get(),
        ))
    }
    let shard = std_rt::Shard::new("asynced-try-trait");
    let strong = counter(&shard, None);
    let weak = strong.downgrade();
    assert_eq!(read(&strong), Ok(0));
    assert_eq!(read(&weak), Ok(0));
    shard.join().unwrap();
    assert_eq!(read(&strong), Err(InvokeError::Closed));
    assert_eq!(read(&weak), Err(InvokeError::Closed));
}
