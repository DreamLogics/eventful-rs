use eventful_rs::*;
use futures::{channel::oneshot, executor::block_on};
use std::{cell::Cell, rc::Rc, sync::Arc, time::Duration};

struct Counter {
    value: Cell<usize>,
    events: Arc<()>,
}
struct Label {
    text: Rc<String>,
    events: Arc<()>,
}
macro_rules! eventful_type {
    ($ty:ty) => {
        impl Eventful for $ty {
            type EventSetType = ();
            type Shard = eventful_rs::DynamicShard;
        }
        impl HasEvents<()> for $ty {
            fn events(&self) -> &Arc<()> {
                &self.events
            }
        }
    };
}
eventful_type!(Counter);
eventful_type!(Label);

fn counter(shard: &impl EventLoop<HandleType = ShardEventHandle>) -> ShardRcHandle<Counter> {
    shard.bind(|bind| {
        bind(Counter {
            value: Cell::new(0),
            events: Arc::new(()),
        })
        .to_handle()
    })
}

fn exercise(shard: &impl EventLoop<HandleType = ShardEventHandle>) {
    let a = counter(shard);
    let b = shard.bind(|bind| {
        bind(Label {
            text: Rc::new("hello".into()),
            events: Arc::new(()),
        })
        .to_handle()
    });
    let owner = block_on(
        shard
            .handle()
            .try_deferred_invoke(a.clone(), async |_| std::thread::current().id()),
    )
    .unwrap();
    let pair = a.join(&b).unwrap();
    let joined = pair.join(&a).unwrap();
    // Successful joins leave both original handles and the source group usable.
    assert!(a.join(&b).is_some());
    assert!(pair.join(&a).is_some());
    drop(pair);
    drop(a);
    drop(b);
    let (tx, rx) = std::sync::mpsc::channel();
    joined.upgrade_in_shard(move |(a, b, same_a)| {
        assert!(std::ptr::eq(a, same_a));
        a.value.set(b.text.len());
        tx.send((same_a.value.get(), std::thread::current().id()))
            .unwrap();
    });
    assert_eq!(rx.recv_timeout(Duration::from_secs(5)).unwrap(), (5, owner));

    let (go, ready) = oneshot::channel();
    let (done, finished) = oneshot::channel();
    joined.upgrade_in_shard_async(async move |(a, b, same_a)| {
        // These references and a non-Send local survive suspension on the shard.
        let local = Rc::clone(&b.text);
        ready.await.unwrap();
        a.value.set(a.value.get() + local.len());
        done.send((same_a.value.get(), std::thread::current().id()))
            .unwrap();
    });
    // A later job must be able to run while the async callback is suspended.
    joined.upgrade_in_shard(move |_| {
        go.send(()).unwrap();
    });
    assert_eq!(block_on(finished).unwrap(), (10, owner));

    assert_eq!(
        block_on(joined.deferred_upgrade_in_shard(async |(a, b, same_a)| {
            futures::future::ready(()).await;
            (a.value.get(), b.text.to_string(), same_a.value.get())
        })),
        (10, "hello".into(), 10)
    );
    assert_eq!(
        block_on(joined.try_deferred_upgrade_in_shard(async |_| -> () {
            panic!("expected joined callback panic");
        })),
        Err(InvokeError::Panicked)
    );

    let (go, ready) = oneshot::channel();
    let completion = joined.try_deferred_upgrade_in_shard(async move |(a, b, _)| {
        ready.await.unwrap();
        a.value.get() + b.text.len()
    });
    drop(joined);
    go.send(()).unwrap();
    assert_eq!(block_on(completion), Ok(15));
}

#[test]
fn standard_joined_callbacks() {
    let shard = shard::Shard::new("joined-standard");
    exercise(&shard);
    shard.join().unwrap();
}

#[cfg(feature = "tokio")]
#[test]
fn tokio_joined_callbacks() {
    let shard = tokio::TokioShard::new("joined-tokio");
    exercise(&shard);
    shard.join().unwrap();
}

#[test]
fn joins_reject_different_shards_even_with_colliding_ids() {
    let first = shard::Shard::new("joined-first");
    let second = shard::Shard::new("joined-second");
    let a = counter(&first);
    let b = counter(&second);
    assert!(a.join(&b).is_none());
    let pair = a.join(&a).unwrap();
    assert!(pair.join(&b).is_none());
    // Failed joins also preserve the inputs, including an existing group.
    assert!(pair.join(&a).is_some());
    assert!(b.join(&b).is_some());
    assert_eq!(
        block_on(pair.deferred_upgrade_in_shard(async |(a, b)| { a.value.get() + b.value.get() })),
        0
    );
    first.join().unwrap();
    second.join().unwrap();
}

#[test]
fn eight_handles_form_a_flat_tuple_and_closed_shards_report_failure() {
    let shard = shard::Shard::new("joined-eight");
    let a = counter(&shard);
    let group = a
        .join(&a)
        .unwrap()
        .join(&a)
        .unwrap()
        .join(&a)
        .unwrap()
        .join(&a)
        .unwrap()
        .join(&a)
        .unwrap()
        .join(&a)
        .unwrap()
        .join(&a)
        .unwrap();
    assert_eq!(
        block_on(
            group.deferred_upgrade_in_shard(async |(a, b, c, d, e, f, g, h)| {
                for counter in [a, b, c, d, e, f, g, h] {
                    counter.value.set(counter.value.get() + 1);
                }
                a.value.get()
            })
        ),
        8
    );
    shard.join().unwrap();
    // Join validates identity even after shutdown; delivery reports Closed.
    assert!(a.join(&a).is_some());
    group.upgrade_in_shard(|_| panic!("closed callback ran"));
    group.upgrade_in_shard_async(async |_| panic!("closed async callback ran"));
    assert_eq!(
        block_on(group.try_deferred_upgrade_in_shard(async |_| 1)),
        Err(InvokeError::Closed)
    );
}
