//! Synchronous local access from strong and weak handles on their own shard.
use eventful_rs::*;
use futures::executor::block_on;
use std::{
    thread,
    time::{Duration, Instant},
};

#[events]
trait Updates {
    fn changed(&self, value: u32);
}

/// Wait until a condition holds, failing after two seconds.
async fn eventually(mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(2);
    while !condition() {
        assert!(Instant::now() < deadline, "condition not reached in time");
        futures_timer::Delay::new(Duration::from_millis(2)).await;
    }
}

declare_shard!(Ui, runtime = main);

#[eventful(Updates, shard = Ui)]
struct Client;

#[test]
fn handles_recover_the_same_local_value() {
    Ui::shard().run_main(|| async {
        let client = Client::bind_local(Client {
            events: Default::default(),
        })
        .unwrap();
        let handle = client.to_handle();
        let weak = handle.downgrade();

        let strong = handle.try_local().unwrap();
        let from_weak = weak.try_local().unwrap().unwrap();
        let from_ref = ShardRc::try_from_ref(&*client).unwrap();
        assert!(std::ptr::eq(&*strong, &*client));
        assert!(std::ptr::eq(&*from_weak, &*from_ref));
        assert!(std::sync::Arc::ptr_eq(strong.events(), client.events()));

        // Off the shard thread, neither handle can produce a local reference.
        let (strong_off, weak_off) = thread::spawn({
            let handle = handle.clone();
            let weak = weak.clone();
            move || (handle.try_local().err(), weak.try_local().err())
        })
        .join()
        .unwrap();
        assert_eq!(strong_off, Some(InvokeError::WrongShard));
        assert_eq!(weak_off, Some(InvokeError::WrongShard));

        // Once every strong reference is released, the value is collected.
        drop((client, handle, strong, from_weak, from_ref));
        eventually(|| weak.try_local().unwrap().is_none()).await;
    });
}

declare_shard!(Worker, runtime = std);
declare_shard!(Other, runtime = std);

#[eventful(shard = Worker)]
struct Session;

#[test]
fn another_shards_thread_is_rejected() {
    let session = block_on(Session::spawn(|| Session {
        events: Default::default(),
    }))
    .unwrap();
    let weak = session.downgrade();
    let (tx, rx) = std::sync::mpsc::channel();
    let probe = session.clone();
    Other::handle()
        .try_invoke(move || {
            let _ = tx.send((probe.try_local().err(), weak.try_local().err()));
        })
        .unwrap();
    assert_eq!(
        rx.recv().unwrap(),
        (Some(InvokeError::WrongShard), Some(InvokeError::WrongShard))
    );
    // On the owning shard both succeed.
    let (tx, rx) = std::sync::mpsc::channel();
    let probe = session.clone();
    Worker::handle()
        .try_invoke(move || {
            let local = probe.try_local().unwrap();
            let weak = probe.downgrade().try_local().unwrap().unwrap();
            let _ = tx.send(std::ptr::eq(&*local, &*weak));
        })
        .unwrap();
    assert!(rx.recv().unwrap());
    drop(session);
    Worker::shard().join().unwrap();
    Other::shard().join().unwrap();
}

declare_shard!(Owned, runtime = main);

#[eventful(Updates, shard = Owned)]
struct Source;

#[eventful(shard = Owned)]
struct Sink;
impl Updates for Sink {
    fn changed(&self, _value: u32) {}
}

#[test]
fn connections_owned_through_a_recovered_reference_drop_with_the_value() {
    Owned::shard().run_main(|| async {
        let source = Source::bind_local(Source {
            events: Default::default(),
        })
        .unwrap();
        let sink = Sink::bind_local(Sink {
            events: Default::default(),
        })
        .unwrap();
        let events = source.events().clone();
        let handle = source.to_handle();
        drop(source);
        let recovered = handle.try_local().unwrap();
        let _ = ShardRc::connect(&recovered, &sink);
        assert_eq!(events.changed().connection_count(), 1);
        drop((recovered, handle));
        eventually(|| events.changed().connection_count() == 0).await;
    });
}
