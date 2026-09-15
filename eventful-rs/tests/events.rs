use eventful_rs::Event;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
#[test]
fn emission_uses_snapshot_and_does_not_hold_connection_lock() {
    let event = Arc::new(Event::<usize>::default());
    let sum = Arc::new(AtomicUsize::new(0));
    let weak = Arc::downgrade(&event);
    event.add_connection(move |_| {
        if let Some(event) = weak.upgrade() {
            event.add_connection(|_| {});
        }
    });
    let result = sum.clone();
    event.add_connection(move |n| {
        result.fetch_add(n, Ordering::SeqCst);
    });
    event.emit(3);
    assert_eq!(event.connection_count(), 3);
    assert_eq!(sum.load(Ordering::SeqCst), 3);
}

#[test]
fn tracked_emission_is_immediate_owned_and_uses_a_snapshot() {
    let event = Arc::new(Event::<usize>::default());
    let sum = Arc::new(AtomicUsize::new(0));
    let weak = Arc::downgrade(&event);
    let result = sum.clone();
    event.add_connection(move |n| {
        result.fetch_add(n, Ordering::SeqCst);
        weak.upgrade()
            .unwrap()
            .add_connection(|_| panic!("next emission only"));
    });
    let delivery = event.emit_tracked(3);
    assert_eq!(sum.load(Ordering::SeqCst), 3);
    drop(event);
    assert_eq!(futures::executor::block_on(delivery), Ok(()));
    assert_eq!(
        futures::executor::block_on(Event::<()>::default().emit_tracked(())),
        Ok(())
    );
}

#[test]
fn tracked_emission_waits_for_all_handlers_even_after_a_failure() {
    use eventful_rs::DeliveryError;
    use futures::{FutureExt, channel::oneshot, executor::block_on};
    let event = Event::<()>::default();
    event.add_connection(|_| panic!("expected callback panic"));
    let (tx, rx) = oneshot::channel();
    let receiver = std::sync::Mutex::new(Some(rx));
    let completed = Arc::new(AtomicUsize::new(0));
    let result = completed.clone();
    event.add_tracked_connection(
        |_| {},
        move |_| {
            let rx = receiver.lock().unwrap().take().unwrap();
            let result = result.clone();
            async move {
                rx.await.unwrap();
                result.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        },
    );
    let delivery = event.emit_tracked(());
    futures::pin_mut!(delivery);
    assert!(delivery.as_mut().now_or_never().is_none());
    tx.send(()).unwrap();
    assert_eq!(block_on(delivery), Err(DeliveryError::Panicked));
    assert_eq!(completed.load(Ordering::SeqCst), 1);
}

#[test]
fn tracked_emission_polls_concurrently_and_reports_future_panics() {
    let event = Event::<()>::default();
    let (tx, rx) = futures::channel::oneshot::channel();
    let receiver = std::sync::Mutex::new(Some(rx));
    event.add_tracked_connection(
        |_| {},
        move |_| {
            let rx = receiver.lock().unwrap().take().unwrap();
            async move {
                rx.await.unwrap();
                Ok(())
            }
        },
    );
    let sender = std::sync::Mutex::new(Some(tx));
    event.add_tracked_connection(
        |_| {},
        move |_| {
            let tx = sender.lock().unwrap().take().unwrap();
            async move {
                tx.send(()).unwrap();
                panic!("expected future panic")
            }
        },
    );
    assert_eq!(
        futures::executor::block_on(event.emit_tracked(())),
        Err(eventful_rs::DeliveryError::Panicked)
    );
}
