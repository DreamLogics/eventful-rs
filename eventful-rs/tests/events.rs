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
