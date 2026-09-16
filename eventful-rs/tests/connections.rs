use eventful_rs::{Connection, Event};
use std::sync::{Arc, Mutex};

fn record(event: &Event<usize>, values: &Arc<Mutex<Vec<usize>>>) -> Connection<usize> {
    let values = values.clone();
    event.add_connection(move |value| values.lock().unwrap().push(value))
}

#[test]
fn dropping_connection_and_its_clone_keeps_listener_connected() {
    let event = Event::default();
    let values = Arc::new(Mutex::new(Vec::new()));
    let connection = record(&event, &values);
    let clone = connection.clone();

    drop(connection);
    event.emit(1);
    drop(clone);
    event.emit(2);

    assert_eq!(event.connection_count(), 1);
    assert_eq!(*values.lock().unwrap(), vec![1, 2]);
}

#[test]
fn disconnect_removes_only_its_listener_and_stale_clones_are_harmless() {
    let event = Event::default();
    let removed = Arc::new(Mutex::new(Vec::new()));
    let retained = Arc::new(Mutex::new(Vec::new()));
    let added = Arc::new(Mutex::new(Vec::new()));
    let connection = record(&event, &removed);
    let clone = connection.clone();
    record(&event, &retained);
    event.emit(1);

    clone.disconnect();
    assert_eq!(event.connection_count(), 1);
    record(&event, &added);
    connection.disconnect();
    assert_eq!(event.connection_count(), 2);
    event.emit(2);

    assert_eq!(*removed.lock().unwrap(), vec![1]);
    assert_eq!(*retained.lock().unwrap(), vec![1, 2]);
    assert_eq!(*added.lock().unwrap(), vec![2]);
}

#[test]
fn scoped_connection_disconnects_only_its_listener_at_scope_exit() {
    let event = Event::default();
    let scoped_values = Arc::new(Mutex::new(Vec::new()));
    let retained = Arc::new(Mutex::new(Vec::new()));
    record(&event, &retained);

    {
        let _connection = record(&event, &scoped_values).scoped();
        assert_eq!(event.connection_count(), 2);
        event.emit(1);
    }

    assert_eq!(event.connection_count(), 1);
    event.emit(2);
    assert_eq!(*scoped_values.lock().unwrap(), vec![1]);
    assert_eq!(*retained.lock().unwrap(), vec![1, 2]);
}

#[test]
fn dropping_either_scoped_clone_disconnects_immediately() {
    for drop_original in [false, true] {
        let event = Event::default();
        let values = Arc::new(Mutex::new(Vec::new()));
        let original = record(&event, &values).scoped();
        let clone = original.clone();
        event.emit(1);

        let remaining = if drop_original {
            drop(original);
            clone
        } else {
            drop(clone);
            original
        };
        assert_eq!(event.connection_count(), 0);
        event.emit(2);
        assert_eq!(*values.lock().unwrap(), vec![1]);

        let added = Arc::new(Mutex::new(Vec::new()));
        record(&event, &added);
        drop(remaining);
        assert_eq!(event.connection_count(), 1);
        event.emit(3);
        assert_eq!(*added.lock().unwrap(), vec![3]);
    }
}

#[test]
fn scoped_drop_after_explicit_disconnect_is_harmless() {
    let event = Event::default();
    let connection = event.add_connection(|()| panic!("disconnected listener"));
    let scoped = connection.clone().scoped();

    connection.disconnect();
    assert_eq!(event.connection_count(), 0);
    let values = Arc::new(Mutex::new(Vec::new()));
    let result = values.clone();
    event.add_connection(move |()| result.lock().unwrap().push(()));
    drop(scoped);

    assert_eq!(event.connection_count(), 1);
    event.emit(());
    assert_eq!(*values.lock().unwrap(), vec![()]);
}

#[test]
fn handles_can_outlive_event_and_release_listener_captures() {
    for scoped in [false, true] {
        let event = Event::<()>::default();
        let captured = Arc::new(());
        let weak = Arc::downgrade(&captured);
        let connection = event.add_connection(move |()| {
            let _ = &captured;
        });

        if scoped {
            let connection = connection.scoped();
            drop(event);
            assert!(weak.upgrade().is_some());
            drop(connection);
        } else {
            drop(event);
            assert!(weak.upgrade().is_some());
            connection.disconnect();
        }
        assert!(weak.upgrade().is_none());
    }
}

#[test]
fn disconnect_during_emission_preserves_current_snapshot() {
    for tracked in [false, true] {
        let event = Event::default();
        let to_disconnect = Arc::new(Mutex::new(None::<Connection<usize>>));
        let target = to_disconnect.clone();
        event.add_connection(move |_| {
            if let Some(connection) = target.lock().unwrap().take() {
                connection.disconnect();
            }
        });
        let values = Arc::new(Mutex::new(Vec::new()));
        *to_disconnect.lock().unwrap() = Some(record(&event, &values));

        for value in [1, 2] {
            if tracked {
                assert_eq!(
                    futures::executor::block_on(event.emit_tracked(value)),
                    Ok(())
                );
            } else {
                event.emit(value);
            }
            assert_eq!(event.connection_count(), 1);
        }
        assert_eq!(*values.lock().unwrap(), vec![1]);
    }
}

#[test]
fn disconnect_stops_both_dispatch_paths_without_canceling_pending_delivery() {
    use futures::{FutureExt, channel::oneshot, executor::block_on};

    for scoped in [false, true] {
        let event = Event::<()>::default();
        let ordinary_calls = Arc::new(Mutex::new(0));
        let calls = ordinary_calls.clone();
        let (tx, rx) = oneshot::channel();
        let receiver = Mutex::new(Some(rx));
        let connection = event.add_tracked_connection(
            move |()| *calls.lock().unwrap() += 1,
            move |()| {
                let rx = receiver.lock().unwrap().take().unwrap();
                async move {
                    rx.await.unwrap();
                    Ok(())
                }
            },
        );
        event.emit(());
        let delivery = event.emit_tracked(());
        futures::pin_mut!(delivery);
        assert!(delivery.as_mut().now_or_never().is_none());

        if scoped {
            drop(connection.scoped());
        } else {
            connection.disconnect();
        }
        assert_eq!(event.connection_count(), 0);
        event.emit(());
        assert_eq!(*ordinary_calls.lock().unwrap(), 1);
        assert_eq!(block_on(event.emit_tracked(())), Ok(()));

        assert!(delivery.as_mut().now_or_never().is_none());
        tx.send(()).unwrap();
        assert_eq!(block_on(delivery), Ok(()));
    }
}
