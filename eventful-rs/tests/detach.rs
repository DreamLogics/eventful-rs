//! Detached values live until shard shutdown and are destroyed newest first.
use eventful_rs::*;
use futures::executor::block_on;
use std::{
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

/// Destruction log entry: value name and destroying thread.
type Drops = Arc<Mutex<Vec<(&'static str, thread::ThreadId)>>>;

declare_shard!(Main, runtime = main);

#[eventful(shard = Main)]
struct Window;

#[test]
fn detached_values_outlive_their_last_reference() {
    Main::shard().run_main(|| async {
        let window = Window::bind_local(Window {
            events: Default::default(),
        })
        .unwrap();
        let weak = ShardRc::detach(window);
        // Give ordinary collection a chance to run; the shard still owns the value.
        futures_timer::Delay::new(Duration::from_millis(20)).await;
        assert!(weak.try_local().unwrap().is_some());
        assert_eq!(
            weak.try_deferred_upgrade_in_shard(async |_| ()).await,
            Ok(())
        );
    });
}

declare_shard!(Worker, runtime = std);

/// A value recording its destruction; `client` models a dependency on an
/// earlier detached value.
#[eventful(shard = Worker)]
struct Service {
    name: &'static str,
    drops: Drops,
    client: Option<ShardRc<Service>>,
}
impl Drop for Service {
    fn drop(&mut self) {
        // The dependency must still be alive while this value is destroyed.
        if let Some(client) = &self.client {
            assert!(
                !client
                    .drops
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|(n, _)| *n == client.name)
            );
        }
        self.drops
            .lock()
            .unwrap()
            .push((self.name, thread::current().id()));
    }
}

#[test]
fn shutdown_destroys_detached_values_newest_first_on_the_owner_thread() {
    let drops = Drops::default();
    let log = drops.clone();
    let (owner, weak) = block_on(Worker::bind_async(move |bind| {
        let client = bind(Service {
            name: "client",
            drops: log.clone(),
            client: None,
            events: Default::default(),
        });
        let rooms = bind(Service {
            name: "rooms",
            drops: log.clone(),
            client: Some(client.clone()),
            events: Default::default(),
        });
        let chat = bind(Service {
            name: "chat",
            drops: log,
            client: Some(client.clone()),
            events: Default::default(),
        });
        let weak = ShardRc::detach(client);
        ShardRc::detach(rooms);
        ShardRc::detach(chat);
        (thread::current().id(), weak)
    }))
    .unwrap();
    assert!(drops.lock().unwrap().is_empty());
    // The detached values are still reachable through weak handles.
    assert_eq!(
        block_on(weak.try_deferred_upgrade_in_shard(async |s| s.name)),
        Ok("client")
    );
    Worker::shard().join().unwrap();
    assert_eq!(
        *drops.lock().unwrap(),
        [("chat", owner), ("rooms", owner), ("client", owner)]
    );
}

declare_shard!(Remote, runtime = std);

#[eventful(shard = Remote)]
struct Session {
    drops: Drops,
}
impl Drop for Session {
    fn drop(&mut self) {
        self.drops
            .lock()
            .unwrap()
            .push(("session", thread::current().id()));
    }
}

#[test]
fn handles_detach_from_other_threads() {
    let drops = Drops::default();
    let log = drops.clone();
    let session = block_on(Session::spawn(move || Session {
        drops: log,
        events: Default::default(),
    }))
    .unwrap();
    let weak = session.detach();
    // Queued detachment is admitted before this call, so the value survives.
    assert_eq!(
        block_on(weak.try_deferred_upgrade_in_shard(async |_| ())),
        Ok(())
    );
    assert!(drops.lock().unwrap().is_empty());
    let owner = block_on(
        Remote::handle().try_deferred_invoke(weak.clone(), async |_| thread::current().id()),
    )
    .unwrap();
    Remote::shard().join().unwrap();
    assert_eq!(*drops.lock().unwrap(), [("session", owner)]);
}

declare_shard!(Stopped, runtime = std);

#[eventful(shard = Stopped)]
struct Late {
    drops: Drops,
}
impl Drop for Late {
    fn drop(&mut self) {
        self.drops
            .lock()
            .unwrap()
            .push(("late", thread::current().id()));
    }
}

#[test]
fn detaching_after_shutdown_only_drops_the_handle() {
    let drops = Drops::default();
    let log = drops.clone();
    let late = block_on(Late::spawn(move || Late {
        drops: log,
        events: Default::default(),
    }))
    .unwrap();
    Stopped::shard().handle().request_shutdown();
    let weak = late.detach();
    Stopped::shard().join().unwrap();
    assert_eq!(drops.lock().unwrap().len(), 1);
    assert_eq!(
        block_on(weak.try_deferred_upgrade_in_shard(async |_| ())),
        Err(InvokeError::Closed)
    );
}
