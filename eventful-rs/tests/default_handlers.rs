//! Event methods with default bodies: listeners override only what they need.
use eventful_rs::*;
use futures::executor::block_on;
use std::sync::Mutex;

declare_shard!(Worker, runtime = std);

#[events]
trait ChatEvents {
    fn on_messages(&self, token: String, count: u32) {}
    fn on_stopped(&self, token: String) {}
}

#[eventful(ChatEvents, shard = Worker)]
struct Feed;

#[eventful(shard = Worker)]
struct View {
    seen: Mutex<Vec<String>>,
}
impl ChatEvents for View {
    fn on_messages(&self, token: String, count: u32) {
        self.seen.lock().unwrap().push(format!("{token}:{count}"));
    }
}

struct Primary;
struct Secondary;
impl ChatEvents<Primary> for View {
    fn on_messages(&self, token: String, _count: u32) {
        self.seen
            .lock()
            .unwrap()
            .push(format!("primary messages {token}"));
    }
}
impl ChatEvents<Secondary> for View {
    fn on_stopped(&self, token: String) {
        self.seen
            .lock()
            .unwrap()
            .push(format!("secondary stopped {token}"));
    }
}

#[test]
fn defaults_run_for_handlers_the_listener_did_not_override() {
    block_on(async {
        let feed = Feed::spawn(|| Feed {
            events: Default::default(),
        })
        .await
        .unwrap();
        let view = View::spawn(|| View {
            seen: Mutex::default(),
            events: Default::default(),
        })
        .await
        .unwrap();
        let _all = feed.connect(&view).scoped();
        let _primary = feed.role::<Primary>().connect(&view).scoped();
        let _secondary = feed.role::<Secondary>().connect(&view).scoped();

        feed.deferred_upgrade_in_shard(async |feed| {
            feed.events
                .on_messages()
                .tracked()
                .emit("a".into(), 2)
                .await?;
            // Every connection delivers; the defaults complete without effect.
            feed.events.on_stopped().tracked().emit("b".into()).await
        })
        .await
        .unwrap();

        let mut seen = view
            .deferred_upgrade_in_shard(async |view| view.seen.lock().unwrap().clone())
            .await;
        seen.sort();
        assert_eq!(seen, ["a:2", "primary messages a", "secondary stopped b"]);
    });
    Worker::shard().join().unwrap();
}
