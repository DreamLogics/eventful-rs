//! Event declarations that are never enabled together may share a signal name.
use eventful_rs::*;
use std::sync::Mutex;

declare_shard!(Worker, runtime = std);

#[events]
trait PlatformEvents {
    #[cfg(unix)]
    fn changed(&self, path: String);
    #[cfg(not(unix))]
    fn changed(&self, code: u32);
    #[cfg_attr(unix, cfg(any()))]
    fn reloaded(&self);
    #[cfg_attr(not(unix), cfg(any()))]
    fn reloaded(&self, path: String);
}

#[eventful(PlatformEvents, shard = Worker)]
struct Watcher;

#[eventful(shard = Worker)]
struct Log {
    seen: Mutex<Vec<String>>,
}

impl PlatformEvents for Log {
    #[cfg(unix)]
    fn changed(&self, path: String) {
        self.seen.lock().unwrap().push(path);
    }
    #[cfg(not(unix))]
    fn changed(&self, code: u32) {
        self.seen.lock().unwrap().push(code.to_string());
    }
    #[cfg_attr(unix, cfg(any()))]
    fn reloaded(&self) {
        self.seen.lock().unwrap().push("reloaded".into());
    }
    #[cfg_attr(not(unix), cfg(any()))]
    fn reloaded(&self, path: String) {
        self.seen.lock().unwrap().push(format!("reloaded {path}"));
    }
}

#[test]
fn mutually_exclusive_declarations_share_a_name() {
    futures::executor::block_on(async {
        let watcher = Watcher::spawn(|| Watcher {
            events: Default::default(),
        })
        .await
        .unwrap();
        let log = Log::spawn(|| Log {
            seen: Mutex::new(Vec::new()),
            events: Default::default(),
        })
        .await
        .unwrap();
        let _connections = watcher.connect(&log).scoped();

        watcher
            .deferred_upgrade_in_shard(async |watcher| {
                #[cfg(unix)]
                {
                    watcher.events.changed().tracked().emit("a".into()).await?;
                    watcher.events.reloaded().tracked().emit("b".into()).await
                }
                #[cfg(not(unix))]
                {
                    watcher.events.changed().tracked().emit(1).await?;
                    watcher.events.reloaded().tracked().emit().await
                }
            })
            .await
            .unwrap();

        let seen = log
            .deferred_upgrade_in_shard(async |log| log.seen.lock().unwrap().clone())
            .await;
        #[cfg(unix)]
        assert_eq!(seen, ["a", "reloaded b"]);
        #[cfg(not(unix))]
        assert_eq!(seen, ["1", "reloaded"]);
    });
    Worker::shard().join().unwrap();
}
