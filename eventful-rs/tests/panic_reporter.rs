//! The reporter is process-wide, so this binary holds a single test.
use eventful_rs::*;
use futures::executor::block_on;
use std::sync::{Arc, Mutex};

declare_shard!(Worker, runtime = std);

#[eventful(shard = Worker)]
struct Probe;

#[asynchronize]
impl Probe {
    #[asynced]
    async fn panic_in_task(&self) -> Result<(), InvokeError> {
        Worker::spawn_local(async { panic!("task {}", 7) })?.await
    }
}

#[test]
fn reporter_receives_isolated_panics_and_cannot_stop_the_shard() {
    let reports = Arc::new(Mutex::new(Vec::new()));
    let seen = reports.clone();
    set_panic_reporter(move |panic| {
        seen.lock()
            .unwrap()
            .push((panic.shard_id(), panic.message().map(str::to_owned)));
        if panic.message() == Some("faulty reporter") {
            panic!("reporter panicked");
        }
    });
    let handle = Worker::handle();
    let shard = handle.shard_id();

    // A queued callback; the roundtrip runs after its report.
    handle.invoke(|| panic!("callback"));
    block_on(handle.try_invoke_tracked(|| {})).unwrap();

    // A local task with a formatted message. Its driver reports the panic
    // before the awaiting job resumes.
    let probe = block_on(Probe::spawn(|| Probe {
        events: Default::default(),
    }))
    .unwrap();
    assert_eq!(block_on(probe.panic_in_task()), Err(InvokeError::Panicked));

    // A panicking reporter is discarded and the shard keeps working.
    handle.invoke(|| panic!("faulty reporter"));
    block_on(handle.try_invoke_tracked(|| {})).unwrap();

    assert_eq!(
        *reports.lock().unwrap(),
        [
            (shard, Some("callback".to_owned())),
            (shard, Some("task 7".to_owned())),
            (shard, Some("faulty reporter".to_owned())),
        ]
    );
    Worker::shard().join().unwrap();
}
