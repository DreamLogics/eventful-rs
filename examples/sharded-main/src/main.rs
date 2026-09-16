use eventful_rs::*;

mod producer {
    use std::cell::Cell;

    use eventful_rs::*;

    shard_std!(PRODUCER);

    #[events]
    pub trait ProducerEvents {
        fn on_produce(&self, item: String);
    }

    #[eventful(ProducerEvents)]
    pub struct Producer {
        count: Cell<usize>,
    }

    #[asynchronize]
    impl Producer {
        pub fn new() -> ShardRcHandle<Self> {
            Self {
                events: Default::default(),
                count: Cell::new(0),
            }
            .into()
        }

        // This method produces items and emits events for each produced item.
        // The `#[asynced]` attribute allows this method to be called asynchronously,
        // even though it is not an async function itself. All that is made async is the
        // wrapper that calls this method, so it can be called from an async context without blocking.
        // This version doesn't track the event emissions, meaning there is no guarantee that the events
        // have been processed by the listeners before this method returns.

        #[asynced]
        pub fn produce(&self, count: usize) -> Vec<String> {
            for i in 0..count {
                let item = format!("Item {}", i);
                self.emit_on_produce(item.clone());
            }
            self.count.update(|c| c + count);
            (0..count).map(|i| format!("Item {}", i)).collect()
        }

        // This is the same as the previous method, but it tracks the event emissions.
        // It uses `futures::future::join_all` to wait for all event emissions to complete
        // before returning. This ensures that all events have been processed by the listeners
        // before this method returns.

        #[asynced]
        pub async fn produce_tracked(&self, count: usize) -> Vec<String> {
            let tracking = (0..count).map(|i| {
                let item = format!("Item {}", i);
                self.emit_on_produce_tracked(item.clone())
            });
            futures::future::join_all(tracking).await;

            self.count.update(|c| c + count);
            (0..count).map(|i| format!("Item {}", i)).collect()
        }

        // An alternative to asynced wrappers is to use the `#[action]` attribute,
        // it functions more or less the same as `#[asynced]`, but it doesn't return anything,
        // thus making it a bit more lightweight. Also, the wrapper is just a normal function,
        // so it can be called from any context, not just async contexts.
        // This is useful for methods that don't need to return a value,
        // but still need to be called asynchronously.

        #[action]
        pub fn reset_count(&self) {
            self.count.set(0);
        }

        // Pretty much anything can be made asynced (or an action), as long as the arguments and
        // return values are Send + 'static.

        #[asynced]
        pub fn get_count(&self) -> usize {
            self.count.get()
        }

        // Normal methods won't be available from outside the shard, but can be called like normal
        // from within the same shard.

        pub fn internal_report(&self) -> String {
            format!("Produced {} items", self.count.get())
        }
    }
}

#[eventful]
struct ProductionReporter {}

impl ProductionReporter {
    pub fn new() -> ShardRcHandle<Self> {
        Self {
            events: Default::default(),
        }
        .into()
    }
}

impl producer::ProducerEvents for ProductionReporter {
    fn on_produce(&self, item: String) {
        println!("Produced: {}", item);
    }
}

#[sharded_main]
async fn main() {
    use producer::*;

    // create the producer and reporter
    let producer = Producer::new();
    let reporter = ProductionReporter::new();

    // connect the reporter to the producer's events
    producer.on_produce().connect(&reporter);

    // produce some items
    // you will notice that this call may return before the reporter has
    // finished processing all events, because we are not tracking if all
    // callbacks have been called before returning
    let produced = producer.produce(12).await;
    println!("Produced items untracked: {:?}", produced);

    // produce some items, but this time we will track the event emissions
    // this means that this call will not return until all callbacks have been called
    let produced_tracked = producer.produce_tracked(12).await;
    println!("Produced items tracked: {:?}", produced_tracked);

    // borrow the producer through its handle
    // the closure runs on the producer's shard thread
    producer.upgrade_in_shard(|producer| {
        // this will run on the producer shard/thread
        println!("Report: {}", producer.internal_report());
    });

    // using an asynced method wrapper is often more convenient than
    // upgrading a handle, but does require an async context
    let count = producer.get_count().await;
    println!("Count from async call: {}", count);

    // we can also join two handles, but they must be on the same shard
    // this allows us to upgrade both handles at the same time,
    // and run a closure on the shard/thread they live in
    let another_producer = Producer::new();
    let joined = producer.join(&another_producer).expect("same shard");

    joined.upgrade_in_shard(|(producer1, producer2)| {
        // this will run on the producer shard/thread
        println!(
            "Report from joined handles: {}",
            producer1.internal_report()
        );
        println!(
            "Report from joined handles: {}",
            producer2.internal_report()
        );
    });

    // there might be instances where no async context is available,
    // luckily asynced action methods do not require one
    fn sync_context(producer: ShardRcHandle<Producer>) {
        // we can also call an action method from a sync context
        producer.reset_count();
    }
    sync_context(producer.clone());
}
