use eventful_rs::*;
use std::cell::RefCell;

declare_shard!(pub Main, runtime = main);
declare_shard!(pub ProducerShard, runtime = std);
declare_shard!(pub ReporterShard, runtime = std);
file_scope!(shard = ProducerShard);

#[events]
trait BatchEvents {
    fn item(&self, name: String);
    fn finished(&self, count: usize);
}

#[eventful(BatchEvents)]
struct Producer;

#[asynchronize]
impl Producer {
    #[asynced]
    async fn produce(&self, names: Vec<String>) -> Result<(), DeliveryError> {
        let count = names.len();
        for name in names {
            self.emit_item_tracked(name).await?;
        }
        self.emit_finished_tracked(count).await
    }
}

// A listener can live on a different shard and use local, mutable state.
#[eventful(shard = ReporterShard)]
struct Reporter {
    names: RefCell<Vec<String>>,
    batches: RefCell<Vec<usize>>,
}

// Implement the whole interface to receive all its events through connect().
impl BatchEvents for Reporter {
    fn item(&self, name: String) {
        self.names.borrow_mut().push(name);
    }

    fn finished(&self, count: usize) {
        self.batches.borrow_mut().push(count);
    }
}

#[asynchronize]
impl Reporter {
    #[asynced]
    fn totals(&self) -> (usize, usize) {
        (self.names.borrow().len(), self.batches.borrow().len())
    }
}

async fn show_totals(reporter: &ShardRcHandle<Reporter>, label: &str, expected: (usize, usize)) {
    let totals = reporter.totals().await;
    assert_eq!(totals, expected);
    println!("{label}: {} items, {} batches", totals.0, totals.1);
}

#[sharded_main(Main)]
async fn main() -> Result<(), DeliveryError> {
    let producer = Producer::spawn(|| Producer {
        events: Default::default(),
    })
    .await?;
    let reporter = Reporter::spawn(|| Reporter {
        names: RefCell::default(),
        batches: RefCell::default(),
        events: Default::default(),
    })
    .await?;

    // One call subscribes to both item and finished. Targets remain weak, so keep
    // the reporter handle alive while it is needed.
    let connections = producer.connect(&reporter);
    producer
        .produce(vec!["apple".into(), "pear".into()])
        .await?;
    show_totals(&reporter, "Connected", (2, 1)).await;

    // Explicit disconnection removes both subscriptions.
    connections.disconnect();
    producer.produce(vec!["not delivered".into()]).await?;
    show_totals(&reporter, "Disconnected", (2, 1)).await;

    {
        // A scoped group disconnects every subscription when its guard drops.
        let _guard = producer.connect(&reporter).scoped();
        producer.produce(vec!["plum".into()]).await?;
        show_totals(&reporter, "Inside scope", (3, 2)).await;
    }
    producer.produce(vec!["also not delivered".into()]).await?;
    show_totals(&reporter, "After scope", (3, 2)).await;

    // A plain group's drop does NOT disconnect, matching individual connections.
    drop(producer.connect(&reporter));
    producer.produce(vec!["peach".into()]).await?;
    show_totals(&reporter, "Dropped plain group", (4, 3)).await;

    // produce() awaits tracked deliveries, so the totals above need no sleeps.
    // sharded_main joins the background shards after this function returns.
    Ok(())
}
