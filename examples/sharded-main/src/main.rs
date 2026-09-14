use eventful_rs::*;

mod producer {
    use eventful_rs::*;

    shard_std!(PRODUCER);

    #[events]
    pub trait ProducerEvents {
        fn on_produce(&self, item: String);
    }

    #[eventful(ProducerEvents)]
    pub struct Producer {}

    #[asynchronize]
    impl Producer {
        pub fn new() -> ShardRcHandle<Self> {
            Self {
                events: Default::default(),
            }
            .into()
        }

        #[asynced]
        pub fn produce(&self, count: usize) -> Vec<String> {
            for i in 0..count {
                let item = format!("Item {}", i);
                self.emit_on_produce(item.clone());
            }
            (0..count).map(|i| format!("Item {}", i)).collect()
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
    let producer = Producer::new();
    let reporter = ProductionReporter::new();

    producer.on_produce().connect(&reporter);

    let produced = producer.produce(12).await;
    println!("Produced items: {:?}", produced);
}
