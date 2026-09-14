use eventful_rs::*;
shard_std!(WORKER);
#[eventful]
struct Value;
fn main() {
    let _local = WORKER.bind(|bind| bind(Value { events: Default::default() }));
}
