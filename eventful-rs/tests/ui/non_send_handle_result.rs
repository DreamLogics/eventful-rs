use eventful_rs::*;
declare_shard!(pub Worker, runtime = std);
#[eventful(shard = Worker)]
struct Value;
fn main() {
    let _local = Worker::shard().bind(|bind| bind(Value { events: Default::default() }));
}
