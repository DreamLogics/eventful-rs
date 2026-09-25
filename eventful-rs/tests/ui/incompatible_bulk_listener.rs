use eventful_rs::*;
#[events]
trait Updates { fn updated(&self); }
#[eventful(Updates, shard = DynamicShard)]
struct Source;
#[eventful(shard = DynamicShard)]
struct Listener;
fn connect(source: ShardRcHandle<Source>, listener: ShardRcHandle<Listener>) {
    source.connect(&listener);
}
fn main() {}
