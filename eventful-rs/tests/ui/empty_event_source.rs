use eventful_rs::*;

#[eventful(shard = DynamicShard)]
struct Receiver;

#[events]
trait Empty {}
#[eventful(Empty, shard = DynamicShard)]
struct ExplicitEmpty;

#[events]
trait Disabled {
    #[cfg(any())]
    fn absent(&self);
}
#[eventful(Disabled, shard = DynamicShard)]
struct DisabledSource;

fn reversed(source: &ShardRc<Receiver>, receiver: &ShardRc<Receiver>) {
    source.connect_to(receiver);
}
fn explicit(source: &ShardRcHandle<ExplicitEmpty>, receiver: &ShardRc<Receiver>) {
    source.connect_to(receiver);
}
fn disabled(source: &ShardRc<DisabledSource>, receiver: &ShardRc<Receiver>) {
    ShardRc::connect(source, receiver);
}
fn main() {}
