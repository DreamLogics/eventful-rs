use eventful_rs::*;
struct Label;
impl EventLabel for Label { fn matches(&self, _: &Self) -> bool { true } }
#[events]
trait Updates {
    fn plain(&self, value: usize);
    #[with_label(Label)]
    fn changed(&self, value: usize);
}
#[eventful(Updates, shard = DynamicShard)]
struct Source;
fn invalid(source: &Source, handle: &ShardRcHandle<Source>, shard: &ShardEventHandle) {
    handle.plain().emit(1);
    handle.events().plain().emit(1);
    source.events.changed().emit(1);
    source.events.plain().labelled(Label);
    source.events.plain().tracked().connect(|_| {});
    handle.plain().on_shard(shard).emit(1);
    handle.plain().on_shard(shard).on_shard(shard);
    handle.changed().labelled(Label).labelled(Label);
    handle.plain().with_receiver(handle).on_shard(shard);
    handle.plain().on_shard(shard).role::<()>();
    source.events.plain().tracked().tracked();
}
fn main() {}
