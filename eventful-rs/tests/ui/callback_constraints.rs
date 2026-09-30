use eventful_rs::*;
trait ListenerKind {}
#[events(ListenerKind)]
trait Buttons { fn clicked(&self, value: usize); }
#[eventful(Buttons, shard = DynamicShard)]
struct Button;
#[eventful(shard = DynamicShard)]
struct Dialog;
impl ListenerKind for Dialog {}
#[eventful(shard = DynamicShard)]
struct Other;
fn wire(button: ShardRcHandle<Button>, dialog: ShardRcHandle<Dialog>, other: ShardRcHandle<Other>) {
    button.clicked().with_receiver(&dialog).connect(|_: &Dialog, _: String| {});
    button.clicked().with_receiver(&other).connect(|_, _| {});
}
fn main() {}
