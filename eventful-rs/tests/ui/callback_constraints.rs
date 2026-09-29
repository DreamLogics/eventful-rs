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
    button.clicked().connect_fn(&dialog, |_: &Dialog, _: String| {});
    button.clicked().connect_fn(&other, |_, _| {});
}
fn main() {}
