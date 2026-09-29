use eventful_rs::*;
struct A;
struct B;
#[events]
trait Buttons { fn clicked(&self); }
#[eventful(Buttons, shard = DynamicShard)]
struct Button;
#[eventful(shard = DynamicShard)]
struct Dialog;
impl Buttons<A> for Dialog { fn clicked(&self) {} }
fn wire(button: ShardRcHandle<Button>, dialog: ShardRcHandle<Dialog>) {
    button.clicked().connect_as::<B, _>(&dialog);
    button.connect_as::<B, _>(&dialog);
    button.clicked().connect(&dialog);
}
fn main() {}
