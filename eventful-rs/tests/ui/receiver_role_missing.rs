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
    button.clicked().role::<B>().connect(&dialog);
    button.role::<B>().connect(&dialog);
    button.clicked().connect(&dialog);
}
fn main() {}
