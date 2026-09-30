use eventful_rs::*;
use std::rc::Rc;
#[events]
trait Buttons { fn clicked(&self); }
#[eventful(Buttons, shard = DynamicShard)]
struct Button;
#[eventful(shard = DynamicShard)]
struct Dialog;
fn wire(button: ShardRcHandle<Button>, dialog: ShardRcHandle<Dialog>) {
    let capture = Rc::new(1);
    button.clicked().with_receiver(&dialog).connect(move |_| { drop(capture.clone()); });
}
fn main() {}
