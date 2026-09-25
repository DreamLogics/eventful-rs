use eventful_rs::*;
#[eventful(shard = DynamicShard)]
struct Value;
fn main() {
    let _ = Value::spawn(|| Value { events: Default::default() });
}
