use eventful_rs::*;
declare_shard!(Worker, runtime = std);
declare_shard!(Other, runtime = std);
#[eventful(shard = Worker)]
struct Value;
fn main() {
    let _ = Other::bind_async(|bind| bind(Value { events: Default::default() }).to_handle());
}
