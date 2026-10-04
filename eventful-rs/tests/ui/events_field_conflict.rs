use eventful_rs::*;

#[eventful(shard = DynamicShard)]
struct Source {
    events: Vec<u32>,
}

fn main() {}
