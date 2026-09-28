eventful_rs::use_shard!(shard = eventful_rs::DynamicShard);
mod child {
    #[eventful_rs::eventful]
    struct Value;
}
fn main() {}
