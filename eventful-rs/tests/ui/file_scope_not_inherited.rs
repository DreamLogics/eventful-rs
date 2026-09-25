eventful_rs::file_scope!(shard = eventful_rs::DynamicShard);
mod child {
    #[eventful_rs::eventful]
    struct Value;
}
fn main() {}
