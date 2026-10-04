#![deny(unused_variables)]
use eventful_rs::*;

#[events]
trait ChatEvents {
    fn on_messages(&self, token: String, messages: Vec<String>) {}
    /// User attributes on a defaulted method are kept.
    fn on_stopped(&self, token: String, reason: u32) {}
    fn on_required(&self, token: String);
}

#[eventful(ChatEvents, shard = DynamicShard)]
struct Feed;

#[eventful(shard = DynamicShard)]
struct View;
impl ChatEvents for View {
    fn on_required(&self, token: String) {
        let _ = token;
    }
}

fn main() {}
