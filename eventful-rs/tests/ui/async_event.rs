#[eventful_rs::events]
trait Unsupported { async fn event(&self); }
fn main() {}
