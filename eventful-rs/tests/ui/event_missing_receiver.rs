#[eventful_rs::events]
trait Events {
    fn some_event(values: Vec<String>);
}
fn main() {}
