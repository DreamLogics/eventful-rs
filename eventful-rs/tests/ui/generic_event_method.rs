#[eventful_rs::events]
trait Events {
    fn some_event<T>(&self, value: T);
}
fn main() {}
