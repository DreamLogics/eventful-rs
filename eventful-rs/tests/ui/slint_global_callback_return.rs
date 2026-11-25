use eventful_rs::slint_events;

#[slint_events(global = Navigator)]
trait Callbacks {
    fn confirm(&self) -> bool;
}
fn main() {}
