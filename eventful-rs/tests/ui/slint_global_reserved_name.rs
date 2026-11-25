use eventful_rs::slint_events;

#[slint_events(global = Navigator)]
trait Callbacks {
    fn connect_to(&self);
}
fn main() {}
