use eventful_rs::slint_events;

#[slint_events]
trait Callbacks {
    fn save(&self);
}
fn main() {}
