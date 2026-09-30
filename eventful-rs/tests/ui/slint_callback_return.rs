use eventful_rs::slint_events;

#[slint_events(component = Ui)]
trait Callbacks {
    fn confirm(&self) -> bool;
}
fn main() {}
