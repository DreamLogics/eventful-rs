use eventful_rs::slint_events;

#[slint_events(component = Ui)]
trait Callbacks {
    #[with_label(usize)]
    fn save(&self);
}
fn main() {}
