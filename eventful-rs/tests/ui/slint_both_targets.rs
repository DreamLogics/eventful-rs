use eventful_rs::slint_events;

#[slint_events(component = Ui, global = Navigator)]
trait Callbacks {
    fn save(&self);
}
fn main() {}
