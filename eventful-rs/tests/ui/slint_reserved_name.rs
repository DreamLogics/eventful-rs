use eventful_rs::slint_events;

#[slint_events(component = Ui)]
trait Callbacks {
    fn connect_to(&self);
}

#[slint_events(component = Ui)]
trait Invalid {
    async fn save(&self);
}

fn main() {}
