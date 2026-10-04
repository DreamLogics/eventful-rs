use eventful_rs::*;

#[events]
trait Roles {
    fn role(&self);
}

#[events]
trait Storage {
    fn signals(&self);
}

#[events]
trait Accessors {
    fn events(&self);
}

#[events]
trait SameSignal {
    fn foo_bar(&self);
    fn foo__bar(&self);
}

fn main() {}
