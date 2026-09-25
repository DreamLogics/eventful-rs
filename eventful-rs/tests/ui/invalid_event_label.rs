use eventful_rs::*;

struct Label;

#[events]
trait Updates {
    #[with_label(Label)]
    fn changed(&self);
}

fn main() {}
