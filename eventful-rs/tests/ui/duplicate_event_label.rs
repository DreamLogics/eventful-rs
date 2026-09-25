use eventful_rs::*;

#[events]
trait Updates {
    #[with_label(())]
    #[with_label(())]
    fn changed(&self);
}

fn main() {}
