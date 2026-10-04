use eventful_rs::*;

#[events]
trait Collides {
    #[cfg(all())]
    fn changed(&self);
    #[cfg(not(any()))]
    fn changed(&self, value: u32);
}

fn main() {}
