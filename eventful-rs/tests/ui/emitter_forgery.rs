use eventful_rs::*;
#[events]
trait Updates { fn changed(&self); }
fn main() {
    let _ = UpdatesEmissions { signals: std::sync::Arc::new(Default::default()) };
}
