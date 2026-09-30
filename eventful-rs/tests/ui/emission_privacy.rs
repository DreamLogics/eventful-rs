use eventful_rs::*;
mod model {
    use super::*;
    #[events]
    pub trait Updates { fn changed(&self); }
    #[eventful(Updates, shard = DynamicShard)]
    pub struct Source;
}
fn invalid(source: &model::Source, events: &model::UpdatesEventSet) {
    source.events.changed().emit();
    events.changed().inner.emit(());
}
fn main() {}
