//! Check macro expansion when the runtime has a different dependency name.
use notifications::*;

declare_shard!(Main, runtime = main);

#[events]
trait Updates {
    fn changed(&self, value: usize);
}
#[eventful(Updates, shard = Main)]
struct State;
impl Updates for State {
    fn changed(&self, _: usize) {}
}
#[asynchronize]
impl State {
    #[asynced]
    fn read(&self) -> usize {
        42
    }
}
#[sharded_main(Main)]
async fn main() -> Result<(), InvokeError> {
    let state = State::spawn(|| State {
        events: Default::default(),
    })
    .await?;
    let _connection = state.changed().connect(&state).scoped();
    state.emit_changed_tracked(state.read().await).await?;
    Ok(())
}
