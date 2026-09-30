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
// A setter-compatible stand-in keeps this dependency-alias fixture independent
// of Slint's renderer; the actual generated component is tested in tests/slint.rs.
type Callback = Box<dyn Fn(usize)>;
#[derive(Default)]
struct Ui {
    callback: std::cell::RefCell<Option<Callback>>,
}
impl Ui {
    fn on_changed(&self, callback: impl Fn(usize) + 'static) {
        self.callback.replace(Some(Box::new(callback)));
    }
}
#[slint_events(component = Ui)]
trait UiActions {
    fn changed(&self, __eventful_target: usize);
}
impl UiActions for State {
    fn changed(&self, _: usize) {}
}

#[sharded_main(Main)]
async fn main() -> Result<(), InvokeError> {
    let state = State::spawn(|| State {
        events: Default::default(),
    })
    .await?;
    let _connection = state.changed().connect(&state).scoped();
    state.emit_changed_tracked(state.read().await).await?;
    let ui = Ui::default();
    let bridge = UiActionsBridge::new(&ui);
    bridge.connect_to(&state);
    bridge.connect_as::<(), _>(&state).disconnect();
    ui.callback.borrow().as_ref().unwrap()(1);
    bridge.emit_changed_tracked(2).await?;
    Ok(())
}
