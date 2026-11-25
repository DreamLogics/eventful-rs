use eventful_rs::*;
#[eventful(shard = DynamicShard)]
struct App;
struct Window {
    app: ShardRc<App>,
}
fn on_key(_: impl FnMut(String, bool) + 'static) {}
fn on_count(_: impl FnMut() -> i32 + 'static) {}
impl Window {
    fn wire_self(&self) {
        // Only a variable name is accepted before `=>`.
        on_key(weak_callback!(self.app => |_text, _ctrl| ()));
        on_key(weak_callback!(self => |_text, _ctrl| ()));
    }
}
fn wire(app: ShardRc<App>, window: Window) {
    // The fallback must take the callback's arguments.
    on_key(weak_callback_or_else!(app => |_text, _ctrl| (), |_text| ()));
    on_key(weak_callback_or_else!(app => |_text, _ctrl| (), |_, _, _| ()));
    on_key(weak_callback_or_else!(app => |_text, _ctrl| (), || ()));
    on_count(weak_callback_or_else!(app => || 1, |_| 0));
    // A callback without a fallback must not return a value.
    on_key(weak_callback!(app => |text, _ctrl| text.len()));
    // The closure's arity must match the setter.
    on_key(weak_callback!(app => |_text| ()));
    // An expression owner must be bound to a variable first.
    on_key(weak_callback!(window.app => |_text, _ctrl| ()));
    on_count(weak_callback_or_else!(window.app => || 1, || 0));
    // The receiver is no longer written in the closure.
    on_key(weak_callback!(app, |app, _text, _ctrl| ()));
    on_count(weak_callback_or_else!(app, |app| 1, || 0));
}
fn main() {}
