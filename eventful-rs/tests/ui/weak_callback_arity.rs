use eventful_rs::*;
#[eventful(shard = DynamicShard)]
struct App;
fn on_key(_: impl FnMut(String, bool) + 'static) {}
fn wire(app: ShardRc<App>) {
    // The fallback must take the callback's arguments.
    on_key(weak_callback_or_else!(app, |app, _text, _ctrl| (), |_text| ()));
    on_key(weak_callback_or_else!(app, |app, _text, _ctrl| (), |_, _, _| ()));
    on_key(weak_callback_or_else!(app, |app, _text, _ctrl| (), || ()));
    // A callback without a fallback must not return a value.
    on_key(weak_callback!(app, |app, text, _ctrl| text.len()));
    // The closure's arity must match the setter.
    on_key(weak_callback!(app, |app, _text| ()));
}
fn main() {}
