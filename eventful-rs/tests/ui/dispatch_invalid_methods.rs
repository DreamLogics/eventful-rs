use eventful_rs::*;

#[eventful(shard = DynamicShard)]
struct Generic;
#[asynchronize]
impl Generic {
    #[action]
    fn store<T: Send + 'static>(&self, value: T) {
        drop(value);
    }
}

#[eventful(shard = DynamicShard)]
struct ImplArgument;
#[asynchronize]
impl ImplArgument {
    #[asynced]
    fn show(&self, value: impl std::fmt::Display + Send + 'static) -> String {
        value.to_string()
    }
}

#[eventful(shard = DynamicShard)]
struct Conflicting;
#[asynchronize]
impl Conflicting {
    #[action]
    #[asynced]
    fn run(&self) {}
}

#[eventful(shard = DynamicShard)]
struct Duplicate;
#[asynchronize]
impl Duplicate {
    #[action]
    #[action]
    fn run(&self) {}
}

#[eventful(shard = DynamicShard)]
struct Binding;
#[asynchronize]
impl Binding {
    #[action]
    fn run(&self, value @ 0..=9: u8) {
        let _ = value;
    }
}

fn main() {}
