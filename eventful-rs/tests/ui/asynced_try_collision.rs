use eventful_rs::*;

#[eventful(shard = DynamicShard)]
struct BothAsynced;
#[asynchronize]
impl BothAsynced {
    #[asynced]
    fn count(&self) -> usize {
        0
    }
    #[asynced]
    fn try_count(&self) -> usize {
        0
    }
}

#[eventful(shard = DynamicShard)]
struct WithAction;
#[asynchronize]
impl WithAction {
    #[action]
    fn try_reset(&self) {}
    #[asynced]
    fn reset(&self) {}
}

#[eventful(shard = DynamicShard)]
struct UnknownOption;
#[asynchronize]
impl UnknownOption {
    #[asynced(no_retry)]
    fn count(&self) -> usize {
        0
    }
}

#[eventful(shard = DynamicShard)]
struct ShadowedByHandle;
#[asynchronize]
impl ShadowedByHandle {
    #[asynced]
    fn local(&self) -> usize {
        0
    }
}

fn main() {}
