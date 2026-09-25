use eventful_rs::*;

declare_shard!(pub Worker, runtime = std);
declare_shard!(pub Other, runtime = std);

#[eventful]
struct BeforeSelection;
file_scope!(shard = Worker,);
#[eventful]
struct AfterSelection;
#[eventful(shard = Other)]
struct Override;

#[path = "support/file_scope_models.rs"]
mod models;

#[scope(shard = crate::Other)]
mod scoped {
    eventful_rs::file_scope!(shard = crate::Worker);
    #[eventful_rs::eventful]
    pub struct Selected;
}

mod imported {
    use super::*;
    file_scope!(shard = Other);
    #[eventful]
    pub struct Selected;
}

mod imported_default {
    use super::*;
    #[eventful]
    pub struct Selected;
}

#[test]
fn file_selection_and_overrides_resolve_to_the_expected_shard() {
    fn worker<T: Eventful<Shard = Worker>>() {}
    fn other<T: Eventful<Shard = Other>>() {}
    worker::<BeforeSelection>();
    worker::<AfterSelection>();
    worker::<imported_default::Selected>();
    other::<Override>();
    other::<models::Local>();
    other::<scoped::Selected>();
    other::<imported::Selected>();
    let value = futures::executor::block_on(models::Local::spawn(models::Local::new)).unwrap();
    let result =
        futures::executor::block_on(value.deferred_upgrade_in_shard(async |value| *value.value));
    assert_eq!(result, 7);
    Other::shard().join().unwrap();
}
