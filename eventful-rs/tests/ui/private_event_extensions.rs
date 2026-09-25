use eventful_rs::events;

mod internal {
    use super::*;

    #[derive(Clone)]
    struct Payload;

    #[events]
    trait Updates {
        fn changed(&self, value: Payload);
    }
}

use internal::{UpdatesEmittersExt, UpdatesSignalsExt};

fn main() {}
