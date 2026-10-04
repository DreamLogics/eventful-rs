use eventful_rs::*;

declare_shard!(Main, runtime = main);

// The generated body is nested privately, so this sibling name stays free.
#[allow(dead_code)]
fn main_sharded_main() {}

#[sharded_main(Main)]
#[expect(unused_variables)]
pub async fn main() {
    let unused = 1;
}
