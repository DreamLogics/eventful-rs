#!/bin/sh
set -eu
cmp README.md eventful-rs/README.md
cargo fmt --all -- --check
cargo test --workspace --locked
cargo test -p eventful-rs --no-default-features --locked
cargo test -p eventful-rs --all-features --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo clippy -p eventful-rs -p eventful-rs-macros --lib --all-features --locked -- -D warnings -D clippy::missing_docs_in_private_items
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps --locked
cargo package -p eventful-rs-macros -p eventful-rs --locked
