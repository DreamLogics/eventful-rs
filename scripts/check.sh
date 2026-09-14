#!/bin/sh
set -eu
cargo fmt --all -- --check
cargo check --workspace --locked
cargo test --workspace --locked
cargo test -p eventful-rs --no-default-features --locked
cargo check -p eventful-rs --all-features --locked
cargo test -p eventful-rs --all-features --test slint --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo doc --workspace --no-deps --locked
cargo package -p eventful-rs-macros -p eventful-rs --locked
