#!/bin/sh
set -eu
cmp README.md eventful-rs/README.md
cargo fmt --all -- --check
cargo run --manifest-path tests/renamed-dependency/Cargo.toml --locked
cargo test --workspace --locked
cargo test -p eventful-rs --no-default-features --locked
cargo test -p eventful-rs --all-features --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo clippy -p eventful-rs -p eventful-rs-macros --lib --all-features --locked -- -D warnings -D clippy::missing_docs_in_private_items -D clippy::missing_errors_doc -D clippy::missing_panics_doc
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps --locked
cargo package -p eventful-rs-macros -p eventful-rs --locked
