#!/bin/sh
set -eu
cd "$(dirname "$0")/.."

# Keep this version in sync with the UI job in .github/workflows/ci.yml.
# Install locally with: rustup toolchain install 1.98.1 --profile minimal
# Review updated .stderr snapshots whenever this version changes.
cargo +1.98.1 test -p eventful-rs --test ui --locked
cargo +1.98.1 test -p eventful-rs --test ui --no-default-features --locked
cargo +1.98.1 test -p eventful-rs --test ui --all-features --locked
