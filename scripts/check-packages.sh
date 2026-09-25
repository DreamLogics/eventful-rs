#!/bin/sh
set -eu
cd "$(dirname "$0")/.."

# Cargo caches dependencies from its temporary packaging registry by version.
# A unique registry path prevents a previous build of this macro version from
# being reused when verifying the runtime crate.
mkdir -p target/package
package_target=$(mktemp -d "$PWD/target/package-check.XXXXXX")
trap 'rm -rf "$package_target"' 0
trap 'exit 1' HUP INT TERM

cargo package -p eventful-rs-macros -p eventful-rs --locked \
    "$@" --target-dir "$package_target"
cp "$package_target"/package/*.crate target/package/
