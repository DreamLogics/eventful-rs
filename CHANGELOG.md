# Changelog

## 0.1.1 - 2026-09-26

- Updated the documentation for clarity.

- Dual-license both crates under MIT OR Apache-2.0 and explicitly credit Sanne
  Ladage alongside contributors in copyright notices and author metadata.

- Add a manually triggered release workflow with changelog/version validation,
  full CI checks, dependency-ordered publishing, commit tags, and curated GitHub
  release notes. Support retrying partial releases from the same commit.

- Align the public API with the Rust API Guidelines: add unconditional `Debug`
  implementations for runtime handles, make identity and event storage read-only,
  seal the internal dispatch contract, and mark error enums non-exhaustive.
- Preserve conditional compilation and visibility in generated items and support
  renamed runtime dependencies. Add regression coverage for these macro contracts.

## 0.1.0 - 2026-09-25

- Initial release of `eventful-rs` and `eventful-rs-macros` crates. The library provides
  thread-affine state, typed events, method dispatch, explicit lifetimes, and runtime
  choice for Rust applications.
