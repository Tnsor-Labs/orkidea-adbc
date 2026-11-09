# Release Process

1. Run `cargo fmt --all -- --check`.
2. Run `cargo test --workspace`.
3. Run the Oracle smoke tests against a user-supplied service.
4. Build the release library and DuckDB extension.
5. Run `cargo clippy --workspace --all-targets --locked -- -D warnings`,
   `cargo deny --locked check`, and `cargo audit --deny warnings`.
6. Generate the SBOM with `cargo cyclonedx --format json` (the release
   artifact is `crates/orkidea-adbc-ffi/orkidea-adbc-ffi.cdx.json`) and
   confirm third-party notices are included.
7. Confirm `docs/legal-release-checklist.md` is approved.
8. Sign artifacts and publish SHA-256 checksums only after legal and security review.

Release artifacts must not contain Oracle Database images, Oracle client
libraries, wallets, credentials, or local build directories.
