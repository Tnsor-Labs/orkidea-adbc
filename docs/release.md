# Release Process

1. Run `cargo fmt --all -- --check`.
2. Run `cargo test --workspace`.
3. Run the Oracle smoke tests against a user-supplied service.
4. Build the release library and DuckDB extension.
5. Generate a dependency license and notice report.
6. Confirm `docs/legal-release-checklist.md` is approved.
7. Sign and publish artifacts only after legal and security review.

Release artifacts must not contain Oracle Database images, Oracle client
libraries, wallets, credentials, or local build directories.
