# Internal Release Legal Checklist

This is an engineering checklist, not legal advice. Obtain review from the
organization's legal and compliance teams before external distribution.

## Current Components

| Component | License / terms | Release implication |
| --- | --- | --- |
| Oracle `rust-oracledb` Rust crate | UPL-1.0 OR Apache-2.0 (dual-licensed; ships LICENSE.txt and NOTICE.txt) | Preserve the Oracle copyright and license notice and verify the pinned beta dependency terms before distributing code or binaries containing the crate. |
| `webpki-roots` (transitive, via `oracledb` TLS) | CDLA-Permissive-2.0 | Mozilla root CA data under a permissive data license; allowed by a crate-scoped exception in `deny.toml`. Preserve the license notice. |
| ADBC core and FFI crates | Apache-2.0 | Preserve Apache notices and provide the applicable license text. |
| DuckDB extension API/source | MIT | Preserve the DuckDB copyright and MIT notice for copied source or redistributed DuckDB binaries. |
| Oracle Database server | Oracle proprietary license | Do not redistribute the server image, database files, passwords, or Oracle client binaries without an applicable Oracle agreement. |
| Oracle Instant Client / ODBC libraries | Oracle proprietary license | The internal artifact must not bundle these libraries unless Oracle distribution rights are confirmed. Prefer user-provided installation paths. |

## Required Before Internal Distribution

- Review the `THIRD_PARTY_LICENSES/` directory of one release archive per
  platform. `scripts/package_release.py` copies the license and notice files
  of every crate compiled into the driver for that target, including
  `rust-oracledb`'s, and lists each crate with its declared license in
  `INDEX.txt`. Crates that ship no license file are listed separately;
  confirm their declared license text is otherwise provided (for example,
  Apache-2.0 via `LICENSE`).
- Confirm each Python wheel carries the same `THIRD_PARTY_LICENSES/` files
  as the release archive for its platform, under
  `adbc_driver_orkidea-<version>.dist-info/licenses/`, together with
  `LICENSE`.
- Confirm the per-platform CycloneDX SBOMs match the bundled crates.
- Decide whether the internal package links to or dynamically loads the Oracle ADBC library.
- Keep Oracle credentials, wallets, database dumps, and client libraries out of source control and release artifacts.
- Confirm the organization's Oracle contract permits the intended number of users, environments, and redistribution model.
- Confirm whether the DuckDB UI binary and extension are distributed together or installed separately.
- Confirm DuckDB extension signing, verification, and unsigned-development-mode boundaries.
- Confirm the Orkidea name and documentation do not imply Oracle endorsement or affiliation.
- Release signing is keyless (Sigstore, bound to `release.yml` on a `v*`
  tag) and build provenance is attested by GitHub; confirm only maintainers
  can push release tags (protect `v*` tags).
- Confirm the installers (`install.sh`, `install.ps1`) only download from
  this repository's releases and verify checksums before installing.

## Required Before Upstream Submission

- Submit only the DuckDB integration code and generic ADBC integration logic.
- Do not submit Oracle credentials, Oracle database artifacts, Oracle client binaries, or internal deployment scripts.
- Confirm contributor copyright/license requirements for the target DuckDB repository.
- Provide tests that use a user-supplied Oracle service or a CI secret, not a bundled database.
- Have legal review approve any Oracle product names, trademarks, and documentation language.

## CI Database Boundary

- CI pulls Oracle Database Free (`gvenzl/oracle-free`, pinned by digest) at
  test time, runs it only on the ephemeral runner, and discards it afterwards.
  The image is never pushed, cached as a release artifact, or redistributed.
- Confirm that use of Oracle Database Free in CI complies with the Oracle Free
  Use Terms and Conditions.

## Demo Artifact Boundary

- `scripts/seed_orkidea_demo.sh` may connect to a user-provided local
  container, but it must not download, copy, or package an Oracle image into a
  source archive, release artifact, SBOM, or container image.
- Demo credentials are generated or supplied at runtime and must never be
  committed.
- The demo SQL and test data are synthetic and must not contain customer data,
  database dumps, wallets, or proprietary schemas.
