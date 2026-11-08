# Internal Release Legal Checklist

This is an engineering checklist, not legal advice. Obtain review from the
organization's legal and compliance teams before external distribution.

## Current Components

| Component | License / terms | Release implication |
| --- | --- | --- |
| Oracle `oracledb` Rust crate | UPL-1.0 or Apache-2.0 | Preserve the Oracle copyright and license notice when distributing code or binaries containing the crate. |
| ADBC core and FFI crates | Apache-2.0 | Preserve Apache notices and provide the applicable license text. |
| DuckDB extension API/source | MIT | Preserve the DuckDB copyright and MIT notice for copied source or redistributed DuckDB binaries. |
| Oracle Database server | Oracle proprietary license | Do not redistribute the server image, database files, passwords, or Oracle client binaries without an applicable Oracle agreement. |
| Oracle Instant Client / ODBC libraries | Oracle proprietary license | The internal artifact must not bundle these libraries unless Oracle distribution rights are confirmed. Prefer user-provided installation paths. |

## Required Before Internal Distribution

- Generate a complete dependency license/notice report for the Rust workspace.
- Include Oracle `oracledb`'s license notice in the driver distribution.
- Include Apache-2.0 notices for ADBC and transitive Apache-licensed components.
- Decide whether the internal package links to or dynamically loads the Oracle ADBC library.
- Keep Oracle credentials, wallets, database dumps, and client libraries out of source control and release artifacts.
- Confirm the organization's Oracle contract permits the intended number of users, environments, and redistribution model.
- Confirm whether the DuckDB UI binary and extension are distributed together or installed separately.

## Required Before Upstream Submission

- Submit only the DuckDB integration code and generic ADBC integration logic.
- Do not submit Oracle credentials, Oracle database artifacts, Oracle client binaries, or internal deployment scripts.
- Confirm contributor copyright/license requirements for the target DuckDB repository.
- Provide tests that use a user-supplied Oracle service or a CI secret, not a bundled database.
- Have legal review approve any Oracle product names, trademarks, and documentation language.
