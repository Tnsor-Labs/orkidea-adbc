# Release Process

Releases are built, tested, signed, and published by
`.github/workflows/release.yml` when a `v*` tag is pushed. A manual run of the
workflow (**Actions → Release → Run workflow**) performs every step except
signing and publishing. Use it as a dry run before tagging.

## Before tagging

1. Make sure CI is green on `main`, including the Oracle integration job.
2. Confirm `docs/legal-release-checklist.md` is approved for this release.
3. Set `version` in the workspace `Cargo.toml` and run
   `cargo check --workspace` so `Cargo.lock` follows. The workflow refuses a
   tag that does not match this version.
4. Optionally, run the workflow manually and check the `release` artifact.

Then tag and push:

```bash
git tag -s v0.2.0 -m "Orkidea ADBC 0.2.0"
git push origin v0.2.0
```

Tags with a suffix (`v0.2.0-rc1`) are published as pre-releases.

## What the workflow does

| Job | Purpose |
| --- | --- |
| `version check` | Reads the workspace version and checks the tag matches it. |
| `build (<platform>)` | Builds on a native runner for each platform. It then smoke-tests the packaged library through the ADBC driver manager, checks platform requirements, and tests the installer and manifest discovery end to end. |
| `compat` | Loads the Linux libraries on Rocky Linux 8, Debian 11, and Amazon Linux 2023. |
| `assemble` | Checks all six platforms are present, generates per-platform CycloneDX SBOMs, adds the installers, and writes `SHA256SUMS`. |
| `publish` | Tags only. Signs `SHA256SUMS` with Sigstore (keyless), records GitHub build-provenance attestations, and creates the release. |

| Platform | Runner | Notes |
| --- | --- | --- |
| `linux_amd64`, `linux_arm64` | `ubuntu-24.04`, `ubuntu-24.04-arm` | Built in `manylinux_2_28`; needs glibc 2.28+ |
| `macos_arm64`, `macos_amd64` | `macos-15`, `macos-15-intel` | Minimum macOS 11; ad-hoc signed |
| `windows_amd64`, `windows_arm64` | `windows-2025`, `windows-11-arm` | MSVC |

Container images are pinned by digest and actions by commit SHA. Dependabot
proposes updates to both.

## Release assets

- `orkidea-adbc-<version>-<platform>.tar.gz` (`.zip` on Windows): the driver
  library, `LICENSE`, and `THIRD_PARTY_LICENSES/` with the license and notice
  files of every crate compiled into the library, plus an index.
- `orkidea-adbc-<version>-<platform>-debug.tar.gz` (`.zip`): split debug
  symbols (`.debug`, `.dSYM`, or `.pdb`).
- `orkidea-adbc-<version>-<platform>.cdx.json`: the CycloneDX SBOM for that
  platform.
- `install.sh`, `install.ps1`: installers that verify and register the driver.
- `SHA256SUMS`, `SHA256SUMS.sigstore.json`: checksums of every asset, and
  their signature.

Release assets must not contain Oracle Database images, Oracle client
libraries, wallets, credentials, or local build directories.

## Verifying a release

```bash
cosign verify-blob --bundle SHA256SUMS.sigstore.json \
  --certificate-identity-regexp '^https://github.com/Tnsor-Labs/orkidea-adbc/\.github/workflows/release\.yml@refs/tags/v' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  SHA256SUMS
sha256sum --ignore-missing -c SHA256SUMS
gh attestation verify orkidea-adbc-0.2.0-linux_amd64.tar.gz --repo Tnsor-Labs/orkidea-adbc
```

`install.sh` and `install.ps1` always verify the checksum. They also verify
the signature when `cosign` is installed.

## Building locally

```bash
cargo build --locked --profile dist -p orkidea-adbc-ffi --target x86_64-unknown-linux-gnu
python3 scripts/package_release.py --target x86_64-unknown-linux-gnu \
  --platform linux_amd64 --version 0.1.0 --out dist
```

A library built directly on a recent distribution needs that
distribution's glibc. Release builds use `manylinux_2_28` to avoid this.
