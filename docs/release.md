# Release Process

Releases are built, tested, signed, and published by
`.github/workflows/release.yml` when a `v*` tag is pushed: release archives
to GitHub, and Python wheels to GitHub and PyPI. A manual run of the
workflow (**Actions → Release → Run workflow**) performs every step except
signing and publishing. Use it as a dry run before tagging.

## One-time setup

PyPI accepts uploads through
[trusted publishing](https://docs.pypi.org/trusted-publishers/), so no API
token is stored in the repository:

1. On PyPI, add a trusted publisher for project `adbc-driver-orkidea` (a
   "pending publisher" before the first upload): owner `Tnsor-Labs`,
   repository `orkidea-adbc`, workflow `release.yml`, environment `pypi`.
2. On GitHub, create the environment `pypi` (**Settings → Environments**).
   Optionally add required reviewers, so every upload waits for approval,
   and restrict it to `v*` tags.
3. Protect `v*` tags (**Settings → Rules**), so only maintainers can start
   a release.

## Before tagging

1. Make sure CI is green on `main`, including the Oracle integration job.
2. Confirm `docs/legal-release-checklist.md` is approved for this release.
3. Set `version` in the workspace `Cargo.toml` and run
   `cargo check --workspace` so `Cargo.lock` follows. The workflow refuses a
   tag that does not match this version.
4. Write the release notes: rename `[Unreleased]` in `CHANGELOG.md` to
   `## [<version>] - <date>` and start a new empty `[Unreleased]` above it.
   Update the link references at the bottom. Preview the result with
   `python3 scripts/release_notes.py <version>`. See
   [Release notes](#release-notes) below.
5. Optionally, run the workflow manually and check the `release` and
   `release-notes` artifacts.

Then tag and push:

```bash
git tag -s v0.4.1 -m "Orkidea ADBC 0.4.1"
git push origin v0.4.1
```

Tags with a suffix (`v0.4.1-rc1`) are published as pre-releases.

## Release notes

The changelog section for a version is its release notes. It is written and
reviewed with the rest of the release, before the tag, and published
unchanged; nothing is edited on GitHub afterwards.
`scripts/release_notes.py` adds install and verification instructions for
that version.

A section has:

- an opening paragraph on what the release is about and how it was tested;
- **"Behaviour changes to read before upgrading"**, always present: anything
  that can change what existing code or data does, such as a type mapping, an
  option default, or an error that used to be a success. When there are none,
  it says so, because an empty section is a claim;
- the changes, grouped by area (queries and types, parameters and PL/SQL,
  ingestion, catalog, partitions, connections, Python, packaging). Each entry
  starts with a bold summary of what changed for the user, not the commit
  subject, and names its owner by GitHub handle (`-- @handle`);
- known limitations, when they change.

## What the workflow does

| Job | Purpose |
| --- | --- |
| `version check` | Reads the workspace version and checks the tag matches it. Renders the release notes from `CHANGELOG.md`; a tag whose version has no changelog section fails here, before anything is built. |
| `build (<platform>)` | Builds on a native runner for each platform. It then smoke-tests the packaged library through the ADBC driver manager, checks platform requirements, and tests the installer and manifest discovery end to end. It builds the Python wheel from the packaged library, checks its manylinux tag with `auditwheel` on Linux, and runs the Python tests against the installed wheel. |
| `compat` | Loads the Linux libraries on Rocky Linux 8, Debian 11, and Amazon Linux 2023. |
| `assemble` | Checks all six platforms and wheels are present, checks the wheels' metadata with `twine`, generates per-platform CycloneDX SBOMs, adds the installers, and writes `SHA256SUMS`. |
| `publish` | Tags only. Signs `SHA256SUMS` with Sigstore (keyless), records GitHub build-provenance attestations, and creates the release with the rendered notes. |
| `publish to PyPI` | Tags only, after `publish`. Uploads the wheels with trusted publishing, which adds PEP 740 attestations. If it fails (for example, before the one-time setup), fix the cause and re-run this job alone. |

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
- `adbc_driver_orkidea-<version>-py3-none-<tag>.whl`: the Python wheel for
  each platform (`manylinux_2_28_x86_64`, `manylinux_2_28_aarch64`,
  `macosx_11_0_x86_64`, `macosx_11_0_arm64`, `win_amd64`, `win_arm64`). It
  bundles the same library and license files as the archive, and works with
  any Python 3.10 or newer. Pre-release versions follow PEP 440
  (`0.4.1-rc1` becomes `0.4.1rc1`).
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
gh attestation verify orkidea-adbc-0.4.1-linux_amd64.tar.gz --repo Tnsor-Labs/orkidea-adbc
```

`install.sh` and `install.ps1` always verify the checksum. They also verify
the signature when `cosign` is installed.

## Building locally

```bash
cargo build --locked --profile dist -p orkidea-adbc-ffi --target x86_64-unknown-linux-gnu
python3 scripts/package_release.py --target x86_64-unknown-linux-gnu \
  --platform linux_amd64 --version 0.4.1 --out dist
```

A library built directly on a recent distribution needs that
distribution's glibc. Release builds use `manylinux_2_28` to avoid this.

To build a wheel from a packaged library (Python 3.11 or newer runs the
build backend, `python/build_backend.py`, which needs only the standard
library):

```bash
tar -xzf dist/orkidea-adbc-0.4.1-linux_amd64.tar.gz -C dist
ORKIDEA_ADBC_LIBRARY=$PWD/dist/orkidea-adbc-0.4.1-linux_amd64/lib/libadbc_driver_orkidea.so \
ORKIDEA_ADBC_THIRD_PARTY_LICENSES=$PWD/dist/orkidea-adbc-0.4.1-linux_amd64/THIRD_PARTY_LICENSES \
ORKIDEA_ADBC_PLATFORM=linux_amd64 \
  python3 -m pip wheel --no-deps --wheel-dir dist ./python
```
