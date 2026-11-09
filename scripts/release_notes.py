#!/usr/bin/env python3
"""Prints the GitHub release notes for a version, from CHANGELOG.md.

    python3 scripts/release_notes.py 0.2.0 > notes.md

The notes are the changelog section `## [<version>] - <date>`, without its
heading, followed by install and verification instructions. Exits non-zero
when the section is missing or empty, so a release cannot be published
without notes.
"""

import argparse
import pathlib
import re
import sys

REPO = "Tnsor-Labs/orkidea-adbc"
ROOT = pathlib.Path(__file__).resolve().parent.parent

FOOTER = """
### Install

```bash
pip install "adbc-driver-orkidea[dbapi]=={pep440}"
```

```bash
# Linux and macOS: any ADBC client, as driver "orkidea"
curl -fsSL https://github.com/{repo}/releases/download/v{version}/install.sh | sh -s -- --version v{version}
```

```powershell
# Windows
& ([scriptblock]::Create((irm https://github.com/{repo}/releases/download/v{version}/install.ps1))) -Version v{version}
```

### Verify

```bash
cosign verify-blob --bundle SHA256SUMS.sigstore.json \\
  --certificate-identity-regexp '^https://github.com/{repo_re}/\\.github/workflows/release\\.yml@refs/tags/v' \\
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \\
  SHA256SUMS
sha256sum --ignore-missing -c SHA256SUMS
gh attestation verify <asset> --repo {repo}
```

See [docs/release.md](https://github.com/{repo}/blob/v{version}/docs/release.md) for every asset.
"""


def pep440(version: str) -> str:
    """The Python package version for a Cargo version (0.2.0-rc1 -> 0.2.0rc1)."""
    match = re.fullmatch(r"(\d+\.\d+\.\d+)(?:-(alpha|beta|rc)\.?(\d+))?", version)
    if not match:
        sys.exit(f"release_notes.py: {version!r} is not a release version")
    release, pre, number = match.groups()
    if pre is None:
        return release
    return release + {"alpha": "a", "beta": "b", "rc": "rc"}[pre] + number


def section(changelog: str, version: str) -> str:
    heading = re.compile(
        rf"^## \[{re.escape(version)}\] - \d{{4}}-\d{{2}}-\d{{2}}\s*$", re.MULTILINE
    )
    match = heading.search(changelog)
    if not match:
        sys.exit(f"release_notes.py: CHANGELOG.md has no '## [{version}] - YYYY-MM-DD' section")
    rest = changelog[match.end() :]
    end = re.search(r"^(## \[|\[[^\]]+\]: )", rest, re.MULTILINE)
    body = (rest[: end.start()] if end else rest).strip()
    if not body:
        sys.exit(f"release_notes.py: the CHANGELOG.md section for {version} is empty")
    return body


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("version", help="the Cargo version, without the leading v")
    args = parser.parse_args()
    changelog = (ROOT / "CHANGELOG.md").read_text(encoding="utf-8")
    body = section(changelog, args.version)
    footer = FOOTER.format(
        repo=REPO,
        repo_re=REPO.replace(".", r"\."),
        version=args.version,
        pep440=pep440(args.version),
    )
    sys.stdout.write(body + "\n" + footer)


if __name__ == "__main__":
    main()
