#!/usr/bin/env python3
"""Packages a built Orkidea ADBC driver into release archives.

Run after `cargo build --locked --profile dist -p orkidea-adbc-ffi --target T`:

    python3 scripts/package_release.py --target T --platform linux_amd64 \
        --version 0.1.0 --out dist

Produces, in --out:

    orkidea-adbc-<version>-<platform>.tar.gz   (.zip on Windows)
        orkidea-adbc-<version>-<platform>/
            lib/<driver library>
            LICENSE
            README.txt
            THIRD_PARTY_LICENSES/<crate>-<version>/...   license and notice files
            THIRD_PARTY_LICENSES/INDEX.txt               every bundled crate
    orkidea-adbc-<version>-<platform>-debug.tar.gz      (.zip on Windows)
        split debug symbols (.debug, .dSYM, or .pdb)

Only the standard library is used, so it runs on every release runner.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tarfile
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
FFI_PACKAGE = "orkidea-adbc-ffi"
LICENSE_PREFIXES = ("LICENSE", "LICENCE", "COPYING", "NOTICE", "COPYRIGHT")


def run(*args: str, cwd: Path | None = None) -> str:
    result = subprocess.run(args, cwd=cwd, check=True, capture_output=True, text=True)
    return result.stdout


def library_names(platform: str) -> tuple[str, str | None]:
    """The driver library and its debug-symbol file for a platform."""
    os_name = platform.split("_", 1)[0]
    if os_name == "linux":
        return "libadbc_driver_orkidea.so", "libadbc_driver_orkidea.so.debug"
    if os_name == "macos":
        return "libadbc_driver_orkidea.dylib", "libadbc_driver_orkidea.dylib.dSYM"
    if os_name == "windows":
        return "adbc_driver_orkidea.dll", "adbc_driver_orkidea.pdb"
    raise SystemExit(f"unknown platform {platform}")


def split_debug_info(platform: str, library: Path, debug: Path) -> None:
    """Moves debug info out of `library` into `debug`, leaving exports intact."""
    os_name = platform.split("_", 1)[0]
    if os_name == "linux":
        run("objcopy", "--only-keep-debug", str(library), str(debug))
        run("objcopy", "--strip-debug", "--strip-unneeded", str(library))
        run("objcopy", f"--add-gnu-debuglink={debug}", str(library))
    elif os_name == "macos":
        run("dsymutil", str(library), "-o", str(debug))
        run("strip", "-x", str(library))
        # Stripping invalidates the linker's ad-hoc signature, which arm64
        # macOS requires before it will load the library.
        run("codesign", "--force", "--sign", "-", str(library))
    # Windows: MSVC already writes debug info to a separate .pdb.


def bundled_packages(target: str) -> list[dict]:
    """Crates compiled into the driver for `target` (normal dependencies only;
    build scripts and dev-dependencies do not ship)."""
    metadata = json.loads(
        run(
            "cargo",
            "metadata",
            "--format-version",
            "1",
            "--locked",
            "--filter-platform",
            target,
            cwd=ROOT,
        )
    )
    packages = {p["id"]: p for p in metadata["packages"]}
    nodes = {n["id"]: n for n in metadata["resolve"]["nodes"]}
    root = next(p["id"] for p in metadata["packages"] if p["name"] == FFI_PACKAGE)
    seen, stack = set(), [root]
    while stack:
        node = nodes[stack.pop()]
        for dep in node["deps"]:
            normal = any(k["kind"] is None for k in dep["dep_kinds"])
            if normal and dep["pkg"] not in seen:
                seen.add(dep["pkg"])
                stack.append(dep["pkg"])
    workspace = set(metadata["workspace_members"])
    return sorted(
        (packages[i] for i in seen if i not in workspace),
        key=lambda p: (p["name"], p["version"]),
    )


def copy_third_party_licenses(target: str, destination: Path) -> None:
    destination.mkdir(parents=True)
    index = ["Crates compiled into this driver, with their declared licenses.", ""]
    missing = []
    for package in bundled_packages(target):
        name = f"{package['name']}-{package['version']}"
        source = Path(package["manifest_path"]).parent
        files = sorted(
            f
            for f in source.iterdir()
            if f.is_file() and f.name.upper().startswith(LICENSE_PREFIXES)
        )
        if files:
            crate_dir = destination / name
            crate_dir.mkdir()
            for f in files:
                shutil.copy2(f, crate_dir / f.name)
        else:
            missing.append(name)
        index.append(f"{name}: {package.get('license') or 'see crate'}")
    if missing:
        index += [
            "",
            "Crates that ship no license file; their declared license applies. The",
            "Apache-2.0 text is in LICENSE at the root of this archive.",
        ]
        index += [f"  {m}" for m in missing]
    (destination / "INDEX.txt").write_text("\n".join(index) + "\n", encoding="utf-8")


def readme(version: str, platform: str, library: str) -> str:
    return f"""Orkidea ADBC driver {version} ({platform})

Not affiliated with, sponsored by, or endorsed by Oracle Corporation.

Install and register the driver with the ADBC driver manager:

  Linux / macOS:  ./install.sh --archive <this archive>
  Windows:        .\\install.ps1 -Archive <this archive>

or download and verify it directly:

  curl -fsSL https://github.com/Tnsor-Labs/orkidea-adbc/releases/download/v{version}/install.sh | sh

Afterwards, load it as driver "orkidea". To load the library directly
instead, use lib/{library} with entrypoint OrkideaDriverInit.

Documentation: https://github.com/Tnsor-Labs/orkidea-adbc
"""


def make_archive(source_dir: Path, archive: Path) -> None:
    if archive.suffix == ".zip":
        with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as z:
            for path in sorted(source_dir.rglob("*")):
                z.write(path, path.relative_to(source_dir.parent))
    else:
        with tarfile.open(archive, "w:gz") as t:
            t.add(source_dir, arcname=source_dir.name)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--target", required=True)
    parser.add_argument("--platform", required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--out", default="dist")
    parser.add_argument("--profile", default="dist")
    args = parser.parse_args()

    library_name, debug_name = library_names(args.platform)
    built = ROOT / "target" / args.target / args.profile
    out = Path(args.out).resolve()
    out.mkdir(parents=True, exist_ok=True)
    base = f"orkidea-adbc-{args.version}-{args.platform}"
    extension = ".zip" if args.platform.startswith("windows") else ".tar.gz"

    work = out / "work"
    shutil.rmtree(work, ignore_errors=True)
    package_dir = work / base
    (package_dir / "lib").mkdir(parents=True)
    debug_dir = work / f"{base}-debug"
    debug_dir.mkdir()

    library = package_dir / "lib" / library_name
    shutil.copy2(built / library_name, library)
    debug = debug_dir / debug_name
    if args.platform.startswith("windows"):
        shutil.copy2(built / debug_name, debug)
    else:
        split_debug_info(args.platform, library, debug)

    shutil.copy2(ROOT / "LICENSE", package_dir / "LICENSE")
    (package_dir / "README.txt").write_text(
        readme(args.version, args.platform, library_name), encoding="utf-8"
    )
    copy_third_party_licenses(args.target, package_dir / "THIRD_PARTY_LICENSES")

    size = library.stat().st_size
    make_archive(package_dir, out / f"{base}{extension}")
    make_archive(debug_dir, out / f"{base}-debug{extension}")
    shutil.rmtree(work)
    print(f"packaged {base}{extension} ({size // 1024} KiB library)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
