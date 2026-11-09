"""PEP 517 build backend for adbc-driver-orkidea.

The wheel bundles a prebuilt Orkidea driver library next to the Python
package, so building it needs no compiler. It uses only the standard library.

Inputs, from the environment:

``ORKIDEA_ADBC_LIBRARY``
    The driver library to bundle. Defaults to the Cargo release build,
    ``../target/release/<library>``.
``ORKIDEA_ADBC_THIRD_PARTY_LICENSES``
    The ``THIRD_PARTY_LICENSES`` directory of the release archive that holds
    the library (see ``scripts/package_release.py``). Its files are added to
    the wheel's license files. Release builds must set it.
``ORKIDEA_ADBC_PLATFORM``
    The ADBC platform the library was built for (``linux_amd64``, ...). It
    selects the wheel's platform tag. Defaults to a tag for the building
    interpreter, which is good enough for a local install.
``SOURCE_DATE_EPOCH``
    Timestamp for the files in the wheel (default 1980-01-01), so identical
    inputs give identical wheels.

Only wheels are built. ``python -m build --wheel`` and ``pip install`` both
work; a source distribution is not provided, because a source build needs
the Rust workspace.
"""

from __future__ import annotations

import base64
import csv
import hashlib
import io
import os
import pathlib
import platform
import re
import sys
import sysconfig
import time
import tomllib
import zipfile

HERE = pathlib.Path(__file__).resolve().parent
ROOT = HERE.parent
PACKAGE = "adbc_driver_orkidea"

# Wheel platform tags. They must agree with the release build: manylinux_2_28
# containers on Linux and MACOSX_DEPLOYMENT_TARGET=11.0 on macOS.
PLATFORM_TAGS = {
    "linux_amd64": "manylinux_2_28_x86_64",
    "linux_arm64": "manylinux_2_28_aarch64",
    "macos_amd64": "macosx_11_0_x86_64",
    "macos_arm64": "macosx_11_0_arm64",
    "windows_amd64": "win_amd64",
    "windows_arm64": "win_arm64",
}

LIBRARY_NAMES = {
    "linux": "libadbc_driver_orkidea.so",
    "macos": "libadbc_driver_orkidea.dylib",
    "windows": "adbc_driver_orkidea.dll",
}

# pyproject.toml [project] keys this backend turns into core metadata. Any
# other key is an error, so that metadata is never silently dropped.
SUPPORTED_KEYS = {
    "name",
    "dynamic",
    "description",
    "readme",
    "requires-python",
    "license",
    "authors",
    "keywords",
    "classifiers",
    "dependencies",
    "optional-dependencies",
    "urls",
}


class BuildError(RuntimeError):
    """The wheel cannot be built from the given inputs."""


def _host_os() -> str:
    if sys.platform.startswith("linux"):
        return "linux"
    if sys.platform == "darwin":
        return "macos"
    if sys.platform == "win32":
        return "windows"
    raise BuildError(f"unsupported platform {sys.platform!r}")


def _platform() -> tuple[str, str]:
    """The target's operating system and wheel platform tag."""
    adbc_platform = os.environ.get("ORKIDEA_ADBC_PLATFORM")
    if adbc_platform:
        try:
            tag = PLATFORM_TAGS[adbc_platform]
        except KeyError:
            known = ", ".join(sorted(PLATFORM_TAGS))
            raise BuildError(
                f"ORKIDEA_ADBC_PLATFORM={adbc_platform!r} is not one of {known}"
            ) from None
        return adbc_platform.split("_", 1)[0], tag
    # A local build: tag the wheel for this interpreter's platform.
    tag = re.sub(r"[-.]", "_", sysconfig.get_platform())
    if tag.endswith("_universal2"):
        tag = tag[: -len("universal2")] + platform.machine()
    return _host_os(), tag


def _version() -> str:
    """The workspace version, as a PEP 440 version."""
    with open(ROOT / "Cargo.toml", "rb") as f:
        version = tomllib.load(f)["workspace"]["package"]["version"]
    match = re.fullmatch(r"(\d+\.\d+\.\d+)(?:-(alpha|beta|rc)\.?(\d+))?", version)
    if not match:
        raise BuildError(f"Cargo version {version!r} has no PEP 440 equivalent")
    release, pre, number = match.groups()
    if pre is None:
        return release
    pep440_pre = {"alpha": "a", "beta": "b", "rc": "rc"}[pre]
    return f"{release}{pep440_pre}{number}"


def _project() -> dict:
    with open(HERE / "pyproject.toml", "rb") as f:
        project = tomllib.load(f)["project"]
    unknown = set(project) - SUPPORTED_KEYS
    if unknown:
        raise BuildError(f"unsupported [project] keys: {', '.join(sorted(unknown))}")
    if project.get("dynamic") != ["version"]:
        raise BuildError("[project] dynamic must be exactly ['version']")
    return project


def _metadata(project: dict, version: str, license_files: list[str]) -> str:
    lines = [
        "Metadata-Version: 2.4",
        f"Name: {project['name']}",
        f"Version: {version}",
        f"Summary: {project['description']}",
    ]
    for author in project.get("authors", []):
        lines.append(f"Author: {author['name']}")
    lines.append(f"License-Expression: {project['license']}")
    lines += [f"License-File: {path}" for path in license_files]
    if project.get("keywords"):
        lines.append(f"Keywords: {','.join(project['keywords'])}")
    lines += [f"Classifier: {c}" for c in project.get("classifiers", [])]
    lines += [f"Project-URL: {k}, {v}" for k, v in project.get("urls", {}).items()]
    lines.append(f"Requires-Python: {project['requires-python']}")
    lines += [f"Requires-Dist: {d}" for d in project.get("dependencies", [])]
    for extra, deps in project.get("optional-dependencies", {}).items():
        lines.append(f"Provides-Extra: {extra}")
        for dep in deps:
            requirement, _, marker = dep.partition(";")
            condition = f'extra == "{extra}"'
            if marker.strip():
                condition = f"({marker.strip()}) and {condition}"
            lines.append(f"Requires-Dist: {requirement.strip()}; {condition}")
    readme = project["readme"]
    if not readme.endswith(".md"):
        raise BuildError("readme must be a Markdown file")
    lines.append("Description-Content-Type: text/markdown")
    description = (HERE / readme).read_text(encoding="utf-8")
    return "\n".join(lines) + "\n\n" + description


def _library(os_name: str) -> pathlib.Path:
    name = LIBRARY_NAMES[os_name]
    configured = os.environ.get("ORKIDEA_ADBC_LIBRARY")
    library = pathlib.Path(configured) if configured else ROOT / "target" / "release" / name
    if not library.is_file():
        hint = "" if configured else " (build it with `cargo build --release -p orkidea-adbc-ffi`)"
        raise BuildError(f"driver library {library} not found{hint}")
    if library.suffix != pathlib.PurePath(name).suffix:
        raise BuildError(f"{library.name} is not a {os_name} driver library")
    return library


def _license_files() -> list[tuple[str, pathlib.Path]]:
    """License files, as (path under .dist-info/licenses, source) pairs."""
    files = [("LICENSE", ROOT / "LICENSE")]
    third_party = os.environ.get("ORKIDEA_ADBC_THIRD_PARTY_LICENSES")
    if third_party:
        base = pathlib.Path(third_party)
        if not (base / "INDEX.txt").is_file():
            raise BuildError(f"{base} is not a THIRD_PARTY_LICENSES directory")
        for path in sorted(p for p in base.rglob("*") if p.is_file()):
            files.append((f"THIRD_PARTY_LICENSES/{path.relative_to(base).as_posix()}", path))
    return files


class _Wheel:
    """Writes a wheel with sorted entries, fixed timestamps, and a RECORD."""

    def __init__(self, path: pathlib.Path) -> None:
        self._zip = zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED)
        self._records: list[tuple[str, str, str]] = []
        epoch = int(os.environ.get("SOURCE_DATE_EPOCH", "315532800"))
        self._date_time = time.gmtime(max(epoch, 315532800))[:6]

    def add(self, name: str, data: bytes, executable: bool = False) -> None:
        info = zipfile.ZipInfo(name, self._date_time)
        info.compress_type = zipfile.ZIP_DEFLATED
        info.external_attr = (0o100755 if executable else 0o100644) << 16
        self._zip.writestr(info, data)
        digest = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=")
        self._records.append((name, f"sha256={digest.decode()}", str(len(data))))

    def close(self, record_name: str) -> None:
        out = io.StringIO()
        writer = csv.writer(out, lineterminator="\n")
        writer.writerows(self._records)
        writer.writerow((record_name, "", ""))
        info = zipfile.ZipInfo(record_name, self._date_time)
        info.compress_type = zipfile.ZIP_DEFLATED
        info.external_attr = 0o100644 << 16
        self._zip.writestr(info, out.getvalue())
        self._zip.close()


def build_wheel(wheel_directory, config_settings=None, metadata_directory=None):
    project = _project()
    version = _version()
    os_name, platform_tag = _platform()
    library = _library(os_name)
    licenses = _license_files()

    dist = project["name"].replace("-", "_")
    dist_info = f"{dist}-{version}.dist-info"
    tag = f"py3-none-{platform_tag}"
    filename = f"{dist}-{version}-{tag}.whl"

    sources = sorted(
        p
        for p in (HERE / PACKAGE).rglob("*")
        if p.is_file() and "__pycache__" not in p.parts and p.suffix != ".pyc"
    )
    wheel = _Wheel(pathlib.Path(wheel_directory) / filename)
    for path in sources:
        wheel.add(path.relative_to(HERE).as_posix(), path.read_bytes())
    wheel.add(f"{PACKAGE}/{LIBRARY_NAMES[os_name]}", library.read_bytes(), executable=True)
    for name, path in licenses:
        wheel.add(f"{dist_info}/licenses/{name}", path.read_bytes())
    metadata = _metadata(project, version, [name for name, _ in licenses])
    wheel.add(f"{dist_info}/METADATA", metadata.encode("utf-8"))
    wheel.add(
        f"{dist_info}/WHEEL",
        (
            "Wheel-Version: 1.0\n"
            "Generator: adbc-driver-orkidea build_backend\n"
            "Root-Is-Purelib: false\n"
            f"Tag: {tag}\n"
        ).encode(),
    )
    wheel.close(f"{dist_info}/RECORD")
    return filename


def get_requires_for_build_wheel(config_settings=None):
    return []


def build_sdist(sdist_directory, config_settings=None):
    raise BuildError(
        "adbc-driver-orkidea is distributed as wheels only; build the wheel "
        "with `python -m build --wheel` after building the Rust library"
    )
