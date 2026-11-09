#!/bin/sh
# Installs the Orkidea ADBC driver and registers it with the ADBC driver
# manager, so it can be loaded as driver "orkidea" from Python, DuckDB, Go,
# R, and other ADBC clients.
#
#   curl -fsSL https://github.com/Tnsor-Labs/orkidea-adbc/releases/latest/download/install.sh | sh
#   sh install.sh --version v0.2.0
#   sh install.sh --archive orkidea-adbc-0.2.0-linux_amd64.tar.gz
#   sh install.sh --system        # all users (/etc/adbc/drivers); needs root
#
# Downloads are verified against the release's SHA256SUMS. When cosign is
# installed, SHA256SUMS is also verified against its Sigstore signature.
set -eu

REPO="Tnsor-Labs/orkidea-adbc"
version="latest"
archive=""
scope="user"

die() { echo "install.sh: $*" >&2; exit 1; }
say() { echo "install.sh: $*"; }

while [ $# -gt 0 ]; do
    case "$1" in
        --version) version="${2:?--version needs a value}"; shift 2 ;;
        --archive) archive="${2:?--archive needs a path}"; shift 2 ;;
        --system) scope="system"; shift ;;
        -h|--help) sed -n '2,13p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) die "unknown argument $1 (see --help)" ;;
    esac
done

case "$(uname -s)" in
    Linux) os="linux"; library="libadbc_driver_orkidea.so" ;;
    Darwin) os="macos"; library="libadbc_driver_orkidea.dylib" ;;
    *) die "unsupported OS $(uname -s); on Windows use install.ps1" ;;
esac
case "$(uname -m)" in
    x86_64|amd64) arch="amd64" ;;
    aarch64|arm64) arch="arm64" ;;
    *) die "unsupported architecture $(uname -m)" ;;
esac
platform="${os}_${arch}"

if [ "$os" = "linux" ]; then
    glibc="$(getconf GNU_LIBC_VERSION 2>/dev/null | awk '{print $2}')" || true
    if [ -z "$glibc" ]; then
        die "the Linux build needs glibc 2.28 or newer (musl is not supported)"
    fi
fi

fetch() { # url destination
    if command -v curl >/dev/null 2>&1; then
        curl -fsSL --retry 3 -o "$2" "$1"
    elif command -v wget >/dev/null 2>&1; then
        wget -q -O "$2" "$1"
    else
        die "curl or wget is required"
    fi
}

sha256() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | awk '{print $1}'
    else
        shasum -a 256 "$1" | awk '{print $1}'
    fi
}

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT INT TERM

if [ -n "$archive" ]; then
    [ -f "$archive" ] || die "archive $archive not found"
    asset="$(basename "$archive")"
    cp "$archive" "$work/$asset"
    sums="$(dirname "$archive")/SHA256SUMS"
    if [ -f "$sums" ]; then
        cp "$sums" "$work/SHA256SUMS"
    else
        say "no SHA256SUMS next to $archive; skipping checksum verification"
    fi
else
    if [ "$version" = "latest" ]; then
        api="https://api.github.com/repos/$REPO/releases/latest"
        fetch "$api" "$work/release.json"
        version="$(sed -n 's/.*"tag_name": *"\([^"]*\)".*/\1/p' "$work/release.json" | head -n 1)"
        [ -n "$version" ] || die "could not determine the latest release"
    fi
    number="${version#v}"
    asset="orkidea-adbc-${number}-${platform}.tar.gz"
    base="https://github.com/$REPO/releases/download/v${number}"
    say "downloading $asset"
    fetch "$base/$asset" "$work/$asset"
    fetch "$base/SHA256SUMS" "$work/SHA256SUMS"
    if command -v cosign >/dev/null 2>&1; then
        fetch "$base/SHA256SUMS.sigstore.json" "$work/SHA256SUMS.sigstore.json"
        cosign verify-blob \
            --bundle "$work/SHA256SUMS.sigstore.json" \
            --certificate-identity-regexp "^https://github.com/$REPO/\\.github/workflows/release\\.yml@refs/tags/v" \
            --certificate-oidc-issuer https://token.actions.githubusercontent.com \
            "$work/SHA256SUMS" >/dev/null 2>&1 || die "SHA256SUMS signature verification failed"
        say "verified the Sigstore signature of SHA256SUMS"
    fi
fi

if [ -f "$work/SHA256SUMS" ]; then
    expected="$(awk -v a="$asset" '$2 == a || $2 == "*"a {print $1}' "$work/SHA256SUMS")"
    [ -n "$expected" ] || die "$asset is not listed in SHA256SUMS"
    [ "$(sha256 "$work/$asset")" = "$expected" ] || die "checksum mismatch for $asset"
    say "verified the SHA-256 checksum of $asset"
fi

case "$asset" in
    orkidea-adbc-*-"$platform".tar.gz) ;;
    *) die "$asset is not a $platform archive" ;;
esac
number="${asset#orkidea-adbc-}"
number="${number%-"$platform".tar.gz}"

if [ "$scope" = "system" ]; then
    if [ "$os" = "macos" ]; then
        data_root="/Library/Application Support/orkidea-adbc"
        manifest_dir="/Library/Application Support/ADBC/Drivers"
    else
        data_root="/usr/local/lib/orkidea-adbc"
        manifest_dir="/etc/adbc/drivers"
    fi
elif [ "$os" = "macos" ]; then
    data_root="$HOME/Library/Application Support/orkidea-adbc"
    manifest_dir="$HOME/Library/Application Support/ADBC/Drivers"
else
    data_root="${XDG_DATA_HOME:-$HOME/.local/share}/orkidea-adbc"
    manifest_dir="${XDG_CONFIG_HOME:-$HOME/.config}/adbc/drivers"
fi

install_dir="$data_root/$number"
mkdir -p "$install_dir" "$manifest_dir"
tar -xzf "$work/$asset" -C "$work"
rm -rf "$install_dir"
mv "$work/orkidea-adbc-${number}-${platform}" "$install_dir"
lib_path="$install_dir/lib/$library"
[ -f "$lib_path" ] || die "the archive does not contain lib/$library"
case "$lib_path" in
    *"'"*) die "install path must not contain a single quote: $lib_path" ;;
esac

# TOML literal strings ('...') take the path verbatim.
cat > "$manifest_dir/orkidea.toml" <<EOF
manifest_version = 1
name = "orkidea"
version = "$number"

[Driver]
entrypoint = "OrkideaDriverInit"

[Driver.shared]
$platform = '$lib_path'
EOF

say "installed Orkidea ADBC $number to $install_dir"
say "registered driver \"orkidea\" in $manifest_dir/orkidea.toml"
