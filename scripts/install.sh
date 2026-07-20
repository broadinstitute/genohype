#!/bin/sh
set -eu

repository="${GENOHYPE_REPOSITORY:-broadinstitute/genohype}"
install_dir="${GENOHYPE_INSTALL_DIR:-$HOME/.local/bin}"
requested_version="${1:-${GENOHYPE_VERSION:-}}"

if [ "${1:-}" = "--help" ] || [ "${1:-}" = "-h" ]; then
    cat <<'USAGE'
Install a prebuilt Genohype release.

Usage:
  install.sh [vX.Y.Z]

Environment:
  GENOHYPE_VERSION       Release to install when no argument is supplied
  GENOHYPE_INSTALL_DIR   Destination directory (default: $HOME/.local/bin)
USAGE
    exit 0
fi

if [ "$#" -gt 1 ]; then
    echo "error: expected at most one version argument" >&2
    exit 1
fi

require_command() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "error: $1 is required" >&2
        exit 1
    fi
}

require_command curl
require_command tar
require_command uname
require_command mktemp

if [ -z "$requested_version" ]; then
    latest_url="$(curl -fsSL -o /dev/null -w '%{url_effective}' \
        "https://github.com/$repository/releases/latest")"
    latest_url="${latest_url%/}"
    requested_version="${latest_url##*/}"
fi

case "$requested_version" in
    v*) tag="$requested_version" ;;
    *) tag="v$requested_version" ;;
esac

if ! printf '%s\n' "$tag" | grep -Eq '^v[0-9]+\.[0-9]+\.[0-9]+$'; then
    echo "error: '$tag' is not a stable release tag such as v0.1.0" >&2
    exit 1
fi

case "$(uname -s)" in
    Darwin) os="apple-darwin" ;;
    Linux) os="unknown-linux-gnu" ;;
    *)
        echo "error: Genohype releases support macOS and Linux" >&2
        exit 1
        ;;
esac

case "$(uname -m)" in
    arm64|aarch64) arch="aarch64" ;;
    x86_64|amd64) arch="x86_64" ;;
    *)
        echo "error: unsupported architecture: $(uname -m)" >&2
        exit 1
        ;;
esac

target="$arch-$os"
case "$target" in
    aarch64-apple-darwin|x86_64-apple-darwin|x86_64-unknown-linux-gnu) ;;
    *)
        echo "error: no Genohype release is published for $target" >&2
        exit 1
        ;;
esac

release_base_url="${GENOHYPE_RELEASE_BASE_URL:-https://github.com/$repository/releases/download/$tag}"
archive="genohype-${tag}-${target}.tar.gz"
worker_archive="genohype-worker-${tag}-x86_64-unknown-linux-gnu.tar.gz"

temp_dir="$(mktemp -d "${TMPDIR:-/tmp}/genohype-install.XXXXXX")"
trap 'rm -rf "$temp_dir"' EXIT HUP INT TERM

printf 'Downloading Genohype %s for %s...\n' "$tag" "$target"
curl -fsSL --retry 3 "$release_base_url/SHA256SUMS" -o "$temp_dir/SHA256SUMS"
curl -fsSL --retry 3 "$release_base_url/$archive" -o "$temp_dir/$archive"

# macOS clients also need the Linux binary used by `genohype pool`.
if [ "$os" = "apple-darwin" ]; then
    curl -fsSL --retry 3 "$release_base_url/$worker_archive" -o "$temp_dir/$worker_archive"
fi

checksum_file() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | awk '{print $1}'
    elif command -v shasum >/dev/null 2>&1; then
        shasum -a 256 "$1" | awk '{print $1}'
    else
        echo "error: sha256sum or shasum is required" >&2
        exit 1
    fi
}

verify_archive() {
    archive_name="$1"
    expected="$(awk -v name="$archive_name" '$2 == name {print $1; exit}' "$temp_dir/SHA256SUMS")"
    if [ -z "$expected" ]; then
        echo "error: $archive_name is missing from SHA256SUMS" >&2
        exit 1
    fi

    actual="$(checksum_file "$temp_dir/$archive_name")"
    if [ "$actual" != "$expected" ]; then
        echo "error: checksum verification failed for $archive_name" >&2
        exit 1
    fi
}

verify_archive "$archive"
if [ "$os" = "apple-darwin" ]; then
    verify_archive "$worker_archive"
fi

mkdir -p "$temp_dir/main"
tar -xzf "$temp_dir/$archive" -C "$temp_dir/main"
if [ ! -f "$temp_dir/main/genohype" ]; then
    echo "error: release archive does not contain genohype" >&2
    exit 1
fi

mkdir -p "$install_dir"
main_temp="$install_dir/.genohype.$$"
cp "$temp_dir/main/genohype" "$main_temp"
chmod 0755 "$main_temp"
mv -f "$main_temp" "$install_dir/genohype"

if [ "$os" = "apple-darwin" ]; then
    mkdir -p "$temp_dir/worker"
    tar -xzf "$temp_dir/$worker_archive" -C "$temp_dir/worker"
    if [ ! -f "$temp_dir/worker/genohype-worker" ]; then
        echo "error: worker archive does not contain genohype-worker" >&2
        exit 1
    fi

    worker_temp="$install_dir/.genohype-worker.$$"
    cp "$temp_dir/worker/genohype-worker" "$worker_temp"
    chmod 0755 "$worker_temp"
    mv -f "$worker_temp" "$install_dir/genohype-worker"
else
    rm -f "$install_dir/genohype-worker"
    ln -s genohype "$install_dir/genohype-worker"
fi

printf 'Installed %s\n' "$install_dir/genohype"
"$install_dir/genohype" --version

case ":${PATH:-}:" in
    *":$install_dir:"*) ;;
    *)
        printf '\n%s is not on PATH. Add this line to your shell profile:\n' "$install_dir"
        printf '  export PATH="%s:%s"\n' "$install_dir" "\$PATH"
        ;;
esac
