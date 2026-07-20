#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

temp_dir="$(mktemp -d "${TMPDIR:-/tmp}/genohype-release-test.XXXXXX")"
trap 'rm -rf "$temp_dir"' EXIT

for target in \
    aarch64-apple-darwin \
    x86_64-apple-darwin \
    x86_64-unknown-linux-gnu
do
    artifact_dir="$temp_dir/artifacts/genohype-$target"
    mkdir -p "$artifact_dir"
    cat > "$artifact_dir/genohype" <<'BINARY'
#!/bin/sh
if [ "${1:-}" = "--version" ]; then
    echo "genohype 0.1.0 (abcdef0)"
else
    echo "Genohype release test binary"
fi
BINARY
    chmod 0755 "$artifact_dir/genohype"
done

./scripts/package-release.sh \
    v0.1.0 \
    "$temp_dir/artifacts" \
    "$temp_dir/release"

expected_assets=(
    genohype-v0.1.0-aarch64-apple-darwin.tar.gz
    genohype-v0.1.0-x86_64-apple-darwin.tar.gz
    genohype-v0.1.0-x86_64-unknown-linux-gnu.tar.gz
    genohype-worker-v0.1.0-x86_64-unknown-linux-gnu.tar.gz
    SHA256SUMS
)
for asset in "${expected_assets[@]}"; do
    test -s "$temp_dir/release/$asset"
done

for target in \
    aarch64-apple-darwin \
    x86_64-apple-darwin \
    x86_64-unknown-linux-gnu
do
    archive_contents="$(tar -tzf "$temp_dir/release/genohype-v0.1.0-$target.tar.gz")"
    grep -qx genohype <<< "$archive_contents"
    grep -qx LICENSE <<< "$archive_contents"
done
worker_contents="$(tar -tzf "$temp_dir/release/genohype-worker-v0.1.0-x86_64-unknown-linux-gnu.tar.gz")"
grep -qx genohype-worker <<< "$worker_contents"
grep -qx LICENSE <<< "$worker_contents"

if command -v sha256sum >/dev/null 2>&1; then
    (cd "$temp_dir/release" && sha256sum --check SHA256SUMS)
else
    (cd "$temp_dir/release" && shasum -a 256 --check SHA256SUMS)
fi

GENOHYPE_VERSION=v0.1.0 \
GENOHYPE_RELEASE_BASE_URL="file://$temp_dir/release" \
GENOHYPE_INSTALL_DIR="$temp_dir/install" \
    sh ./scripts/install.sh

test -x "$temp_dir/install/genohype"
test -x "$temp_dir/install/genohype-worker"
test "$("$temp_dir/install/genohype" --version)" = "genohype 0.1.0 (abcdef0)"

# A modified archive must be rejected before installation.
cp -R "$temp_dir/release" "$temp_dir/tampered-release"
case "$(uname -s)-$(uname -m)" in
    Darwin-arm64|Darwin-aarch64)
        tampered_asset="genohype-v0.1.0-aarch64-apple-darwin.tar.gz"
        ;;
    Darwin-x86_64|Darwin-amd64)
        tampered_asset="genohype-v0.1.0-x86_64-apple-darwin.tar.gz"
        ;;
    Linux-x86_64|Linux-amd64)
        tampered_asset="genohype-v0.1.0-x86_64-unknown-linux-gnu.tar.gz"
        ;;
    *)
        echo "error: release installer test does not support this host" >&2
        exit 1
        ;;
esac
printf 'tampered\n' >> "$temp_dir/tampered-release/$tampered_asset"

if GENOHYPE_VERSION=v0.1.0 \
    GENOHYPE_RELEASE_BASE_URL="file://$temp_dir/tampered-release" \
    GENOHYPE_INSTALL_DIR="$temp_dir/tampered-install" \
    sh ./scripts/install.sh >/dev/null 2>&1
then
    echo "error: installer accepted an archive with the wrong checksum" >&2
    exit 1
fi

printf 'Release packaging and installer tests passed.\n'
