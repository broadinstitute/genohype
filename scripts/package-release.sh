#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

if [[ $# -ne 3 ]]; then
    echo "usage: $0 <vX.Y.Z> <binary-artifact-dir> <output-dir>" >&2
    exit 1
fi

tag="$1"
artifact_dir="$(cd "$2" && pwd)"
output_dir="$3"

if [[ ! "$tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "error: expected a stable release tag such as v0.1.0" >&2
    exit 1
fi

mkdir -p "$output_dir"
output_dir="$(cd "$output_dir" && pwd)"
rm -f "$output_dir"/*.tar.gz "$output_dir/SHA256SUMS"

stage_root="$(mktemp -d "${TMPDIR:-/tmp}/genohype-package.XXXXXX")"
trap 'rm -rf "$stage_root"' EXIT

targets=(
    aarch64-apple-darwin
    x86_64-apple-darwin
    x86_64-unknown-linux-gnu
)

for target in "${targets[@]}"; do
    source_binary="$artifact_dir/genohype-$target/genohype"
    if [[ ! -f "$source_binary" ]]; then
        echo "error: missing build artifact $source_binary" >&2
        exit 1
    fi

    package_dir="$stage_root/$target"
    mkdir -p "$package_dir"
    cp "$source_binary" "$package_dir/genohype"
    cp LICENSE "$package_dir/LICENSE"
    chmod 0755 "$package_dir/genohype"

    archive="genohype-${tag}-${target}.tar.gz"
    tar -czf "$output_dir/$archive" -C "$package_dir" genohype LICENSE
    printf 'Created %s\n' "$archive"
done

linux_binary="$artifact_dir/genohype-x86_64-unknown-linux-gnu/genohype"
worker_dir="$stage_root/worker"
mkdir -p "$worker_dir"
cp "$linux_binary" "$worker_dir/genohype-worker"
cp LICENSE "$worker_dir/LICENSE"
chmod 0755 "$worker_dir/genohype-worker"
worker_archive="genohype-worker-${tag}-x86_64-unknown-linux-gnu.tar.gz"
tar -czf "$output_dir/$worker_archive" -C "$worker_dir" genohype-worker LICENSE
printf 'Created %s\n' "$worker_archive"

checksum() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | awk '{print $1}'
    else
        shasum -a 256 "$1" | awk '{print $1}'
    fi
}

for archive_path in "$output_dir"/*.tar.gz; do
    archive_name="$(basename "$archive_path")"
    printf '%s  %s\n' "$(checksum "$archive_path")" "$archive_name" >> "$output_dir/SHA256SUMS"
done

printf 'Created SHA256SUMS\n'
