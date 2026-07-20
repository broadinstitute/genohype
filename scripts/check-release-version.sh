#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

tag="${1:-}"
if [[ ! "$tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "error: expected a stable release tag such as v0.1.0" >&2
    exit 1
fi

version="${tag#v}"
release_notes="release-notes/${tag}.md"

if [[ ! -s "$release_notes" ]]; then
    echo "error: $release_notes must exist and contain release notes" >&2
    exit 1
fi

cargo metadata --locked --no-deps --format-version 1 | python3 -c '
import json
import sys

expected = sys.argv[1]
metadata = json.load(sys.stdin)
packages = [
    package
    for package in metadata["packages"]
    if package["name"].startswith("genohype-")
]

if not packages:
    print("error: cargo metadata did not contain Genohype packages", file=sys.stderr)
    raise SystemExit(1)

mismatches = [
    "{}: {}".format(package["name"], package["version"])
    for package in packages
    if package["version"] != expected
]
if mismatches:
    print(
        f"error: tag v{expected} does not match every Genohype package:\n  "
        + "\n  ".join(mismatches),
        file=sys.stderr,
    )
    raise SystemExit(1)

print(f"Release version v{expected} matches {len(packages)} workspace packages.")
' "$version"

printf 'Release notes: %s\n' "$release_notes"
