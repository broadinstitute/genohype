#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ui_dir="$repo_root/ui"
app_dir="$ui_dir/apps/association-viewer"
embedded_dir="$repo_root/cli/static/association-viewer/dist"

if ! command -v npm >/dev/null 2>&1; then
    echo "error: npm is required to build the association viewer" >&2
    exit 1
fi
if [[ ! -f "$ui_dir/package-lock.json" || ! -f "$app_dir/package.json" ]]; then
    echo "error: Phase 2 association viewer sources or ui/package-lock.json are missing" >&2
    exit 1
fi

echo "Installing UI dependencies from ui/package-lock.json..."
(
    cd "$ui_dir"
    npm ci
    npm run build --workspace @genohype/association-viz
    npm run build --workspace @genohype/association-viewer
)

rm -rf "$embedded_dir"
mkdir -p "$(dirname "$embedded_dir")"
cp -R "$app_dir/dist" "$embedded_dir"

index="$embedded_dir/index.html"
if [[ ! -f "$index" ]]; then
    echo "error: association viewer build did not produce dist/index.html" >&2
    exit 1
fi
if ! grep -Eq '(src|href)="\./assets/[^\"]+-[A-Za-z0-9_-]{8,}\.(js|css)"' "$index"; then
    echo "error: association viewer index lacks relative checksum-addressed assets" >&2
    exit 1
fi
if ! find "$embedded_dir/assets" -type f | grep -Eq '/[^/]+-[A-Za-z0-9_-]{8,}\.(js|css)$'; then
    echo "error: association viewer build lacks checksum-addressed JS/CSS assets" >&2
    exit 1
fi

echo "Association viewer is ready for embedding."
