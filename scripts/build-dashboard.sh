#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
dashboard_dir="$repo_root/frontend/pool-dashboard"
embedded_dir="$repo_root/cli/static/dist"

if ! command -v npm >/dev/null 2>&1; then
    echo "error: npm is required to build the pool dashboard" >&2
    exit 1
fi

echo "Installing pool-dashboard dependencies from package-lock.json..."
(
    cd "$dashboard_dir"
    npm ci
    npm run build
)

echo "Copying dashboard assets to cli/static/dist..."
rm -rf "$embedded_dir"
cp -R "$dashboard_dir/dist" "$embedded_dir"

echo "Pool dashboard is ready for embedding."
