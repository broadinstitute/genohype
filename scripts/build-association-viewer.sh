#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ui_dir="$repo_root/ui"
app_dir="$ui_dir/apps/association-viewer"
embedded_dir="$repo_root/cli/static/association-viewer/dist"
check=false
install=true

usage() {
    echo "usage: $0 [--check] [--skip-install]" >&2
}

while (($#)); do
    case "$1" in
        --check) check=true ;;
        --skip-install) install=false ;;
        -h|--help) usage; exit 0 ;;
        *) echo "error: unknown argument: $1" >&2; usage; exit 2 ;;
    esac
    shift
done

if ! command -v npm >/dev/null 2>&1; then
    echo "error: npm is required to build the association viewer" >&2
    exit 1
fi
if [[ ! -f "$ui_dir/package-lock.json" || ! -f "$app_dir/package.json" ]]; then
    echo "error: association viewer sources or ui/package-lock.json are missing" >&2
    exit 1
fi

if $install; then
    echo "Installing UI dependencies from ui/package-lock.json..."
    (cd "$ui_dir" && npm ci)
fi

(
    cd "$ui_dir"
    npm run build --workspace @genohype/association-viz
    npm run build --workspace @genohype/association-viewer
)

validate_dist() {
    local dist="$1"
    local index="$dist/index.html"
    if [[ ! -f "$index" ]]; then
        echo "error: association viewer build did not produce dist/index.html" >&2
        exit 1
    fi
    if ! grep -Eq '(src|href)="\./assets/[^\"]+-[A-Za-z0-9_-]{8,}\.(js|css)"' "$index"; then
        echo "error: association viewer index lacks relative checksum-addressed assets" >&2
        exit 1
    fi
    if [[ ! -d "$dist/assets" ]] || ! find "$dist/assets" -type f | grep -Eq '/[^/]+-[A-Za-z0-9_-]{8,}\.(js|css)$'; then
        echo "error: association viewer build lacks checksum-addressed JS/CSS assets" >&2
        exit 1
    fi
}

validate_dist "$app_dir/dist"
if $check; then
    if [[ ! -d "$embedded_dir" ]]; then
        echo "error: committed association viewer assets are missing; run $0" >&2
        exit 1
    fi
    validate_dist "$embedded_dir"
    if ! diff -qr "$app_dir/dist" "$embedded_dir"; then
        echo "error: committed association viewer assets are stale; run $0 and commit cli/static/association-viewer/dist" >&2
        exit 1
    fi
    echo "Committed association viewer assets exactly match a fresh production build."
else
    rm -rf "$embedded_dir"
    mkdir -p "$(dirname "$embedded_dir")"
    cp -R "$app_dir/dist" "$embedded_dir"
    validate_dist "$embedded_dir"
    echo "Association viewer is ready for embedding."
fi
