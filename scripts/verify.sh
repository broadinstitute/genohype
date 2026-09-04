#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

./scripts/test-release.sh
./scripts/build-dashboard.sh
./scripts/build-association-viewer.sh
(
  cd ui
  npm run build
  npm run typecheck
  npm test
)

cargo metadata --locked --format-version 1 >/dev/null
cargo fmt --all --check
cargo test --workspace --locked
cargo test --workspace --all-features --locked
cargo check --workspace --all-features --locked
cargo build --release --locked

target_dir="${CARGO_TARGET_DIR:-$repo_root/target}"
if [[ "$target_dir" != /* ]]; then
  target_dir="$repo_root/$target_dir"
fi
release_binary="$target_dir/release/genohype"
"$release_binary" --version
"$release_binary" --help >/dev/null
./scripts/test-association-viewer-package.sh "$release_binary"
(
  cd ui
  GENOHYPE_E2E_BINARY="$release_binary" npm run test:e2e
)
