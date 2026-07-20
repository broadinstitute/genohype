#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

./scripts/build-dashboard.sh

cargo metadata --locked --format-version 1 >/dev/null
cargo fmt --all --check
cargo test --workspace --locked
cargo test --workspace --all-features --locked
cargo check --workspace --all-features --locked
cargo build --release --locked
