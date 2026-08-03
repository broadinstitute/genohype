#!/usr/bin/env bash
# Cross-build every Linux binary used by the GCP harness on the host. Nothing
# emitted by this script requires a Rust toolchain or repository checkout on a guest.
set -Eeuo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
OUTPUT_DIR="${1:-$REPO_ROOT/target/e2e-gcp-artifacts}"
TARGET="x86_64-unknown-linux-gnu"
REVISION="$(git -C "$REPO_ROOT" rev-parse HEAD)"
EXPECTED_REVISION="${2:-$REVISION}"
ATTESTATION_TOOL="$REPO_ROOT/scripts/e2e/gcp/tools/make-attestation.py"
TARGET_RELEASE="$REPO_ROOT/target/$TARGET/release"
FIXTURE_DIR="$OUTPUT_DIR/custom-worker"
STAGE="$OUTPUT_DIR/.package-stage"

if [[ "$EXPECTED_REVISION" != "$REVISION" ]]; then
  echo "error: requested revision does not match the checked-out host revision" >&2
  exit 2
fi
for command in cargo git python3 tar; do
  command -v "$command" >/dev/null || {
    echo "error: $command is required" >&2
    exit 2
  }
done
command -v cargo-zigbuild >/dev/null || {
  echo "error: cargo-zigbuild is required" >&2
  exit 2
}

mkdir -p "$OUTPUT_DIR" "$FIXTURE_DIR"
rm -rf "$STAGE"
mkdir -p "$STAGE/main" "$STAGE/worker"

printf '%s\n' '--- build dashboard and main Linux CLI ---'
"$REPO_ROOT/scripts/build-dashboard.sh"
(
  cd "$REPO_ROOT"
  ulimit -n 16384 2>/dev/null || ulimit -n 8192 2>/dev/null || true
  cargo zigbuild --locked --target "$TARGET" --release \
    --package genohype-cli --bin genohype --features full
)
MAIN_BINARY="$TARGET_RELEASE/genohype"
test -x "$MAIN_BINARY"

cp "$MAIN_BINARY" "$STAGE/main/genohype"
cp "$MAIN_BINARY" "$STAGE/worker/genohype-worker"
cp "$REPO_ROOT/LICENSE" "$STAGE/main/LICENSE"
cp "$REPO_ROOT/LICENSE" "$STAGE/worker/LICENSE"
chmod 0755 "$STAGE/main/genohype" "$STAGE/worker/genohype-worker"
tar -czf "$OUTPUT_DIR/genohype-v0.0.0-$TARGET.tar.gz" \
  -C "$STAGE/main" genohype LICENSE
tar -czf "$OUTPUT_DIR/genohype-worker-v0.0.0-$TARGET.tar.gz" \
  -C "$STAGE/worker" genohype-worker LICENSE

check_identity_when_runnable() {
  local binary="$1"
  local build_identity="$2"
  local identity_json
  local -a runner

  if [[ "$(uname -s)" == Linux && "$(uname -m)" == x86_64 ]]; then
    identity_json="$("$binary" --identity-json)"
  elif [[ -n "${GENOHYPE_E2E_LINUX_RUNNER:-}" ]]; then
    read -r -a runner <<< "$GENOHYPE_E2E_LINUX_RUNNER"
    identity_json="$("${runner[@]}" "$binary" --identity-json)"
  else
    echo "identity execution skipped for $build_identity (set GENOHYPE_E2E_LINUX_RUNNER to enable)"
    return 0
  fi

  IDENTITY_JSON="$identity_json" python3 - "$REVISION" "$build_identity" <<'PY'
import json
import os
import sys

identity = json.loads(os.environ["IDENTITY_JSON"])
revision, build_identity = sys.argv[1], sys.argv[2]
expected = {
    "architecture": "x86_64",
    "build_id": build_identity,
    "build_identity": f"e2e:{revision}:{build_identity}",
    "build_version": f"e2e:{revision}:{build_identity}",
    "platform": "linux",
    "protocol_version": 1,
    "revision": revision,
    "schema_version": 1,
    "task_schema_version": 1,
}
if identity != expected:
    raise SystemExit(f"fixture identity mismatch: expected {expected!r}, got {identity!r}")
PY
}

build_fixture() {
  local build_identity="$1"
  local artifact="$FIXTURE_DIR/$build_identity"
  local sidecar="$artifact.attestation.json"

  printf '%s\n' "--- build Linux custom worker $build_identity ---"
  (
    cd "$REPO_ROOT"
    GENOHYPE_E2E_REVISION="$REVISION" \
      GENOHYPE_E2E_BUILD_ID="$build_identity" \
      cargo zigbuild --locked --target "$TARGET" --release \
        --package genohype-e2e-custom-worker --bin genohype-e2e-custom-worker
  )
  cp "$TARGET_RELEASE/genohype-e2e-custom-worker" "$artifact"
  chmod 0755 "$artifact"
  check_identity_when_runnable "$artifact" "$build_identity"
  python3 "$ATTESTATION_TOOL" create \
    --binary "$artifact" \
    --output "$sidecar" \
    --revision "$REVISION" \
    --build-identity "$build_identity"
  python3 "$ATTESTATION_TOOL" validate \
    --binary "$artifact" \
    --attestation "$sidecar" \
    --expected-revision "$REVISION" \
    --expected-build-identity "$build_identity"
}

build_fixture fixture-a
build_fixture fixture-b

rm -rf "$FIXTURE_DIR/manifests"
cp -R "$REPO_ROOT/scripts/e2e/gcp/fixtures/custom-worker/manifests" "$FIXTURE_DIR/"
for build_identity in fixture-a fixture-b; do
  fixture_stage="$STAGE/custom-$build_identity"
  mkdir -p "$fixture_stage/manifests"
  cp "$FIXTURE_DIR/$build_identity" "$fixture_stage/genohype-e2e-custom-worker"
  cp "$FIXTURE_DIR/$build_identity.attestation.json" "$fixture_stage/attestation.json"
  cp "$FIXTURE_DIR/manifests"/*.json "$fixture_stage/manifests/"
  tar -czf "$OUTPUT_DIR/genohype-e2e-custom-worker-$build_identity-$TARGET.tar.gz" \
    -C "$fixture_stage" genohype-e2e-custom-worker attestation.json manifests
done
rm -rf "$STAGE"

OUTPUT_DIR="$OUTPUT_DIR" python3 <<'PY'
import hashlib
import os
from pathlib import Path

root = Path(os.environ["OUTPUT_DIR"])
entries = []
for path in sorted(candidate for candidate in root.rglob("*") if candidate.is_file()):
    if path.name == "SHA256SUMS":
        continue
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    entries.append(f"{digest.hexdigest()}  {path.relative_to(root).as_posix()}\n")
(root / "SHA256SUMS").write_text("".join(entries), encoding="ascii")
PY

printf 'Host-built GCP E2E artifacts: %s\nRevision: %s\n' "$OUTPUT_DIR" "$REVISION"
