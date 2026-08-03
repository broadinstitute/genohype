#!/usr/bin/env bash
# One-worker attested custom bootstrap and lifecycle-free exact-job acceptance.
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# Resolved relative to this script at runtime.
# shellcheck disable=SC1091
source "$SCRIPT_DIR/lib/snapshots.sh"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/lib/nested-pool.sh"

mode="${1:-run}"
shift || true

capability_preflight() {
  local missing=() help
  help="$(genohype pool create --help 2>&1 || true)"
  grep -q -- '--worker-attestation' <<<"$help" || missing+=("pool create --worker-attestation")
  help="$(genohype pool scale --help 2>&1 || true)"
  grep -q -- '--worker-attestation' <<<"$help" || missing+=("pool scale --worker-attestation")
  help="$(genohype pool submit --help 2>&1 || true)"
  grep -q -- '--detach' <<<"$help" || missing+=("pool submit --detach")
  grep -q -- '--output' <<<"$help" || missing+=("pool submit --output json")
  help="$(genohype pool receipts --help 2>&1 || true)"
  grep -q -- '--job-id' <<<"$help" || missing+=("pool receipts --job-id")
  if ((${#missing[@]})); then
    echo "pool-lifecycle capability preflight failed before pool mutation" >&2
    printf 'Missing product dependency: %s\n' "${missing[@]}" >&2
    return 3
  fi
  echo "POOL_LIFECYCLE_CAPABILITIES_OK"
}

if [[ "$mode" == preflight ]]; then
  capability_preflight
  exit
fi
[[ "$mode" == run && $# -eq 7 ]] || {
  echo "usage: $0 preflight | run <project> <zone> <config> <pool> <worker-sa> <pool-marker> <results-dir>" >&2
  exit 2
}
PROJECT="$1"
ZONE="$2"
CONFIG="$3"
POOL="$4"
WORKER_SA="$5"
POOL_MARKER="$6"
RESULTS_DIR="$7"
RELEASE_DIR="/tmp/genohype-e2e-release"
FIXTURE_A="$RELEASE_DIR/custom-worker/fixture-a"
ATTESTATION_A="$FIXTURE_A.attestation.json"
QUICK_MANIFEST="$RELEASE_DIR/custom-worker/manifests/quick.json"
LIFECYCLE_RESULTS="$RESULTS_DIR/lifecycle"
TUNNEL_PID=""

cleanup_tunnel() {
  if [[ -n "$TUNNEL_PID" ]]; then
    kill "$TUNNEL_PID" >/dev/null 2>&1 || true
    wait "$TUNNEL_PID" >/dev/null 2>&1 || true
  fi
}
trap cleanup_tunnel EXIT

wait_exact_readiness() {
  local label="$1" expected_build="$2" consecutive=0 deadline
  if [[ -z "$TUNNEL_PID" ]]; then
    gcloud compute ssh "${POOL}-coordinator" --project "$PROJECT" --zone "$ZONE" \
      --tunnel-through-iap --quiet -- -N -L 13001:localhost:3000 \
      >"$LIFECYCLE_RESULTS/readiness-tunnel.log" 2>&1 &
    TUNNEL_PID=$!
  fi
  deadline=$((SECONDS + 180))
  while ((SECONDS < deadline)); do
    if curl -fsS http://localhost:13001/api/dashboard/workers \
      >"$LIFECYCLE_RESULTS/readiness-${label}.json" \
      && jq -e --arg worker "${POOL}-worker-0" --arg build "$expected_build" '
        length == 1 and .[0].worker_id == $worker and
        .[0].build_version == $build and
        (.[0].status == "active" or .[0].status == "idle") and
        .[0].last_seen_secs < 15
      ' "$LIFECYCLE_RESULTS/readiness-${label}.json" >/dev/null; then
      ((consecutive += 1))
      ((consecutive >= 2)) && return 0
    else
      consecutive=0
    fi
    sleep 3
  done
  echo "worker did not reach two consecutive exact readiness observations" >&2
  return 1
}

capability_preflight
mkdir -p "$LIFECYCLE_RESULTS"
for path in "$FIXTURE_A" "$ATTESTATION_A" "$QUICK_MANIFEST"; do
  [[ -f "$path" ]] || {
    echo "missing host-built custom-worker artifact: $path" >&2
    exit 1
  }
done
[[ -x "$FIXTURE_A" ]]
expected_sha="$(jq -er '.sha256' "$ATTESTATION_A")"
expected_build="$(jq -er '.build_version' "$ATTESTATION_A")"
expected_revision="$(jq -er '.revision' "$ATTESTATION_A")"
[[ "$(sha256sum "$FIXTURE_A" | awk '{print $1}')" == "$expected_sha" ]]
"$FIXTURE_A" --identity-json | tee "$LIFECYCLE_RESULTS/fixture-identity.json" \
  | jq -e --arg build "$expected_build" --arg revision "$expected_revision" \
      '.build_id == "fixture-a" and .build_identity == $build and
       .build_version == $build and .revision == $revision and .platform == "linux" and
       .architecture == "x86_64"'

printf '%s\n' '--- local core smoke on clean install ---'
genohype info /tmp/tiny-keyed.ht --json \
  | tee "$LIFECYCLE_RESULTS/core-info.json" \
  | jq -e '.partitions == 1 and .key_fields == ["gene_id", "chrom", "start"]'
genohype query /tmp/tiny-keyed.ht --json >"$LIFECYCLE_RESULTS/tiny.ndjson"
test "$(wc -l <"$LIFECYCLE_RESULTS/tiny.ndjson" | tr -d ' ')" = 2
genohype export parquet /tmp/tiny-keyed.ht "$LIFECYCLE_RESULTS/tiny.parquet"
file "$LIFECYCLE_RESULTS/tiny.parquet"

# Mark before create so every partial/interrupted nested pool is cleaned.
printf '%s\n' "$POOL" >"$POOL_MARKER"
printf '%s\n' '--- coordinator-only create ---'
timeout --foreground 900s genohype --config "$CONFIG" pool create "$POOL" \
  --workers 0 --with-coordinator --wait --skip-build \
  --worker-binary "$FIXTURE_A" --worker-attestation "$ATTESTATION_A"
instances="$(gcloud compute instances list --project "$PROJECT" \
  --filter="tags.items:pool-${POOL}" --format='value(name)' | sort)"
[[ "$instances" == "${POOL}-coordinator" ]]
COORDINATOR_SA="$(awk -F'"' '/^coordinator_service_account = / {print $2}' "$CONFIG")"
gcloud compute instances describe "${POOL}-coordinator" --project "$PROJECT" \
  --zone "$ZONE" --format=json >"$LIFECYCLE_RESULTS/coordinator.json"
jq -e --arg sa "$COORDINATOR_SA" '
  .status == "RUNNING" and .serviceAccounts[0].email == $sa and
  ((.networkInterfaces[0].accessConfigs // []) | length == 0)
' "$LIFECYCLE_RESULTS/coordinator.json" >/dev/null

echo '--- attested scale 0 to 1 ---'
timeout --foreground 900s genohype --config "$CONFIG" pool scale "$POOL" --workers 1 \
  --skip-build --worker-binary "$FIXTURE_A" --worker-attestation "$ATTESTATION_A"
gcloud compute instances describe "${POOL}-worker-0" --project "$PROJECT" \
  --zone "$ZONE" --format=json >"$LIFECYCLE_RESULTS/worker.json"
jq -e --arg sa "$WORKER_SA" '
  .status == "RUNNING" and .serviceAccounts[0].email == $sa and
  ((.networkInterfaces[0].accessConfigs // []) | length == 0)
' "$LIFECYCLE_RESULTS/worker.json" >/dev/null
wait_exact_readiness initial "$expected_build"
capture_worker_snapshot "$PROJECT" "$ZONE" "$POOL" "$LIFECYCLE_RESULTS/warm-before.json"
jq -e --arg sha "$expected_sha" --arg build "$expected_build" '
  .service.ActiveState == "active" and .service.SubState == "running" and
  .service.MainPID > 0 and .process.installed_sha256 == $sha and
  .deployment.sha256 == $sha and .deployment.build_version == $build and
  (.deployment.deployment_id | type == "string" and length > 0)
' "$LIFECYCLE_RESULTS/warm-before.json" >/dev/null
ARTIFACT_URL="$(jq -er '.deployment.artifact_url | select(startswith("gs://"))' \
  "$LIFECYCLE_RESULTS/warm-before.json")"
capture_gcs_object_snapshot "$LIFECYCLE_RESULTS/artifacts-before.json" \
  "$ARTIFACT_URL" "$ARTIFACT_URL.attestation.json"

echo '--- detached submit and exact durable receipts ---'
ack_start=$SECONDS
if ! genohype --config "$CONFIG" pool submit "$POOL" --detach --output json \
  --batch-size 1 -- custom --manifest "$QUICK_MANIFEST" \
  --payload '{"suite":"genohype-e2e"}' \
  >"$LIFECYCLE_RESULTS/submit-ack.json" \
  2>"$LIFECYCLE_RESULTS/submit-stderr.log"; then
  cat "$LIFECYCLE_RESULTS/submit-stderr.log" >&2
  exit 1
fi
((SECONDS - ack_start < 30)) || {
  echo "submit acknowledgement exceeded 30 seconds" >&2
  exit 1
}
# Machine mode is an exact one-document stdout contract. Parse all stdout so
# any leaked diagnostic or journal line fails rather than being discarded.
jq -s -e 'length == 1 and (.[0] | type == "object")' \
  "$LIFECYCLE_RESULTS/submit-ack.json" >/dev/null
cat "$LIFECYCLE_RESULTS/submit-ack.json"
JOB_ID="$(jq -er 'select(.acknowledged == true) | .job_id |
  select(type == "string" and length > 0)' "$LIFECYCLE_RESULTS/submit-ack.json")"
printf '%s\n' "$JOB_ID" >"$LIFECYCLE_RESULTS/job-id.txt"

# Poll only the acknowledged exact ID. Receipt authority is durable SQLite,
# never current-job inference or journal text.
deadline=$((SECONDS + 300))
while :; do
  genohype --config "$CONFIG" pool receipts "$POOL" --job-id "$JOB_ID" \
    >"$LIFECYCLE_RESULTS/receipts.tmp.json"
  if jq -e --arg id "$JOB_ID" '
      .job_id == $id and .job_found == true and .job_status == "completed" and .complete == true
    ' "$LIFECYCLE_RESULTS/receipts.tmp.json" >/dev/null; then
    mv "$LIFECYCLE_RESULTS/receipts.tmp.json" "$LIFECYCLE_RESULTS/receipts.json"
    break
  fi
  ((SECONDS < deadline)) || {
    echo "exact custom job did not complete within 300 seconds" >&2
    exit 1
  }
  sleep 2
done
jq -e --arg id "$JOB_ID" --arg build "$expected_build" --arg revision "$expected_revision" '
  .job_id == $id and .expected_task_count == 2 and .accepted_count == 2 and
  .failed_attempt_count == 0 and .terminal_receipt_count == 2 and
  (.canonical_sha256 | test("^[0-9a-f]{64}$")) and
  ([.receipts[].worker_build_version] | all(. == $build)) and
  ([.receipts[].report.result_json.fixture_identity.revision] | all(. == $revision))
' "$LIFECYCLE_RESULTS/receipts.json" >/dev/null

capture_worker_snapshot "$PROJECT" "$ZONE" "$POOL" "$LIFECYCLE_RESULTS/warm-after.json"
capture_gcs_object_snapshot "$LIFECYCLE_RESULTS/artifacts-after.json" \
  "$ARTIFACT_URL" "$ARTIFACT_URL.attestation.json"
assert_snapshot_unchanged "$LIFECYCLE_RESULTS/warm-before.json" \
  "$LIFECYCLE_RESULTS/warm-after.json"
cmp -s "$LIFECYCLE_RESULTS/artifacts-before.json" "$LIFECYCLE_RESULTS/artifacts-after.json" || {
  echo "custom worker artifact generation changed during normal submit" >&2
  diff -u "$LIFECYCLE_RESULTS/artifacts-before.json" \
    "$LIFECYCLE_RESULTS/artifacts-after.json" >&2 || true
  exit 1
}

echo '--- scale 1 to 0 to 1 with reused name and fresh generation ---'
timeout --foreground 600s genohype --config "$CONFIG" pool scale "$POOL" --workers 0 --skip-build
if gcloud compute instances describe "${POOL}-worker-0" --project "$PROJECT" \
  --zone "$ZONE" >/dev/null 2>&1; then
  echo "worker remains after scale to zero" >&2
  exit 1
fi
timeout --foreground 900s genohype --config "$CONFIG" pool scale "$POOL" --workers 1 \
  --skip-build --worker-binary "$FIXTURE_A" --worker-attestation "$ATTESTATION_A"
wait_exact_readiness recreated "$expected_build"
capture_worker_snapshot "$PROJECT" "$ZONE" "$POOL" "$LIFECYCLE_RESULTS/recreated.json"
capture_gcs_object_snapshot "$LIFECYCLE_RESULTS/artifacts-after-recreate.json" \
  "$ARTIFACT_URL" "$ARTIFACT_URL.attestation.json"
cmp -s "$LIFECYCLE_RESULTS/artifacts-before.json" \
  "$LIFECYCLE_RESULTS/artifacts-after-recreate.json" || {
  echo "content-addressed artifact was not reused across worker recreation" >&2
  exit 1
}
assert_recreated_generation "$LIFECYCLE_RESULTS/warm-before.json" \
  "$LIFECYCLE_RESULTS/recreated.json"
jq -e --arg sha "$expected_sha" --arg build "$expected_build" '
  .process.installed_sha256 == $sha and .deployment.sha256 == $sha and
  .deployment.build_version == $build
' "$LIFECYCLE_RESULTS/recreated.json" >/dev/null

echo '--- guarded destroy and stable-empty nested leak checks ---'
destroy_nested_pool_guarded "$PROJECT" "$ZONE" "$POOL" "$CONFIG" \
  "$POOL_MARKER" "$(command -v genohype)"

echo "POOL_LIFECYCLE_OK job_id=$JOB_ID"
