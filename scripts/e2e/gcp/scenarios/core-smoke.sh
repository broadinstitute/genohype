#!/usr/bin/env bash
# Current-main clean install, core checks, and one-stock-worker pool smoke.
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/lib/nested-pool.sh"

[[ $# -eq 10 ]] || {
  echo "usage: $0 <project> <zone> <bucket> <config> <pool> <coordinator-sa> <worker-sa> <input-uri> <output-uri> <pool-marker>" >&2
  exit 2
}
PROJECT="$1"
ZONE="$2"
BUCKET="$3"
CONFIG="$4"
POOL="$5"
COORDINATOR_SA="$6"
WORKER_SA="$7"
INPUT_URI="$8"
OUTPUT_URI="$9"
POOL_MARKER="${10}"
TUNNEL_PID=""

cleanup_tunnel() {
  if [[ -n "$TUNNEL_PID" ]]; then
    kill "$TUNNEL_PID" >/dev/null 2>&1 || true
    wait "$TUNNEL_PID" >/dev/null 2>&1 || true
  fi
}
trap cleanup_tunnel EXIT

printf '%s\n' '--- core smoke ---'
genohype info /tmp/tiny-keyed.ht --json \
  | jq -e '.partitions == 1 and .key_fields == ["gene_id", "chrom", "start"]'
genohype query /tmp/tiny-keyed.ht --json >/tmp/tiny.ndjson
test "$(wc -l </tmp/tiny.ndjson | tr -d ' ')" = 2
genohype export parquet /tmp/tiny-keyed.ht /tmp/tiny.parquet
file /tmp/tiny.parquet

python3 -m venv /tmp/duckdb-venv
/tmp/duckdb-venv/bin/pip -q install duckdb
/tmp/duckdb-venv/bin/python - <<'PY'
import duckdb
rows = duckdb.connect().execute(
    "select count(*) from read_parquet('/tmp/tiny.parquet')"
).fetchone()[0]
assert rows == 2, rows
print(f"CORE_PARQUET_ROWS={rows}")
PY

printf '%s\n' '--- stage distributed fixture ---'
gcloud storage rsync --recursive /tmp/tiny-keyed.ht "$INPUT_URI"
gcloud storage rm --recursive "$OUTPUT_URI" >/dev/null 2>&1 || true

printf '%s\n' '--- create one-worker pool ---'
# Mark before create so interrupted/partial creation is still cleaned exactly.
printf '%s\n' "$POOL" >"$POOL_MARKER"
timeout --foreground 900s genohype --config "$CONFIG" pool create "$POOL" --workers 1 --wait

genohype --config "$CONFIG" pool list "$POOL"
genohype --config "$CONFIG" pool status "$POOL"
genohype --config "$CONFIG" pool workers "$POOL"

coordinator_attached_sa="$(gcloud compute instances describe "${POOL}-coordinator" \
  --project "$PROJECT" --zone "$ZONE" --format='value(serviceAccounts[0].email)')"
worker_attached_sa="$(gcloud compute instances describe "${POOL}-worker-0" \
  --project "$PROJECT" --zone "$ZONE" --format='value(serviceAccounts[0].email)')"
[[ "$coordinator_attached_sa" == "$COORDINATOR_SA" ]]
[[ "$worker_attached_sa" == "$WORKER_SA" ]]

printf '%s\n' '--- dashboard through IAP ---'
gcloud compute ssh "${POOL}-coordinator" \
  --project "$PROJECT" --zone "$ZONE" --tunnel-through-iap --quiet \
  -- -N -L 13000:localhost:3000 >/tmp/dashboard-tunnel.log 2>&1 &
TUNNEL_PID=$!
for attempt in $(seq 1 30); do
  if curl -fsS http://localhost:13000/status >/tmp/dashboard-status.json; then
    break
  fi
  [[ "$attempt" != 30 ]] || {
    echo "dashboard tunnel did not become ready" >&2
    exit 1
  }
  sleep 2
done
curl -fsS http://localhost:13000/api/dashboard/summary \
  | tee /tmp/dashboard-summary-before.json | jq -e .
for attempt in $(seq 1 30); do
  if curl -fsS http://localhost:13000/api/dashboard/workers \
    | tee /tmp/dashboard-workers.json | jq -e 'length == 1' >/dev/null; then
    break
  fi
  [[ "$attempt" != 30 ]] || {
    echo "worker did not register with the coordinator" >&2
    exit 1
  }
  sleep 2
done

printf '%s\n' '--- distributed stress smoke ---'
timeout --foreground 300s genohype --config "$CONFIG" pool submit "$POOL" \
  --batch-size 2 -- stress --partitions 8 --cpu-secs 0.2 --memory-mb 64

genohype --config "$CONFIG" pool status "$POOL"
genohype --config "$CONFIG" pool events "$POOL"
genohype --config "$CONFIG" pool failures "$POOL"
curl -fsS http://localhost:13000/api/dashboard/summary \
  | tee /tmp/dashboard-summary-after-stress.json | jq -e .

printf '%s\n' '--- tiny distributed Parquet export ---'
timeout --foreground 600s genohype --config "$CONFIG" pool submit "$POOL" -- \
  export parquet "$INPUT_URI" "$OUTPUT_URI"

mkdir -p /tmp/distributed-output
gcloud storage cp "${OUTPUT_URI}/**.parquet" /tmp/distributed-output/
test "$(find /tmp/distributed-output -name '*.parquet' -type f | wc -l | tr -d ' ')" = 1
/tmp/duckdb-venv/bin/python - <<'PY'
import duckdb
rows = duckdb.connect().execute(
    "select count(*) from read_parquet('/tmp/distributed-output/*.parquet')"
).fetchone()[0]
assert rows == 2, rows
print(f"DISTRIBUTED_PARQUET_ROWS={rows}")
PY

printf '%s\n' '--- explicit guarded pool cleanup ---'
destroy_nested_pool_guarded "$PROJECT" "$ZONE" "$POOL" "$CONFIG" \
  "$POOL_MARKER" "$(command -v genohype)"

echo "CORE_SMOKE_OK bucket=$BUCKET"
