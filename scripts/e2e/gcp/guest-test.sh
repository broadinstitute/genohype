#!/usr/bin/env bash
# Runs inside the disposable driver VM. Arguments contain resource identifiers,
# never credentials; authentication comes only from the VM metadata service.
set -Eeuo pipefail

if [[ $# -ne 8 ]]; then
  echo "usage: $0 <project> <zone> <network> <subnet> <bucket> <service-account> <pool> <expected-revision>" >&2
  exit 2
fi

PROJECT="$1"
ZONE="$2"
NETWORK="$3"
SUBNET="$4"
BUCKET="$5"
SERVICE_ACCOUNT="$6"
POOL="$7"
EXPECTED_REVISION="$8"
CONFIG="/tmp/genohype-e2e.toml"
INPUT_URI="gs://${BUCKET}/fixture/tiny-keyed.ht"
OUTPUT_URI="gs://${BUCKET}/output/parquet"
TUNNEL_PID=""
POOL_CREATED=0

cleanup() {
  local rc=$?
  trap - EXIT
  if [[ -n "$TUNNEL_PID" ]]; then
    kill "$TUNNEL_PID" >/dev/null 2>&1 || true
    wait "$TUNNEL_PID" >/dev/null 2>&1 || true
  fi
  if [[ "$POOL_CREATED" == 1 ]]; then
    timeout 300 genohype --config "$CONFIG" pool destroy "$POOL" || true
  fi
  exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

export DEBIAN_FRONTEND=noninteractive
sudo apt-get update -qq
sudo apt-get install -y -qq apt-transport-https ca-certificates curl file gnupg jq python3 python3-venv

# The stock Ubuntu GCE image does not guarantee that gcloud is installed.
curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg \
  | sudo gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg
printf '%s\n' "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" \
  | sudo tee /etc/apt/sources.list.d/google-cloud-sdk.list >/dev/null
sudo apt-get update -qq
sudo apt-get install -y -qq google-cloud-cli

# Install the current-commit artifacts copied by the host. This exercises the
# same checksum and archive logic as a public release installation.
GENOHYPE_VERSION=v0.0.0 \
GENOHYPE_RELEASE_BASE_URL=file:///tmp/genohype-e2e-release \
  sh /tmp/install.sh
export PATH="$HOME/.local/bin:$PATH"

printf '%s\n' '--- identity and installation ---'
gcloud auth list --filter=status:ACTIVE --format='value(account)'
uname -a
genohype --version | tee /tmp/genohype-version.txt
grep -F "($EXPECTED_REVISION)" /tmp/genohype-version.txt
command -v genohype-worker
test -x "$HOME/.local/bin/genohype"
test -e "$HOME/.local/bin/genohype-worker"

cat > "$CONFIG" <<EOF
[defaults]
project = "$PROJECT"
zone = "$ZONE"
network = "$NETWORK"
subnet = "$SUBNET"
public_ip = false
manage_firewall = false

[pools.$POOL]
machine_type = "e2-standard-2"
starting_workers = 1
workers = 1
spot = false
with_coordinator = true
service_account = "$SERVICE_ACCOUNT"
pool_db_path = "gs://${BUCKET}/ops.db"
EOF

printf '%s\n' '--- core smoke ---'
genohype info /tmp/tiny-keyed.ht --json \
  | jq -e '.partitions == 1 and .key_fields == ["gene_id", "chrom", "start"]'
genohype query /tmp/tiny-keyed.ht --json > /tmp/tiny.ndjson
test "$(wc -l < /tmp/tiny.ndjson | tr -d ' ')" = 2
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

printf '%s\n' '--- create pool ---'
timeout 900 genohype --config "$CONFIG" pool create "$POOL" --wait
POOL_CREATED=1

genohype --config "$CONFIG" pool list "$POOL"
genohype --config "$CONFIG" pool status "$POOL"
genohype --config "$CONFIG" pool workers "$POOL"

printf '%s\n' '--- dashboard through IAP ---'
gcloud compute ssh "${POOL}-coordinator" \
  --project "$PROJECT" --zone "$ZONE" --tunnel-through-iap --quiet \
  -- -N -L 13000:localhost:3000 >/tmp/dashboard-tunnel.log 2>&1 &
TUNNEL_PID=$!
for attempt in $(seq 1 30); do
  if curl -fsS http://localhost:13000/status >/tmp/dashboard-status.json; then
    break
  fi
  if [[ "$attempt" == 30 ]]; then
    echo "dashboard tunnel did not become ready" >&2
    exit 1
  fi
  sleep 2
done
curl -fsS http://localhost:13000/api/dashboard/summary | tee /tmp/dashboard-summary-before.json | jq -e .
for attempt in $(seq 1 30); do
  if curl -fsS http://localhost:13000/api/dashboard/workers \
    | tee /tmp/dashboard-workers.json | jq -e 'length == 1' >/dev/null; then
    break
  fi
  if [[ "$attempt" == 30 ]]; then
    echo "worker did not register with the coordinator" >&2
    exit 1
  fi
  sleep 2
done

printf '%s\n' '--- distributed stress smoke ---'
timeout 300 genohype --config "$CONFIG" pool submit "$POOL" \
  --batch-size 2 -- \
  stress --partitions 8 --cpu-secs 0.2 --memory-mb 64

genohype --config "$CONFIG" pool status "$POOL"
genohype --config "$CONFIG" pool events "$POOL"
genohype --config "$CONFIG" pool failures "$POOL"
curl -fsS http://localhost:13000/api/dashboard/summary \
  | tee /tmp/dashboard-summary-after-stress.json | jq -e .

printf '%s\n' '--- tiny distributed Parquet export ---'
timeout 600 genohype --config "$CONFIG" pool submit "$POOL" -- \
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

printf '%s\n' '--- explicit pool cleanup ---'
timeout 300 genohype --config "$CONFIG" pool destroy "$POOL"
POOL_CREATED=0

test -z "$(gcloud compute instances list --project "$PROJECT" \
  --filter="tags.items:pool-${POOL}" --format='value(name)')"

printf '%s\n' 'GENOHYPE_POOL_E2E_OK'
