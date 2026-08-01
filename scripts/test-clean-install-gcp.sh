#!/usr/bin/env bash
# Provision a disposable x86_64 GCP VM, install the published Genohype release,
# run a small gnomAD v4.1.1 interval export, validate its Parquet output, and
# delete the VM. No repository checkout or local build artifacts enter the VM.
#
# Example for a project without a default VPC and with IAP-only SSH:
#   PROJECT=my-project NETWORK=my-vpc SUBNET=my-subnet USE_IAP=1 \
#     ./scripts/test-clean-install-gcp.sh
#
# Set KEEP_VM=1 only when retaining a failed VM for interactive diagnosis.
set -Eeuo pipefail

PROJECT="${PROJECT:-$(gcloud config get-value project 2>/dev/null)}"
ZONE="${ZONE:-us-central1-a}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-2}"
NETWORK="${NETWORK:-}"
SUBNET="${SUBNET:-}"
USE_IAP="${USE_IAP:-0}"
GENOHYPE_VERSION="${GENOHYPE_VERSION:-}"
KEEP_VM="${KEEP_VM:-0}"
RUN_ID="$(date -u +%Y%m%dt%H%M%Sz)"
VM_NAME="${VM_NAME:-genohype-clean-${RUN_ID}}"
LOG_FILE="${LOG_FILE:-${TMPDIR:-/tmp}/${VM_NAME}.log}"
GUEST_SCRIPT="$(mktemp "${TMPDIR:-/tmp}/genohype-clean-guest.XXXXXX")"
VM_CREATED=0

if [[ -z "$PROJECT" || "$PROJECT" == "(unset)" ]]; then
  echo "error: set PROJECT or configure a gcloud project" >&2
  exit 2
fi

for command in gcloud; do
  command -v "$command" >/dev/null || {
    echo "error: $command is required" >&2
    exit 2
  }
done

cleanup() {
  local rc=$?
  trap - EXIT
  rm -f "$GUEST_SCRIPT"
  if [[ "$VM_CREATED" == 1 && "$KEEP_VM" != 1 ]]; then
    echo "Deleting disposable VM $VM_NAME..."
    gcloud compute instances delete "$VM_NAME" \
      --project "$PROJECT" --zone "$ZONE" --quiet || true
  elif [[ "$VM_CREATED" == 1 ]]; then
    echo "KEEP_VM=1; retained $VM_NAME in $PROJECT/$ZONE"
  fi
  echo "Guest log: $LOG_FILE"
  exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

cat > "$GUEST_SCRIPT" <<'GUEST'
#!/usr/bin/env bash
set -Eeuo pipefail

REQUESTED_VERSION="${1:-}"
INPUT="gs://gcp-public-data--gnomad/release/4.1.1/ht/browser/gnomad.browser.v4.1.1.sites.ht"
INTERVAL="chr22:50000000-50010000"
OUTPUT="/tmp/gnomad-v4.1.1-small.parquet"

export DEBIAN_FRONTEND=noninteractive
sudo apt-get update -qq
sudo apt-get install -y -qq ca-certificates curl file python3 python3-venv

# Exercise the public installation path exactly; pin only when requested by the
# host through GENOHYPE_VERSION.
if [[ -n "$REQUESTED_VERSION" ]]; then
  curl -fsSL https://raw.githubusercontent.com/broadinstitute/genohype/main/scripts/install.sh \
    | GENOHYPE_VERSION="$REQUESTED_VERSION" sh
else
  curl -fsSL https://raw.githubusercontent.com/broadinstitute/genohype/main/scripts/install.sh | sh
fi
export PATH="$HOME/.local/bin:$PATH"

printf '%s\n' '--- clean-room identity ---'
uname -a
genohype --version
command -v genohype
command -v genohype-worker
test -x "$HOME/.local/bin/genohype"
test -e "$HOME/.local/bin/genohype-worker"

printf '%s\n' '--- source metadata ---'
genohype info "$INPUT" --json | tee /tmp/gnomad-info.json

printf '%s\n' '--- interval query ---'
genohype query "$INPUT" --interval "$INTERVAL" --limit 1 --json \
  | tee /tmp/gnomad-query.ndjson
test -s /tmp/gnomad-query.ndjson

printf '%s\n' '--- interval export ---'
rm -f "$OUTPUT"
time genohype export parquet "$INPUT" "$OUTPUT" \
  --interval "$INTERVAL" --benchmark
file "$OUTPUT"
test -s "$OUTPUT"

printf '%s\n' '--- independent Parquet validation ---'
python3 -m venv /tmp/duckdb-venv
/tmp/duckdb-venv/bin/pip -q install duckdb
/tmp/duckdb-venv/bin/python - <<'PY'
import duckdb
import json

path = "/tmp/gnomad-v4.1.1-small.parquet"
con = duckdb.connect()
rows, minimum, maximum = con.execute(
    "SELECT count(*), min(locus.position), max(locus.position) FROM read_parquet(?)",
    [path],
).fetchone()
assert rows > 0, "interval export contained no rows"
assert minimum >= 50_000_000, minimum
assert maximum <= 50_010_000, maximum
print(json.dumps({
    "parquet": path,
    "rows": rows,
    "minimum_position": minimum,
    "maximum_position": maximum,
}))
PY

printf '%s\n' 'CLEAN_ROOM_E2E_OK'
GUEST
chmod 700 "$GUEST_SCRIPT"

create_args=(
  "$VM_NAME"
  --project "$PROJECT"
  --zone "$ZONE"
  --machine-type "$MACHINE_TYPE"
  --image-family ubuntu-2404-lts-amd64
  --image-project ubuntu-os-cloud
  --boot-disk-size 20GB
  --scopes cloud-platform
  --labels purpose=genohype-clean-install
  --quiet
)
if [[ -n "$NETWORK" ]]; then
  create_args+=(--network "$NETWORK")
fi
if [[ -n "$SUBNET" ]]; then
  create_args+=(--subnet "$SUBNET")
fi

echo "Creating disposable VM $VM_NAME in $PROJECT/$ZONE ($MACHINE_TYPE)..."
gcloud compute instances create "${create_args[@]}"
VM_CREATED=1

ssh_args=(
  "$VM_NAME"
  --project "$PROJECT"
  --zone "$ZONE"
  --quiet
)
if [[ "$USE_IAP" == 1 ]]; then
  ssh_args+=(--tunnel-through-iap)
fi

echo "Waiting for SSH..."
for attempt in $(seq 1 36); do
  if gcloud compute ssh "${ssh_args[@]}" \
    --command true >/dev/null 2>&1; then
    break
  fi
  if [[ "$attempt" == 36 ]]; then
    echo "error: SSH did not become ready" >&2
    exit 1
  fi
  sleep 5
done

echo "Running clean-room installation and interval export..."
set -o pipefail
gcloud compute ssh "${ssh_args[@]}" \
  --command "bash -s -- '$GENOHYPE_VERSION'" \
  < "$GUEST_SCRIPT" 2>&1 | tee "$LOG_FILE"

grep -q '^CLEAN_ROOM_E2E_OK$' "$LOG_FILE"
echo "Clean-room test passed."
