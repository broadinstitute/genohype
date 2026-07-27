#!/usr/bin/env bash
# Build the current Linux commit, install it on a Terraform-provisioned clean
# driver VM, run core + one-worker pool smoke tests, and destroy every resource.
set -Eeuo pipefail

MODE="${1:-plan}"
if [[ "$MODE" != plan && "$MODE" != apply ]]; then
  echo "usage: PROJECT=<non-production-project> $0 [plan|apply]" >&2
  exit 2
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
TF_SOURCE="$REPO_ROOT/scripts/e2e/gcp/terraform"
PROJECT="${PROJECT:-}"
REGION="${REGION:-us-east1}"
ZONE="${ZONE:-us-east1-b}"
DRIVER_MACHINE_TYPE="${DRIVER_MACHINE_TYPE:-e2-standard-2}"
KEEP_ON_FAILURE="${KEEP_ON_FAILURE:-0}"
RUN_ID="${RUN_ID:-r$(date -u +%Y%m%dt%H%M%Sz)}"
STATE_DIR="${STATE_DIR:-${TMPDIR:-/tmp}/genohype-e2e-${RUN_ID}}"
TF_DIR="$STATE_DIR/terraform"
RELEASE_DIR="$STATE_DIR/genohype-e2e-release"
LOG_FILE="$STATE_DIR/e2e.log"
POOL="gh-e2e-${RUN_ID}"
EXPECTED_REVISION="$(git -C "$REPO_ROOT" rev-parse --short=7 HEAD)"
APPLIED=0
TEST_PASSED=0

if [[ -z "$PROJECT" ]]; then
  echo "error: PROJECT is required; use a dedicated non-production project" >&2
  exit 2
fi

for command in cargo gcloud jq tar terraform; do
  command -v "$command" >/dev/null || {
    echo "error: $command is required" >&2
    exit 2
  }
done

mkdir -p "$TF_DIR" "$RELEASE_DIR"
cp "$TF_SOURCE"/*.tf "$TF_DIR/"

{
  printf 'PROJECT=%q\n' "$PROJECT"
  printf 'REGION=%q\n' "$REGION"
  printf 'ZONE=%q\n' "$ZONE"
  printf 'RUN_ID=%q\n' "$RUN_ID"
  printf 'POOL=%q\n' "$POOL"
  printf 'TF_DIR=%q\n' "$TF_DIR"
  printf 'DRIVER_MACHINE_TYPE=%q\n' "$DRIVER_MACHINE_TYPE"
} > "$STATE_DIR/run.env"

cleanup_pool_from_host() {
  local rows
  rows="$(gcloud compute instances list --project "$PROJECT" \
    --filter="tags.items:pool-${POOL}" \
    --format='value(name,zone.basename())' 2>/dev/null || true)"
  if [[ -n "$rows" ]]; then
    echo "Host fallback: deleting exact-tag pool VMs..."
    while IFS=$'\t' read -r name zone; do
      [[ -n "$name" && -n "$zone" ]] || continue
      gcloud compute instances delete "$name" --project "$PROJECT" \
        --zone "$zone" --quiet || true
    done <<< "$rows"
  fi
}

cleanup() {
  local rc=$?
  trap - EXIT
  if [[ "$APPLIED" == 1 ]]; then
    cleanup_pool_from_host
    if [[ "$KEEP_ON_FAILURE" == 1 && "$TEST_PASSED" != 1 ]]; then
      echo "KEEP_ON_FAILURE=1; retaining Terraform resources in $STATE_DIR" >&2
    else
      echo "Destroying Terraform-managed E2E infrastructure..."
      if ! terraform -chdir="$TF_DIR" destroy -auto-approve \
        -var="project_id=$PROJECT" \
        -var="region=$REGION" \
        -var="zone=$ZONE" \
        -var="run_id=$RUN_ID" \
        -var="driver_machine_type=$DRIVER_MACHINE_TYPE"; then
        echo "error: Terraform cleanup failed; recover with cleanup.sh $STATE_DIR" >&2
        if [[ "$rc" == 0 ]]; then
          rc=1
        fi
      fi
    fi
  fi
  echo "E2E state and logs: $STATE_DIR"
  exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

printf '%s\n' "Project: $PROJECT" "Run ID: $RUN_ID" "State:   $STATE_DIR"
gcloud projects describe "$PROJECT" --format='value(projectId,lifecycleState)'

terraform -chdir="$TF_DIR" init -input=false
terraform -chdir="$TF_DIR" plan -out="$STATE_DIR/plan.tfplan" \
  -var="project_id=$PROJECT" \
  -var="region=$REGION" \
  -var="zone=$ZONE" \
  -var="run_id=$RUN_ID" \
  -var="driver_machine_type=$DRIVER_MACHINE_TYPE"

if [[ "$MODE" == plan ]]; then
  echo "Plan-only mode complete. Re-run with 'apply' to execute the E2E test."
  exit 0
fi

printf '%s\n' '--- build current x86_64 Linux artifact ---'
cd "$REPO_ROOT"
./scripts/build-dashboard.sh
cargo zigbuild --locked --target x86_64-unknown-linux-gnu \
  --release --package genohype-cli --bin genohype --features full
LINUX_BINARY="$REPO_ROOT/target/x86_64-unknown-linux-gnu/release/genohype"
test -x "$LINUX_BINARY"
"$LINUX_BINARY" --version

printf '%s\n' '--- package installer-compatible E2E release ---'
PACKAGE_STAGE="$STATE_DIR/package-stage"
mkdir -p "$PACKAGE_STAGE/main" "$PACKAGE_STAGE/worker"
cp "$LINUX_BINARY" "$PACKAGE_STAGE/main/genohype"
cp "$LINUX_BINARY" "$PACKAGE_STAGE/worker/genohype-worker"
cp "$REPO_ROOT/LICENSE" "$PACKAGE_STAGE/main/LICENSE"
cp "$REPO_ROOT/LICENSE" "$PACKAGE_STAGE/worker/LICENSE"
chmod 0755 "$PACKAGE_STAGE/main/genohype" "$PACKAGE_STAGE/worker/genohype-worker"
tar -czf "$RELEASE_DIR/genohype-v0.0.0-x86_64-unknown-linux-gnu.tar.gz" \
  -C "$PACKAGE_STAGE/main" genohype LICENSE
tar -czf "$RELEASE_DIR/genohype-worker-v0.0.0-x86_64-unknown-linux-gnu.tar.gz" \
  -C "$PACKAGE_STAGE/worker" genohype-worker LICENSE
(
  cd "$RELEASE_DIR"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum ./*.tar.gz > SHA256SUMS
  else
    for archive in ./*.tar.gz; do
      printf '%s  %s\n' "$(shasum -a 256 "$archive" | awk '{print $1}')" "${archive#./}"
    done > SHA256SUMS
  fi
)

# Mark cleanup active before apply so a partial Terraform failure is destroyed.
APPLIED=1
terraform -chdir="$TF_DIR" apply -auto-approve "$STATE_DIR/plan.tfplan"

DRIVER="$(terraform -chdir="$TF_DIR" output -raw driver_name)"
NETWORK="$(terraform -chdir="$TF_DIR" output -raw network_name)"
SUBNET="$(terraform -chdir="$TF_DIR" output -raw subnet_name)"
BUCKET="$(terraform -chdir="$TF_DIR" output -raw bucket_name)"
SERVICE_ACCOUNT="$(terraform -chdir="$TF_DIR" output -raw service_account_email)"

SSH_ARGS=(
  "$DRIVER"
  --project "$PROJECT"
  --zone "$ZONE"
  --tunnel-through-iap
  --quiet
)

echo "Waiting for driver SSH through IAP..."
for attempt in $(seq 1 48); do
  if gcloud compute ssh "${SSH_ARGS[@]}" --command true >/dev/null 2>&1; then
    break
  fi
  if [[ "$attempt" == 48 ]]; then
    echo "error: driver SSH did not become ready" >&2
    exit 1
  fi
  sleep 5
done

gcloud compute ssh "${SSH_ARGS[@]}" --command \
  'rm -rf /tmp/genohype-e2e-release /tmp/tiny-keyed.ht'
gcloud compute scp --recurse --project "$PROJECT" --zone "$ZONE" \
  --tunnel-through-iap --quiet \
  "$RELEASE_DIR" "${DRIVER}:/tmp/"
gcloud compute scp --project "$PROJECT" --zone "$ZONE" \
  --tunnel-through-iap --quiet \
  "$REPO_ROOT/scripts/install.sh" "$REPO_ROOT/scripts/e2e/gcp/guest-test.sh" \
  "${DRIVER}:/tmp/"
gcloud compute scp --recurse --project "$PROJECT" --zone "$ZONE" \
  --tunnel-through-iap --quiet \
  "$REPO_ROOT/core/tests/fixtures/tiny-keyed.ht" "${DRIVER}:/tmp/"

set -o pipefail
gcloud compute ssh "${SSH_ARGS[@]}" --command \
  "bash /tmp/guest-test.sh '$PROJECT' '$ZONE' '$NETWORK' '$SUBNET' '$BUCKET' '$SERVICE_ACCOUNT' '$POOL' '$EXPECTED_REVISION'" \
  2>&1 | tee "$LOG_FILE"
grep -q '^GENOHYPE_POOL_E2E_OK$' "$LOG_FILE"
TEST_PASSED=1

echo "Build/install/core/pool E2E passed."
