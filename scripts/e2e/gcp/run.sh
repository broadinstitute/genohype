#!/usr/bin/env bash
# Plan or execute the private, one-worker GCP clean-room smoke harness.
set -Eeuo pipefail
umask 077

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
# Resolved relative to this script at runtime.
# shellcheck disable=SC1091
source "$SCRIPT_DIR/lib/common.sh"

MODE="${1:-plan}"
PROJECT="${PROJECT:-}"
CONFIRM_PROJECT="${CONFIRM_PROJECT:-}"
CONFIRM_RUN_ID="${CONFIRM_RUN_ID:-}"
REGION="${REGION:-us-east1}"
ZONE="${ZONE:-us-east1-b}"
DRIVER_MACHINE_TYPE="${DRIVER_MACHINE_TYPE:-e2-standard-2}"
SCENARIOS="${SCENARIOS:-pool-lifecycle}"
ROUTINE_WORKERS="${ROUTINE_WORKERS:-1}"
MAX_WORKERS="${MAX_WORKERS:-1}"
ENABLE_COORDINATOR_SCALING="${ENABLE_COORDINATOR_SCALING:-0}"
KEEP_ON_FAILURE="${KEEP_ON_FAILURE:-0}"
ALLOW_BILLABLE="${ALLOW_BILLABLE:-0}"
RUN_ID="${RUN_ID:-r$(date -u +%Y%m%dt%H%M%Sz)}"
STATE_DIR="${STATE_DIR:-${TMPDIR:-/tmp}/genohype-e2e-${RUN_ID}}"
TF_SOURCE="$SCRIPT_DIR/terraform"
TF_DIR="$STATE_DIR/terraform"
RELEASE_DIR="$STATE_DIR/genohype-e2e-release"
HOST_RESULTS="$STATE_DIR/host-results"
PLAN_PATH="$STATE_DIR/plan.tfplan"
PLAN_JSON="$STATE_DIR/plan.json"
POOL="gh-e2e-${RUN_ID}"
EXPECTED_REVISION="$(git -C "$REPO_ROOT" rev-parse HEAD)"
DRIVER=""
APPLY_STARTED=0
CLEANED=0
RESULTS_RETRIEVED=0
TF_TIMEOUT_SECS="${TF_TIMEOUT_SECS:-1200}"
SSH_TIMEOUT_SECS="${SSH_TIMEOUT_SECS:-600}"
GUEST_TIMEOUT_OVERRIDE="${GUEST_TIMEOUT_SECS:-}"
GUEST_SETUP_BUDGET_SECS=600
GUEST_PREFLIGHT_BUDGET_SECS=60
GUEST_SCENARIO_BUDGET_SECS=1100
GUEST_CLEANUP_BUDGET_SECS=300
GUEST_TRANSPORT_MARGIN_SECS=180

[[ "$MODE" == plan || "$MODE" == apply ]] || {
  echo "usage: PROJECT=<non-production-project> $0 [plan|apply]" >&2
  exit 2
}
[[ -n "$PROJECT" ]] || fail "PROJECT is required; use a dedicated non-production project"
validate_project "$PROJECT"
validate_region "$REGION"
validate_zone "$ZONE"
validate_run_id "$RUN_ID"
validate_pool "$RUN_ID" "$POOL"
[[ "$ZONE" == "$REGION"-* ]] || fail "ZONE must belong to REGION"
[[ "$ROUTINE_WORKERS" == 1 && "$MAX_WORKERS" == 1 ]] \
  || fail "this harness is fixed to one on-demand worker"
[[ "$ENABLE_COORDINATOR_SCALING" == 0 ]] \
  || fail "coordinator-driven scaling is outside this harness slice"
[[ "$KEEP_ON_FAILURE" == 0 || "$KEEP_ON_FAILURE" == 1 ]] || fail "KEEP_ON_FAILURE must be 0 or 1"
[[ "$ALLOW_BILLABLE" == 0 || "$ALLOW_BILLABLE" == 1 ]] || fail "ALLOW_BILLABLE must be 0 or 1"
if [[ "$KEEP_ON_FAILURE" == 1 && "$ALLOW_BILLABLE" != 1 ]]; then
  fail "KEEP_ON_FAILURE=1 requires ALLOW_BILLABLE=1"
fi
if [[ "$MODE" == apply && "$CONFIRM_PROJECT" != "$PROJECT" ]]; then
  fail "apply requires CONFIRM_PROJECT to exactly equal PROJECT"
fi
if [[ "$MODE" == apply && "$CONFIRM_RUN_ID" != "$RUN_ID" ]]; then
  fail "apply requires CONFIRM_RUN_ID to exactly equal RUN_ID"
fi
IFS=',' read -r -a scenario_list <<<"$SCENARIOS"
seen_scenarios=","
lifecycle_preflight_count=0
for scenario in "${scenario_list[@]}"; do
  [[ "$scenario" == core-smoke || "$scenario" == pool-lifecycle ]] \
    || fail "unsupported scenario '$scenario' (allowed: core-smoke,pool-lifecycle)"
  [[ "$seen_scenarios" != *",${scenario},"* ]] || fail "duplicate scenario: $scenario"
  seen_scenarios+="${scenario},"
  [[ "$scenario" != pool-lifecycle ]] || lifecycle_preflight_count=1
done
MIN_GUEST_TIMEOUT_SECS=$((
  GUEST_SETUP_BUDGET_SECS +
  lifecycle_preflight_count * GUEST_PREFLIGHT_BUDGET_SECS +
  ${#scenario_list[@]} * GUEST_SCENARIO_BUDGET_SECS +
  GUEST_CLEANUP_BUDGET_SECS +
  GUEST_TRANSPORT_MARGIN_SECS
))
GUEST_TIMEOUT_SECS="${GUEST_TIMEOUT_OVERRIDE:-$MIN_GUEST_TIMEOUT_SECS}"
for value in "$TF_TIMEOUT_SECS" "$SSH_TIMEOUT_SECS"; do
  validate_positive_timeout timeout "$value" 3600
done
validate_positive_timeout guest-timeout "$GUEST_TIMEOUT_SECS" 7200
((GUEST_TIMEOUT_SECS >= MIN_GUEST_TIMEOUT_SECS)) \
  || fail "GUEST_TIMEOUT_SECS must be at least derived phase budget ${MIN_GUEST_TIMEOUT_SECS}s"
for command in gcloud jq python3 tar terraform timeout; do require_command "$command"; done

if [[ -e "$STATE_DIR" ]]; then
  fail "STATE_DIR already exists; choose a fresh RUN_ID/STATE_DIR: $STATE_DIR"
fi
mkdir -m 0700 "$STATE_DIR"
mkdir -m 0700 "$TF_DIR" "$RELEASE_DIR" "$HOST_RESULTS"
mkdir -p "$HOST_RESULTS/logs" "$HOST_RESULTS/receipts"
cp "$TF_SOURCE"/*.tf "$TF_DIR/"

DRIVER_SA="ghd-${RUN_ID}@${PROJECT}.iam.gserviceaccount.com"
COORDINATOR_SA="ghc-${RUN_ID}@${PROJECT}.iam.gserviceaccount.com"
WORKER_SA="ghw-${RUN_ID}@${PROJECT}.iam.gserviceaccount.com"

write_run_manifest() {
  local retained="$1" tmp="$STATE_DIR/run.json.tmp.$$"
  jq -n \
    --arg project "$PROJECT" --arg region "$REGION" --arg zone "$ZONE" \
    --arg run_id "$RUN_ID" --arg pool "$POOL" --arg prefix "gh-e2e-${RUN_ID}" \
    --arg tf_dir "$TF_DIR" --arg state_dir "$STATE_DIR" \
    --arg machine "$DRIVER_MACHINE_TYPE" --arg scenarios "$SCENARIOS" \
    --arg revision "$EXPECTED_REVISION" --arg driver_sa "$DRIVER_SA" \
    --arg coordinator_sa "$COORDINATOR_SA" --arg worker_sa "$WORKER_SA" \
    --arg started_at "$(utc_now)" --argjson retained "$retained" \
    '{schema_version:1,project:$project,region:$region,zone:$zone,
      run_id:$run_id,pool:$pool,prefix:$prefix,terraform_dir:$tf_dir,
      state_dir:$state_dir,driver_machine_type:$machine,
      scenarios:($scenarios|split(",")),max_workers:1,
      expected_revision:$revision,started_at:$started_at,retained:$retained,
      service_accounts:{driver_orchestrator:$driver_sa,
        coordinator_runtime:$coordinator_sa,worker_runtime:$worker_sa}}' >"$tmp"
  chmod 0600 "$tmp"
  mv "$tmp" "$STATE_DIR/run.json"
}
write_run_manifest false

retrieve_guest_results() {
  [[ "$RESULTS_RETRIEVED" == 0 && -n "$DRIVER" ]] || return 0
  mkdir -p "$STATE_DIR/guest-results"
  if timeout --foreground "${SSH_TIMEOUT_SECS}s" gcloud compute scp --recurse \
    --project "$PROJECT" --zone "$ZONE" --tunnel-through-iap --quiet \
    "${DRIVER}:/var/tmp/genohype-e2e/results" "$STATE_DIR/guest-results/" \
    >/dev/null 2>&1; then
    RESULTS_RETRIEVED=1
    tar -czf "$STATE_DIR/guest-results.tar.gz" -C "$STATE_DIR/guest-results" results
    if command -v sha256sum >/dev/null; then
      sha256sum "$STATE_DIR/guest-results.tar.gz" >"$STATE_DIR/guest-results.tar.gz.sha256"
    else
      shasum -a 256 "$STATE_DIR/guest-results.tar.gz" >"$STATE_DIR/guest-results.tar.gz.sha256"
    fi
  fi
}

cleanup() {
  local rc=$?
  trap - EXIT
  retrieve_guest_results || true
  if [[ "$APPLY_STARTED" == 1 && "$CLEANED" != 1 ]]; then
    # KEEP is checked before any nested-pool fallback deletion.
    if [[ "$rc" != 0 && "$KEEP_ON_FAILURE" == 1 ]]; then
      write_run_manifest true
      echo "KEEP_ON_FAILURE=1: retaining driver, nested pool, and Terraform resources." >&2
      echo "Billable resources have no automatic expiry. Cleanup exactly with:" >&2
      echo "  $SCRIPT_DIR/cleanup.sh $STATE_DIR" >&2
    elif "$SCRIPT_DIR/cleanup.sh" "$STATE_DIR"; then
      CLEANED=1
    else
      echo "error: cleanup failed; retry: $SCRIPT_DIR/cleanup.sh $STATE_DIR" >&2
      rc=1
    fi
  fi
  echo "E2E state, plan, receipts, and logs: $STATE_DIR"
  exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

printf '%s\n' "Project: $PROJECT" "Run ID: $RUN_ID" "State: $STATE_DIR" \
  "Scenarios: $SCENARIOS" "Workers: 1 on-demand" \
  "Timeouts: terraform=${TF_TIMEOUT_SECS}s ssh=${SSH_TIMEOUT_SECS}s guest=${GUEST_TIMEOUT_SECS}s (minimum derived=${MIN_GUEST_TIMEOUT_SECS}s)"
echo "WARNING: during apply, ephemeral driver gh-e2e-${RUN_ID}-driver receives project-wide roles/compute.instanceAdmin.v1 in ${PROJECT}." >&2
echo "Safety boundary: exact confirmed project + unique run prefix gh-e2e-${RUN_ID}; teardown is immediate unless explicitly retained." >&2
gcloud projects describe "$PROJECT" --format='value(projectId,lifecycleState)'

run_phase "$HOST_RESULTS" "$RUN_ID" "$EXPECTED_REVISION" host terraform-init \
  "$TF_TIMEOUT_SECS" terraform -chdir="$TF_DIR" init -input=false
run_phase "$HOST_RESULTS" "$RUN_ID" "$EXPECTED_REVISION" host terraform-plan \
  "$TF_TIMEOUT_SECS" terraform -chdir="$TF_DIR" plan -input=false -out="$PLAN_PATH" \
  -var="project_id=$PROJECT" -var="region=$REGION" -var="zone=$ZONE" \
  -var="run_id=$RUN_ID" -var="driver_machine_type=$DRIVER_MACHINE_TYPE"
terraform -chdir="$TF_DIR" show -json "$PLAN_PATH" >"$PLAN_JSON"

jq -e '
  [.resource_changes[]? | select(.mode == "managed")] as $changes |
  ($changes | length > 0) and all($changes[]; .change.actions == ["create"]) and
  all($changes[]; (.type | IN(
    "google_service_account","google_project_iam_member",
    "google_service_account_iam_member","google_compute_network",
    "google_compute_subnetwork","google_compute_subnetwork_iam_member",
    "google_compute_router","google_compute_router_nat","google_compute_firewall",
    "google_storage_bucket","google_storage_bucket_iam_member","google_compute_instance")))
' "$PLAN_JSON" >/dev/null || fail "plan contains a non-create or unexpected resource type"
if grep -En 'roles/compute\.admin|google_service_account_key|google_.*_iam_(policy|binding)|access_config' "$TF_DIR"/*.tf; then
  fail "Terraform contains a broad role, key, policy replacement, or public IP"
fi
echo "Saved reviewed plan: $PLAN_PATH"
echo "Managed additions: $(jq '[.resource_changes[]? | select(.mode=="managed")] | length' "$PLAN_JSON")"
if [[ "$MODE" == plan ]]; then
  echo "Plan-only mode complete; no GCP resources were created."
  exit 0
fi

require_command cargo
run_phase "$HOST_RESULTS" "$RUN_ID" "$EXPECTED_REVISION" host build-artifacts 1800 \
  "$SCRIPT_DIR/build-artifacts.sh" "$RELEASE_DIR" "$EXPECTED_REVISION"

APPLY_STARTED=1
run_phase "$HOST_RESULTS" "$RUN_ID" "$EXPECTED_REVISION" host terraform-apply \
  "$TF_TIMEOUT_SECS" terraform -chdir="$TF_DIR" apply -input=false -auto-approve "$PLAN_PATH"
DRIVER="$(terraform -chdir="$TF_DIR" output -raw driver_name)"
NETWORK="$(terraform -chdir="$TF_DIR" output -raw network_name)"
SUBNET="$(terraform -chdir="$TF_DIR" output -raw subnet_name)"
BUCKET="$(terraform -chdir="$TF_DIR" output -raw bucket_name)"
DRIVER_SA="$(terraform -chdir="$TF_DIR" output -raw driver_orchestrator_service_account_email)"
COORDINATOR_SA="$(terraform -chdir="$TF_DIR" output -raw coordinator_runtime_service_account_email)"
WORKER_SA="$(terraform -chdir="$TF_DIR" output -raw worker_runtime_service_account_email)"
# Persist the authoritative Terraform outputs for interrupted-run cleanup.
write_run_manifest false
[[ "$DRIVER_SA" != "$COORDINATOR_SA" && "$DRIVER_SA" != "$WORKER_SA" && "$COORDINATOR_SA" != "$WORKER_SA" ]] \
  || fail "service accounts are not distinct"
for account in "$DRIVER_SA" "$COORDINATOR_SA" "$WORKER_SA"; do
  test -z "$(gcloud iam service-accounts keys list --iam-account "$account" \
    --project "$PROJECT" --managed-by=user --format='value(name)')"
done
gcloud compute instances describe "$DRIVER" --project "$PROJECT" --zone "$ZONE" \
  --format=json >"$STATE_DIR/driver.json"
jq -e --arg sa "$DRIVER_SA" '.serviceAccounts[0].email == $sa and
  ((.networkInterfaces[0].accessConfigs // []) | length == 0)' "$STATE_DIR/driver.json" >/dev/null

SSH_ARGS=("$DRIVER" --project "$PROJECT" --zone "$ZONE" --tunnel-through-iap --quiet)
echo "Waiting for private driver SSH through IAP..."
deadline=$((SECONDS + SSH_TIMEOUT_SECS))
until timeout --foreground 30s gcloud compute ssh "${SSH_ARGS[@]}" --command true >/dev/null 2>&1; do
  ((SECONDS < deadline)) || fail "driver SSH did not become ready"
  sleep 5
done

timeout --foreground "${SSH_TIMEOUT_SECS}s" gcloud compute ssh "${SSH_ARGS[@]}" --command \
  'rm -rf /tmp/genohype-e2e-release /tmp/genohype-e2e-harness /tmp/tiny-keyed.ht; mkdir -p /tmp/genohype-e2e-harness'
timeout --foreground "${SSH_TIMEOUT_SECS}s" gcloud compute scp --recurse \
  --project "$PROJECT" --zone "$ZONE" --tunnel-through-iap --quiet \
  "$RELEASE_DIR" "${DRIVER}:/tmp/"
timeout --foreground "${SSH_TIMEOUT_SECS}s" gcloud compute scp --recurse \
  --project "$PROJECT" --zone "$ZONE" --tunnel-through-iap --quiet \
  "$SCRIPT_DIR/lib" "$SCRIPT_DIR/scenarios" "${DRIVER}:/tmp/genohype-e2e-harness/"
timeout --foreground "${SSH_TIMEOUT_SECS}s" gcloud compute scp \
  --project "$PROJECT" --zone "$ZONE" --tunnel-through-iap --quiet \
  "$REPO_ROOT/scripts/install.sh" "$SCRIPT_DIR/guest-test.sh" \
  "${DRIVER}:/tmp/genohype-e2e-harness/"
timeout --foreground "${SSH_TIMEOUT_SECS}s" gcloud compute scp --recurse \
  --project "$PROJECT" --zone "$ZONE" --tunnel-through-iap --quiet \
  "$REPO_ROOT/core/tests/fixtures/tiny-keyed.ht" "${DRIVER}:/tmp/"

printf -v GUEST_COMMAND \
  'bash /tmp/genohype-e2e-harness/guest-test.sh %q %q %q %q %q %q %q %q %q %q %q %q' \
  "$PROJECT" "$ZONE" "$NETWORK" "$SUBNET" "$BUCKET" "$DRIVER_SA" \
  "$COORDINATOR_SA" "$WORKER_SA" "$POOL" "$EXPECTED_REVISION" "$SCENARIOS" "$KEEP_ON_FAILURE"
run_phase "$HOST_RESULTS" "$RUN_ID" "$EXPECTED_REVISION" host guest-suite \
  "$GUEST_TIMEOUT_SECS" gcloud compute ssh "${SSH_ARGS[@]}" --command "$GUEST_COMMAND"

retrieve_guest_results
test -f "$STATE_DIR/guest-results/results/GENOHYPE_POOL_E2E_OK"
jq -s -e 'length > 0 and all(.[]; .status == "passed")' \
  "$STATE_DIR/guest-results/results/receipts"/*.json >/dev/null

echo "Build/install/core/one-worker E2E passed."
