#!/usr/bin/env bash
# Runs on the disposable private driver. Arguments are identifiers, never credentials.
set -Eeuo pipefail

[[ $# -eq 12 ]] || {
  echo "usage: $0 <project> <zone> <network> <subnet> <bucket> <driver-sa> <coordinator-sa> <worker-sa> <pool> <full-revision> <scenarios> <keep-on-failure>" >&2
  exit 2
}

PROJECT="$1"
ZONE="$2"
NETWORK="$3"
SUBNET="$4"
BUCKET="$5"
DRIVER_SA="$6"
COORDINATOR_SA="$7"
WORKER_SA="$8"
POOL="$9"
EXPECTED_REVISION="${10}"
SCENARIOS="${11}"
KEEP_ON_FAILURE="${12}"
RUN_ID="${POOL#gh-e2e-}"
STOCK_CONFIG="/tmp/genohype-e2e-stock.toml"
ATTESTED_CONFIG="/tmp/genohype-e2e-attested.toml"
ACTIVE_CONFIG="$ATTESTED_CONFIG"
INPUT_URI="gs://${BUCKET}/fixture/tiny-keyed.ht"
OUTPUT_URI="gs://${BUCKET}/output/parquet"
HARNESS_DIR="/tmp/genohype-e2e-harness"
RESULTS_DIR="/var/tmp/genohype-e2e/results"
POOL_MARKER="$RESULTS_DIR/pool-created"

# The harness is copied to this runtime path.
# shellcheck disable=SC1091
source "$HARNESS_DIR/lib/common.sh"
# shellcheck disable=SC1091
source "$HARNESS_DIR/lib/pool-config.sh"
# shellcheck disable=SC1091
source "$HARNESS_DIR/lib/nested-pool.sh"
validate_project "$PROJECT"
validate_zone "$ZONE"
validate_run_id "$RUN_ID"
validate_pool "$RUN_ID" "$POOL"
[[ "$KEEP_ON_FAILURE" == 0 || "$KEEP_ON_FAILURE" == 1 ]] || fail "invalid KEEP_ON_FAILURE"
[[ "$DRIVER_SA" != "$COORDINATOR_SA" && "$DRIVER_SA" != "$WORKER_SA" && "$COORDINATOR_SA" != "$WORKER_SA" ]] \
  || fail "driver, coordinator, and worker service accounts must be distinct"

mkdir -p "$RESULTS_DIR/logs" "$RESULTS_DIR/receipts"
chmod 0700 "$RESULTS_DIR"

cleanup() {
  local rc=$?
  trap - EXIT
  if [[ -f "$POOL_MARKER" ]]; then
    if [[ "$rc" != 0 && "$KEEP_ON_FAILURE" == 1 ]]; then
      echo "KEEP_ON_FAILURE=1: retaining nested pool for diagnosis" >&2
    elif ! destroy_nested_pool_guarded "$PROJECT" "$ZONE" "$POOL" \
      "$ACTIVE_CONFIG" "$POOL_MARKER" "$HOME/.local/bin/genohype"; then
      echo "error: guarded guest cleanup failed; marker retained for host recovery" >&2
      rc=1
    fi
  fi
  exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

run_phase "$RESULTS_DIR" "$RUN_ID" "$EXPECTED_REVISION" clean-room setup 600 \
  bash "$HARNESS_DIR/scenarios/setup.sh" "$EXPECTED_REVISION" "$DRIVER_SA"
export PATH="$HOME/.local/bin:$PATH"

# Core jobs must use the packaged stock worker. The lifecycle profile is the
# only profile allowed to reference the deterministic custom fixture.
write_pool_config "$STOCK_CONFIG" stock "$PROJECT" "$ZONE" "$NETWORK" "$SUBNET" \
  "$POOL" "$WORKER_SA" "$COORDINATOR_SA" "$BUCKET" /tmp/genohype-e2e-release
write_pool_config "$ATTESTED_CONFIG" attested-custom "$PROJECT" "$ZONE" "$NETWORK" "$SUBNET" \
  "$POOL" "$WORKER_SA" "$COORDINATOR_SA" "$BUCKET" /tmp/genohype-e2e-release

IFS=',' read -r -a scenario_list <<<"$SCENARIOS"
# A requested target scenario must fail before core-smoke mutates anything when
# product dependencies are absent; it is never silently skipped.
for scenario in "${scenario_list[@]}"; do
  if [[ "$scenario" == pool-lifecycle ]]; then
    run_phase "$RESULTS_DIR" "$RUN_ID" "$EXPECTED_REVISION" pool-lifecycle \
      pool-lifecycle-preflight 60 bash "$HARNESS_DIR/scenarios/pool-lifecycle.sh" preflight
  fi
done

for scenario in "${scenario_list[@]}"; do
  case "$scenario" in
    core-smoke)
      ACTIVE_CONFIG="$STOCK_CONFIG"
      run_phase "$RESULTS_DIR" "$RUN_ID" "$EXPECTED_REVISION" core-smoke \
        core-smoke 1100 bash "$HARNESS_DIR/scenarios/core-smoke.sh" \
        "$PROJECT" "$ZONE" "$BUCKET" "$STOCK_CONFIG" "$POOL" "$COORDINATOR_SA" \
        "$WORKER_SA" "$INPUT_URI" "$OUTPUT_URI" "$POOL_MARKER"
      ;;
    pool-lifecycle)
      ACTIVE_CONFIG="$ATTESTED_CONFIG"
      run_phase "$RESULTS_DIR" "$RUN_ID" "$EXPECTED_REVISION" pool-lifecycle \
        pool-lifecycle 1100 bash "$HARNESS_DIR/scenarios/pool-lifecycle.sh" run \
        "$PROJECT" "$ZONE" "$ATTESTED_CONFIG" "$POOL" "$WORKER_SA" "$POOL_MARKER" "$RESULTS_DIR"
      ;;
    *)
      fail "unsupported guest scenario: $scenario"
      ;;
  esac
done

[[ ! -f "$POOL_MARKER" ]] || fail "scenario returned without destroying its pool"
jq -s -e 'length > 0 and all(.[]; .status == "passed")' \
  "$RESULTS_DIR/receipts"/*.json >/dev/null
printf 'run_id=%s revision=%s scenarios=%s\n' "$RUN_ID" "$EXPECTED_REVISION" "$SCENARIOS" \
  >"$RESULTS_DIR/GENOHYPE_POOL_E2E_OK"
chmod 0600 "$RESULTS_DIR/GENOHYPE_POOL_E2E_OK"
printf '%s\n' 'GENOHYPE_POOL_E2E_OK'
