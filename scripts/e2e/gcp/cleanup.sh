#!/usr/bin/env bash
# Recover an interrupted run from a validated JSON manifest. Every nested
# deletion requires both the exact pool tag and an anchored run-owned name.
set -Eeuo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <state-dir>" >&2
  exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Resolved relative to this script at runtime.
# shellcheck disable=SC1091
source "$SCRIPT_DIR/lib/common.sh"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/lib/nested-pool.sh"
require_command gcloud
require_command jq
require_command terraform
require_command timeout

STATE_DIR="$(cd "$1" && pwd -P)"
MANIFEST="$STATE_DIR/run.json"
[[ -f "$MANIFEST" ]] || {
  echo "error: missing $MANIFEST" >&2
  exit 2
}
jq -e '
  .schema_version == 1
  and (.project | type == "string")
  and (.run_id | type == "string")
  and (.pool | type == "string")
  and (.terraform_dir | type == "string")
  and (.service_accounts | type == "object")
' "$MANIFEST" >/dev/null

PROJECT="$(jq -r .project "$MANIFEST")"
REGION="$(jq -r .region "$MANIFEST")"
ZONE="$(jq -r .zone "$MANIFEST")"
RUN_ID="$(jq -r .run_id "$MANIFEST")"
POOL="$(jq -r .pool "$MANIFEST")"
PREFIX="$(jq -r .prefix "$MANIFEST")"
RECORDED_STATE_DIR="$(jq -r .state_dir "$MANIFEST")"
TF_DIR="$(jq -r .terraform_dir "$MANIFEST")"
DRIVER_MACHINE_TYPE="$(jq -r .driver_machine_type "$MANIFEST")"
MAX_WORKERS="$(jq -r .max_workers "$MANIFEST")"

validate_project "$PROJECT"
validate_region "$REGION"
validate_zone "$ZONE"
validate_run_id "$RUN_ID"
validate_pool "$RUN_ID" "$POOL"
[[ "$ZONE" == "$REGION-"* ]]
[[ "$MAX_WORKERS" == 1 ]]
[[ "$PREFIX" == "gh-e2e-${RUN_ID}" && "$RECORDED_STATE_DIR" == "$STATE_DIR" ]] || {
  echo "error: manifest run prefix/state directory mismatch" >&2
  exit 2
}
[[ "$TF_DIR" == "$STATE_DIR/terraform" && -d "$TF_DIR" ]] || {
  echo "error: Terraform directory escapes or is missing from state directory" >&2
  exit 2
}

FAIL=0
for key in driver_orchestrator coordinator_runtime worker_runtime; do
  email="$(jq -r --arg key "$key" '.service_accounts[$key] // empty' "$MANIFEST")"
  [[ "$email" =~ ^gh[dcw]-${RUN_ID}@${PROJECT}\.iam\.gserviceaccount\.com$ ]] || {
    echo "error: service account does not match this exact run: $email" >&2
    exit 2
  }
  if ! keys="$(gcloud iam service-accounts keys list --iam-account "$email" --project "$PROJECT" \
    --managed-by=user --format='value(name)')"; then
    echo "error: failed to list user-managed keys for $email; refusing destroy" >&2
    exit 1
  fi
  [[ -z "$keys" ]] || { echo "error: user-managed keys exist for $email: $keys" >&2; FAIL=1; }
done

_guard_nested_pool_instances "$PROJECT" "$ZONE" "$POOL" || exit 1
rows="$NESTED_POOL_INSTANCE_ROWS"
count="$NESTED_POOL_INSTANCE_COUNT"
if [[ -n "$rows" ]]; then
  ((count <= MAX_WORKERS + 1)) || {
    echo "error: refusing to delete $count instances; manifest maximum is $((MAX_WORKERS + 1))" >&2
    exit 2
  }

  while IFS=$'\t' read -r name instance_zone; do
    [[ -n "$name" && -n "$instance_zone" ]] || continue
    echo "Deleting exact run-owned pool VM $name ($instance_zone)..."
    timeout --foreground 300s gcloud compute instances delete "$name" --project "$PROJECT" \
      --zone "$instance_zone" --quiet || FAIL=1
  done <<<"$rows"
fi

# Auto-delete should remove boot disks. Delete only anchored leftovers with the
# exact VM names recorded by this one-worker topology.
if ! disk_rows="$(gcloud compute disks list --project "$PROJECT" \
  --filter="name~'^${POOL}-(coordinator|worker-0)$'" \
  --format='value(name,zone.basename())')"; then
  echo "error: failed to list exact-name nested pool disks; refusing Terraform destroy" >&2
  exit 1
fi
if [[ -n "$disk_rows" ]]; then
  while IFS=$'\t' read -r name disk_zone; do
    [[ ( "$name" == "${POOL}-coordinator" || "$name" == "${POOL}-worker-0" ) && "$disk_zone" == "$ZONE" ]]
    echo "Deleting exact run-owned orphan disk $name ($disk_zone)..."
    timeout --foreground 300s gcloud compute disks delete "$name" \
      --project "$PROJECT" --zone "$disk_zone" --quiet || FAIL=1
  done <<<"$disk_rows"
fi

# manage_firewall=false means this should not exist, but remove only the exact
# legacy name if an interrupted CLI created it.
POOL_FIREWALL="allow-hail-coord-int-${POOL}"
if ! firewall_rows="$(gcloud compute firewall-rules list --project "$PROJECT" \
  --filter="name=${POOL_FIREWALL}" --format='value(name)')"; then
  echo "error: failed to list exact nested pool firewall; refusing Terraform destroy" >&2
  exit 1
fi
if [[ -n "$firewall_rows" ]]; then
  [[ "$firewall_rows" == "$POOL_FIREWALL" ]] || {
    echo "error: unexpected firewall rows: $firewall_rows" >&2
    exit 2
  }
  timeout --foreground 300s gcloud compute firewall-rules delete "$POOL_FIREWALL" \
    --project "$PROJECT" --quiet || FAIL=1
fi

if [[ "$FAIL" != 0 ]]; then
  echo "error: nested cleanup failed; refusing Terraform destroy" >&2
  exit 1
fi
verify_nested_pool_empty_stable "$PROJECT" "$ZONE" "$POOL" || {
  echo "error: nested pool is not stably empty; refusing Terraform destroy" >&2
  exit 1
}

if [[ -f "$TF_DIR/terraform.tfstate" ]] || [[ -d "$TF_DIR/.terraform" ]]; then
  echo "Destroying Terraform-managed E2E resources..."
  timeout --foreground 1200s terraform -chdir="$TF_DIR" destroy -input=false -auto-approve \
    -var="project_id=$PROJECT" \
    -var="region=$REGION" \
    -var="zone=$ZONE" \
    -var="run_id=$RUN_ID" \
    -var="driver_machine_type=$DRIVER_MACHINE_TYPE" || FAIL=1
fi

strict_capture() {
  [[ $# -ge 3 ]] || return 2
  local target="$1" label="$2" value
  shift 2
  if ! value="$("$@")"; then
    echo "error: residual query failed for $label" >&2
    FAIL=1
    value="<query-failed:$label>"
  fi
  printf -v "$target" '%s' "$value"
}

strict_capture remaining_instances instances gcloud compute instances list --project "$PROJECT" \
  --filter="tags.items:pool-${POOL} OR labels.run_id=${RUN_ID}" --format='value(name)'
strict_capture remaining_disks disks gcloud compute disks list --project "$PROJECT" \
  --filter="name~'^(${PREFIX}|${POOL})'" --format='value(name)'
strict_capture remaining_addresses addresses gcloud compute addresses list --project "$PROJECT" \
  --filter="name~'^(${PREFIX}|${POOL})'" --format='value(name)'
strict_capture remaining_firewall firewalls gcloud compute firewall-rules list --project "$PROJECT" \
  --filter="name~'^(${PREFIX}|${POOL_FIREWALL}$)'" --format='value(name)'
strict_capture remaining_network networks gcloud compute networks list --project "$PROJECT" \
  --filter="name=${PREFIX}-net" --format='value(name)'
strict_capture remaining_subnet subnet gcloud compute networks subnets list --project "$PROJECT" --regions "$REGION" \
  --filter="name=${PREFIX}-subnet" --format='value(name)'
strict_capture remaining_router router gcloud compute routers list --project "$PROJECT" --regions "$REGION" \
  --filter="name=${PREFIX}-router" --format='value(name)'
strict_capture remaining_buckets buckets gcloud storage buckets list --project "$PROJECT" \
  --filter="labels.run_id=${RUN_ID}" --format='value(name)'
[[ -z "$remaining_instances" ]] || { echo "error: instances remain: $remaining_instances" >&2; FAIL=1; }
[[ -z "$remaining_disks" ]] || { echo "error: disks remain: $remaining_disks" >&2; FAIL=1; }
[[ -z "$remaining_addresses" ]] || { echo "error: addresses remain: $remaining_addresses" >&2; FAIL=1; }
[[ -z "$remaining_firewall" ]] || { echo "error: firewalls remain: $remaining_firewall" >&2; FAIL=1; }
[[ -z "$remaining_network" ]] || { echo "error: network remains: $remaining_network" >&2; FAIL=1; }
[[ -z "$remaining_subnet" ]] || { echo "error: subnet remains: $remaining_subnet" >&2; FAIL=1; }
[[ -z "$remaining_router" ]] || { echo "error: router remains: $remaining_router" >&2; FAIL=1; }
if [[ -n "$remaining_buckets" ]]; then
  echo "error: buckets/objects remain: $remaining_buckets" >&2
  while IFS= read -r bucket; do
    gcloud storage ls "gs://${bucket}/**" >&2 2>/dev/null || true
  done <<<"$remaining_buckets"
  FAIL=1
fi

for key in driver_orchestrator coordinator_runtime worker_runtime; do
  email="$(jq -r --arg key "$key" '.service_accounts[$key] // empty' "$MANIFEST")"
  [[ -n "$email" ]] || continue
  strict_capture remaining_sa "service account $email" \
    gcloud iam service-accounts list --project "$PROJECT" \
    --filter="email=${email}" --format='value(email)'
  [[ -z "$remaining_sa" ]] || {
    echo "error: Terraform service account remains: $remaining_sa" >&2
    FAIL=1
  }
  strict_capture residual_member "IAM memberships for $email" \
    gcloud projects get-iam-policy "$PROJECT" \
    --flatten='bindings[].members' \
    --filter="bindings.members:serviceAccount:${email}" \
    --format='value(bindings.role)'
  [[ -z "$residual_member" ]] || {
    echo "error: project IAM memberships remain for $email: $residual_member" >&2
    FAIL=1
  }
done

if [[ -f "$TF_DIR/terraform.tfstate" ]]; then
  strict_capture residual_state "Terraform state" terraform -chdir="$TF_DIR" state list
  [[ -z "$residual_state" ]] || {
    echo "error: Terraform state still contains resources: $residual_state" >&2
    FAIL=1
  }
fi

if [[ "$FAIL" != 0 ]]; then
  echo "error: cleanup incomplete for $RUN_ID; inspect $STATE_DIR" >&2
  exit 1
fi

echo "Cleanup complete with no run-scoped instance/disk/address/firewall/network/bucket/IAM/Terraform residuals for $RUN_ID."
