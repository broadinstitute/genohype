#!/usr/bin/env bash
# Exact one-worker nested-pool guards shared by guest scenarios and cleanup.

# These globals carry validated rows/count to callers after the guard returns.
# shellcheck disable=SC2034
NESTED_POOL_INSTANCE_ROWS=""
NESTED_POOL_INSTANCE_COUNT=0

_guard_nested_pool_instances() {
  [[ $# -eq 3 ]] || return 2
  local project="$1" zone="$2" pool="$3" rows name instance_zone count=0
  local coordinator_seen=0 worker_seen=0

  if ! rows="$(gcloud compute instances list --project "$project" \
    --filter="tags.items:pool-${pool}" --format='value(name,zone.basename())')"; then
    echo "error: failed to list exact-tag nested pool instances; refusing cleanup" >&2
    return 1
  fi

  if [[ -n "$rows" ]]; then
    while IFS=$'\t' read -r name instance_zone; do
      [[ -n "$name" && "$instance_zone" == "$zone" ]] || {
        echo "error: refusing malformed or wrong-zone exact-tag instance: ${name:-<empty>} ${instance_zone:-<empty>}" >&2
        return 1
      }
      case "$name" in
        "${pool}-coordinator")
          ((coordinator_seen += 1))
          ((coordinator_seen == 1)) || {
            echo "error: duplicate coordinator row in nested-pool listing" >&2
            return 1
          }
          ;;
        "${pool}-worker-0")
          ((worker_seen += 1))
          ((worker_seen == 1)) || {
            echo "error: duplicate worker row in nested-pool listing" >&2
            return 1
          }
          ;;
        *)
          echo "error: refusing unexpected exact-tag instance: $name" >&2
          return 1
          ;;
      esac
      ((count += 1))
    done <<<"$rows"
  fi
  ((count <= 2)) || {
    echo "error: refusing $count exact-tag instances; one-worker ceiling is 2 VMs" >&2
    return 1
  }
  # Consumed by cleanup.sh after this sourced function returns.
  # shellcheck disable=SC2034
  NESTED_POOL_INSTANCE_ROWS="$rows"
  NESTED_POOL_INSTANCE_COUNT="$count"
}

_verify_nested_pool_empty_once() {
  [[ $# -eq 3 ]] || return 2
  local project="$1" zone="$2" pool="$3" rows name resource_zone

  _guard_nested_pool_instances "$project" "$zone" "$pool" || return 1
  ((NESTED_POOL_INSTANCE_COUNT == 0)) || return 1

  if ! rows="$(gcloud compute disks list --project "$project" \
    --filter="name~'^${pool}-(coordinator|worker-0)$'" \
    --format='value(name,zone.basename())')"; then
    echo "error: failed to list nested pool disks" >&2
    return 1
  fi
  if [[ -n "$rows" ]]; then
    while IFS=$'\t' read -r name resource_zone; do
      [[ "$resource_zone" == "$zone" && ( "$name" == "${pool}-coordinator" || "$name" == "${pool}-worker-0" ) ]] || {
        echo "error: unexpected nested pool disk row: $name $resource_zone" >&2
        return 1
      }
    done <<<"$rows"
    echo "error: nested pool disks remain: $rows" >&2
    return 1
  fi

  if ! rows="$(gcloud compute firewall-rules list --project "$project" \
    --filter="name=allow-hail-coord-int-${pool}" --format='value(name)')"; then
    echo "error: failed to list nested pool firewall" >&2
    return 1
  fi
  [[ -z "$rows" ]] || {
    [[ "$rows" == "allow-hail-coord-int-${pool}" ]] || {
      echo "error: unexpected nested pool firewall rows: $rows" >&2
      return 1
    }
    echo "error: nested pool firewall remains: $rows" >&2
    return 1
  }
}

verify_nested_pool_empty_stable() {
  [[ $# -eq 3 ]] || return 2
  local attempt
  for attempt in $(seq 1 12); do
    if _verify_nested_pool_empty_once "$@"; then
      sleep 3
      if _verify_nested_pool_empty_once "$@"; then
        return 0
      fi
    fi
    [[ "$attempt" == 12 ]] || sleep 5
  done
  echo "error: nested pool did not reach two stable-empty observations" >&2
  return 1
}

destroy_nested_pool_guarded() {
  [[ $# -eq 6 ]] || {
    echo "destroy_nested_pool_guarded requires project, zone, pool, config, marker, genohype binary" >&2
    return 2
  }
  local project="$1" zone="$2" pool="$3" config="$4" marker="$5" binary="$6"

  _guard_nested_pool_instances "$project" "$zone" "$pool" || return 1
  if ((NESTED_POOL_INSTANCE_COUNT > 0)); then
    timeout --foreground 300s "$binary" --config "$config" pool destroy "$pool" || {
      echo "error: product pool destroy failed; preserving marker and evidence" >&2
      return 1
    }
  fi
  verify_nested_pool_empty_stable "$project" "$zone" "$pool" || return 1
  rm -f "$marker"
}
