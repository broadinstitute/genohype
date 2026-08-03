#!/usr/bin/env bash
# Shared strict validation and phase receipts for the GCP E2E harness.

fail() {
  echo "error: $*" >&2
  return 1
}

utc_now() {
  date -u +%Y-%m-%dT%H:%M:%SZ
}

validate_project() {
  [[ "$1" =~ ^[a-z][a-z0-9-]{4,28}[a-z0-9]$ ]] || fail "invalid GCP project ID: $1"
}

validate_region() {
  [[ "$1" =~ ^[a-z]+-[a-z]+[0-9]$ ]] || fail "invalid GCP region: $1"
}

validate_zone() {
  [[ "$1" =~ ^[a-z]+-[a-z]+[0-9]-[a-z]$ ]] || fail "invalid GCP zone: $1"
}

validate_run_id() {
  [[ "$1" =~ ^[a-z][a-z0-9-]{5,24}$ ]] || fail "RUN_ID must be 6-25 lowercase letters, digits, or hyphens"
}

validate_pool() {
  local run_id="$1" pool="$2"
  [[ "$pool" == "gh-e2e-$run_id" ]] || fail "pool does not exactly match run ID"
}

validate_positive_timeout() {
  local label="$1" value="$2" maximum="$3"
  [[ "$value" =~ ^[1-9][0-9]*$ ]] || fail "$label must be a positive integer"
  ((value <= maximum)) || fail "$label exceeds maximum $maximum"
}

require_command() {
  command -v "$1" >/dev/null || fail "$1 is required"
}

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

run_phase() {
  [[ $# -ge 7 ]] || fail "run_phase requires result-dir, run-id, revision, scenario, phase, timeout, command"
  local results="$1" run_id="$2" revision="$3" scenario="$4" phase="$5" timeout_secs="$6"
  shift 6
  [[ "$scenario" =~ ^[a-z0-9-]+$ && "$phase" =~ ^[a-z0-9-]+$ ]] \
    || fail "unsafe receipt scenario/phase name"
  validate_positive_timeout phase-timeout "$timeout_secs" 3600
  mkdir -p "$results/logs" "$results/receipts"
  local log="$results/logs/${scenario}-${phase}.log"
  local receipt="$results/receipts/${scenario}-${phase}.json"
  local started ended duration rc status tmp
  started="$(date +%s)"
  printf 'phase=%s scenario=%s started=%s command=' "$phase" "$scenario" "$(utc_now)" | tee "$log"
  printf '%q ' "$@" | tee -a "$log"
  printf '\n' | tee -a "$log"

  set +e
  timeout --foreground "${timeout_secs}s" "$@" 2>&1 | tee -a "$log"
  rc=${PIPESTATUS[0]}
  set -e
  ended="$(date +%s)"
  duration=$(((ended - started) * 1000))
  status=passed
  [[ "$rc" == 0 ]] || status=failed
  tmp="$receipt.tmp.$$"
  jq -n --arg run_id "$run_id" --arg revision "$revision" \
    --arg scenario "$scenario" --arg phase "$phase" --arg status "$status" \
    --arg log "$log" --argjson exit_code "$rc" --argjson duration_ms "$duration" \
    '{schema_version:1,run_id:$run_id,revision:$revision,scenario:$scenario,
      phase:$phase,status:$status,exit_code:$exit_code,duration_ms:$duration_ms,log:$log}' >"$tmp"
  chmod 0600 "$tmp"
  mv "$tmp" "$receipt"
  return "$rc"
}
