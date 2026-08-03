#!/usr/bin/env bash
# Canonical VM/service/process/deployment snapshots for lifecycle-free assertions.
set -Eeuo pipefail

capture_worker_snapshot() {
  local project="$1" zone="$2" pool="$3" output="$4"
  local worker="${pool}-worker-0" tmpdir encoded
  tmpdir="$(mktemp -d)"

  gcloud compute instances describe "$worker" --project "$project" --zone "$zone" \
    --format=json >"$tmpdir/vm.json"

  encoded="$(base64 <<'PY' | tr -d '\n'
import hashlib
import json
import os
import pathlib
import subprocess

def command(*args):
    return subprocess.check_output(args, text=True).strip()

def optional_command(*args):
    try:
        return command(*args)
    except Exception:
        return None

def digest(path):
    try:
        h = hashlib.sha256()
        with open(path, "rb") as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b""):
                h.update(block)
        return h.hexdigest()
    except Exception:
        return None

pid_text = optional_command("systemctl", "show", "genohype-worker.service", "-p", "MainPID", "--value") or "0"
pid = int(pid_text)
unit_path = optional_command("systemctl", "show", "genohype-worker.service", "-p", "FragmentPath", "--value")
properties = {}
for key in ("ActiveState", "SubState", "ActiveEnterTimestampMonotonic", "ExecMainStartTimestampMonotonic", "NRestarts"):
    properties[key] = optional_command("systemctl", "show", "genohype-worker.service", "-p", key, "--value")
start_ticks = None
exe = None
process_sha = None
if pid > 0:
    try:
        start_ticks = pathlib.Path(f"/proc/{pid}/stat").read_text().split()[21]
        exe = os.readlink(f"/proc/{pid}/exe")
        process_sha = digest(f"/proc/{pid}/exe")
    except Exception:
        pass
deployment_path = pathlib.Path("/var/lib/genohype/bootstrap-ready.json")
try:
    deployment = json.loads(deployment_path.read_text())
except Exception:
    deployment = None
print(json.dumps({
    "service": {"MainPID": pid, "FragmentPath": unit_path, **properties,
                "unit_sha256": digest(unit_path) if unit_path else None},
    "process": {"start_ticks": start_ticks, "executable": exe,
                "process_sha256": process_sha,
                "installed_sha256": digest("/usr/local/bin/genohype")},
    "deployment": deployment,
}, sort_keys=True, separators=(",", ":")))
PY
)"
  gcloud compute ssh "$worker" --project "$project" --zone "$zone" \
    --tunnel-through-iap --quiet --command \
    "printf '%s' '$encoded' | base64 -d | sudo python3 -" >"$tmpdir/runtime.json"

  jq -S -n --slurpfile vm "$tmpdir/vm.json" --slurpfile runtime "$tmpdir/runtime.json" '
    ($vm[0]) as $v | ($runtime[0]) as $r |
    {
      worker_id: $v.name,
      vm: {
        id: $v.id,
        status: $v.status,
        creation_timestamp: $v.creationTimestamp,
        last_start_timestamp: ($v.lastStartTimestamp // null),
        zone: $v.zone,
        service_account: $v.serviceAccounts[0].email,
        network: $v.networkInterfaces[0].network,
        subnetwork: $v.networkInterfaces[0].subnetwork,
        network_ip: $v.networkInterfaces[0].networkIP,
        tags: ($v.tags.items // [] | sort),
        boot_disk_source: $v.disks[0].source,
        boot_disk_device: $v.disks[0].deviceName
      },
      service: $r.service,
      process: $r.process,
      deployment: $r.deployment
    }
  ' >"$output"
  rm -rf "$tmpdir"
}

capture_gcs_object_snapshot() {
  local output="$1"
  shift
  local tmpdir url name
  tmpdir="$(mktemp -d)"
  : >"$tmpdir/objects.ndjson"
  for url in "$@"; do
    name="$(printf '%s' "$url" | sha256sum | awk '{print $1}')"
    gcloud storage objects describe "$url" --format=json >"$tmpdir/$name.json"
    jq -c '{name,generation,metageneration,size,updateTime,md5Hash,crc32c}' \
      "$tmpdir/$name.json" >>"$tmpdir/objects.ndjson"
  done
  jq -S -s 'sort_by(.name)' "$tmpdir/objects.ndjson" >"$output"
  rm -rf "$tmpdir"
}

assert_snapshot_unchanged() {
  local before="$1" after="$2"
  cmp -s "$before" "$after" || {
    diff -u "$before" "$after" >&2 || true
    echo "worker VM/service/process/deployment snapshot changed unexpectedly" >&2
    return 1
  }
}

assert_recreated_generation() {
  local before="$1" after="$2"
  [[ "$(jq -r '.worker_id' "$before")" == "$(jq -r '.worker_id' "$after")" ]]
  [[ "$(jq -r '.vm.id' "$before")" != "$(jq -r '.vm.id' "$after")" ]] || {
    echo "recreated worker did not receive a new GCE instance ID" >&2
    return 1
  }
  [[ "$(jq -r '.deployment.deployment_id' "$before")" != \
     "$(jq -r '.deployment.deployment_id' "$after")" ]] || {
    echo "recreated worker did not receive a fresh deployment ID" >&2
    return 1
  }
}
