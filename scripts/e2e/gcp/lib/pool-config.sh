#!/usr/bin/env bash
# Generate the two deliberately distinct pool profiles used by the guest suite.

write_pool_config() {
  [[ $# -eq 11 ]] || {
    echo "write_pool_config requires path, profile, project, zone, network, subnet, pool, worker SA, coordinator SA, bucket, release dir" >&2
    return 2
  }
  local path="$1" profile="$2" project="$3" zone="$4" network="$5" subnet="$6"
  local pool="$7" worker_sa="$8" coordinator_sa="$9" bucket="${10}" release_dir="${11}"
  local tmp="${path}.tmp.$$"

  [[ "$profile" == stock || "$profile" == attested-custom ]] || {
    echo "unsupported pool profile: $profile" >&2
    return 2
  }

  cat >"$tmp" <<EOF
[defaults]
project = "$project"
zone = "$zone"
network = "$network"
subnet = "$subnet"
public_ip = false
manage_firewall = false

[pools.$pool]
machine_type = "e2-standard-2"
starting_workers = 0
workers = 1
spot = false
with_coordinator = true
service_account = "$worker_sa"
coordinator_service_account = "$coordinator_sa"
pool_db_path = "gs://${bucket}/ops.db"
EOF

  if [[ "$profile" == attested-custom ]]; then
    cat >>"$tmp" <<EOF
worker_binary = "${release_dir}/custom-worker/fixture-a"
worker_attestation = "${release_dir}/custom-worker/fixture-a.attestation.json"
EOF
  fi

  chmod 0600 "$tmp"
  mv "$tmp" "$path"
}
