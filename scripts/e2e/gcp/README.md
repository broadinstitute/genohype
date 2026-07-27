# GCP Build/Install/Pool E2E

This harness builds the current x86_64 Linux commit, packages installer-compatible archives, installs them on a fresh GCE driver VM, runs deterministic core checks, creates a one-coordinator/one-worker private pool, runs stress and distributed Parquet smoke tests, probes the dashboard through IAP, and destroys the run.

Use a dedicated non-production GCP project. `gnomadev` is suitable for initial manual runs; a dedicated `genohype-e2e` project is preferable for recurring CI.

## Safety model

Every resource receives a unique run prefix. Terraform creates a dedicated:

- service account and additive IAM memberships;
- VPC, subnet, router, and Cloud NAT;
- IAP SSH and internal coordinator firewall rules;
- force-destroy test bucket with a one-day lifecycle rule;
- Ubuntu x86_64 driver VM.

Genohype creates only the coordinator and worker inside that VPC. The wrapper destroys exact pool-tagged VMs before destroying Terraform resources. It never reuses or modifies an existing VPC, bucket, service account, firewall rule, or Terraform state.

The test account temporarily receives broad `compute.admin` permission because it provisions and manages nested pool VMs. The account and additive IAM members are deleted with the run.

## Requirements

On the build host:

- authenticated `gcloud` with permission to create the listed resources and additive IAM members;
- Terraform 1.5+;
- Rust, Zig, and `cargo-zigbuild`;
- Node/npm for the embedded dashboard build;
- `jq` and standard archive/checksum tools.

Required project APIs must already be enabled. The harness deliberately does not enable or disable shared project services.

## Review the plan

Plan is the default mode and creates no resources:

```bash
PROJECT=gnomadev ./scripts/e2e/gcp/run.sh plan
```

Review that every proposed resource begins with the unique `gh-e2e-...` run prefix or the corresponding `genohype-e2e-...` bucket prefix.

## Run

```bash
PROJECT=gnomadev \
REGION=us-east1 \
ZONE=us-east1-b \
  ./scripts/e2e/gcp/run.sh apply
```

The default topology is one `e2-standard-2` driver, one `e2-standard-2` coordinator, and one `e2-standard-2` worker. Pool VMs have no external addresses; Cloud NAT supplies outbound package access and Private Google Access supplies GCS access.

The run succeeds only after emitting:

```text
GENOHYPE_POOL_E2E_OK
```

The wrapper then destroys all resources even on failure. It prints the state directory containing logs, the Terraform state, and a recovery manifest.

## Recover an interrupted run

If the host process is killed before its exit trap runs:

```bash
./scripts/e2e/gcp/cleanup.sh /tmp/genohype-e2e-<run-id>
```

The recovery command deletes only VMs carrying the exact pool tag recorded in `run.env`, then destroys that run's Terraform state and verifies no matching instances remain.

For diagnosis, `KEEP_ON_FAILURE=1` retains Terraform resources after a failed run. This is opt-in and billable; always invoke `cleanup.sh` afterward.
