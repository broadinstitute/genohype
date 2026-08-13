# GCP clean-room one-worker E2E

This plan-first harness builds the current x86_64 Linux revision on the host, installs only packaged artifacts on a fresh private Ubuntu driver, runs deterministic core checks, and exercises a private coordinator plus one **on-demand** worker. There is no performance/32-worker mode and no coordinator-driven scaling in this slice.

> [!IMPORTANT]
> This harness tests legacy distributed Parquet and pool/custom-worker lifecycle behavior, **not publication-safe export**. Safe export is pre-production and only local/mock qualified; no cloud pilot is approved. Adding a separate safe-export cloud scenario requires explicit approval for the exact project, buckets/prefixes, pool, worker artifact, and cleanup plan. Do not turn `core-smoke.sh` into a production-like safe-export write.

The default `pool-lifecycle` scenario exercises the complete requested contract. It is capability-gated: if attested bootstrap or detached job-ID output is unavailable, preflight lists every dependency and exits before creating a pool. It never guesses flags or silently skips assertions.

The lifecycle scenario performs:

1. clean packaged installation and revision verification;
2. coordinator-only create (`--workers 0`);
3. scale `0 → 1` with the handler-backed custom worker and canonical attestation;
4. exact readiness plus VM/service/process/executable/deployment snapshots;
5. detached custom submit with an exact durable job ID, exact status/receipt polling, and an unchanged warm-worker assertion;
6. scale `1 → 0 → 1`, proving the reused worker name has a fresh VM identity;
7. pool destroy and exact instance/disk/firewall leak checks.

No compilation occurs on the guest.

## Identity and network topology

Terraform creates three distinct, keyless, run-scoped service accounts using only additive IAM member resources:

| Identity | Attached to | Scoped access |
|---|---|---|
| driver/orchestrator | disposable driver VM | project `compute.instanceAdmin.v1`, OS Login/IAP/service usage; network use on only the run subnet; `actAs` on only the two runtime accounts; object admin on only the run bucket |
| coordinator runtime | coordinator VM | project `compute.viewer`; object user on only the run bucket for coordinator DB/artifact access |
| worker runtime | worker VM | object viewer + creator on only the run bucket for inputs, artifacts, and unique outputs |

Coordinator and worker identities have no Compute mutation or IAM role. No service-account key resource exists; the harness checks that all three accounts have no user-managed keys. Authentication uses metadata credentials and the `cloud-platform` OAuth scope.

The isolated driver, coordinator, and worker have no public address. Cloud NAT supplies egress, Private Google Access supplies GCS access, and IAP supplies administrative SSH. Terraform owns the run VPC, `/24` subnet, router/NAT, IAP and internal-coordinator firewall rules, force-destroy bucket, identities, and driver. Pool profiles set `public_ip = false` and `manage_firewall = false`.

The custom-worker sidecar is an integrity/provenance binding, not a publisher signature. Its schema exactly matches the product's version-1 Rust sidecar: `schema_version`, SHA-256, size, full 40- or 64-hex revision, build identity/version, platform, and architecture. The generator emits deterministic canonical JSON, while validation accepts any equivalent JSON representation accepted by Rust. Protocol compatibility belongs to the fixture's runtime identity JSON and is not an unused sidecar claim. Host tests reject altered bytes, malformed or unknown claims, and the wrong ELF architecture.

## Requirements and cost

Use a dedicated non-production project. The operator has explicitly selected shared `gnomadev` for this bounded manual run. **During an apply, the ephemeral driver has the predefined project-wide `roles/compute.instanceAdmin.v1` role.** This is broader than the run prefix: a compromised or defective driver could mutate unrelated Compute resources in that project. The harness does not add further privilege and does not claim an untested IAM condition/custom role. It mitigates the accepted risk by requiring an exact project confirmation plus a fresh unique run ID/prefix, printing a preflight warning, fixing topology at one worker, and tearing down immediately unless explicit billable retention is requested. Required APIs must already be enabled; the harness never enables/disables shared APIs.

Host requirements: authenticated `gcloud`, Terraform 1.5+, Rust, Zig, `cargo-zigbuild`, Node/npm, Python 3, `jq`, and standard archive/checksum tools.

An apply briefly bills one `e2-standard-2` driver, one `e2-standard-2` coordinator, one `e2-standard-2` worker, three small balanced boot disks, Cloud NAT, and small GCS/network usage. Actual cost follows current GCP regional pricing and runtime. The default cleanup runs on success and failure. Retention is explicit, billable, and has no automatic expiry.

## Plan in `gnomadev` (default; creates nothing)

Use a fresh run ID so the saved state and plan path are unambiguous:

```bash
RUN_ID=r$(date -u +%Y%m%dt%H%M%Sz)
PROJECT=gnomadev \
CONFIRM_PROJECT=gnomadev \
RUN_ID="$RUN_ID" \
  ./scripts/e2e/gcp/run.sh plan
```

`CONFIRM_PROJECT` is accepted in plan mode for copy/paste symmetry but only enforced by apply. Plan mode performs no build and creates no GCP resources. Review:

```text
/tmp/genohype-e2e-<run-id>/plan.tfplan
/tmp/genohype-e2e-<run-id>/plan.json
```

The guard permits isolated creates only and rejects updates, replacements, destroys, unexpected resource types, policy-replacing IAM, service-account keys, `roles/compute.admin`, and a driver public IP.

## Apply the one-worker lifecycle smoke

Apply requires exact confirmation of both the project and the unique run ID:

```bash
RUN_ID=r$(date -u +%Y%m%dt%H%M%Sz)
PROJECT=gnomadev \
CONFIRM_PROJECT=gnomadev \
RUN_ID="$RUN_ID" \
CONFIRM_RUN_ID="$RUN_ID" \
KEEP_ON_FAILURE=0 \
  ./scripts/e2e/gcp/run.sh apply
```

The run emits `GENOHYPE_POOL_E2E_OK` only after all requested guest receipts pass and the pool is destroyed. The host retrieves/checksums guest evidence before Terraform cleanup. The outer guest timeout is derived from setup, requested scenario, lifecycle preflight, guarded cleanup, and transport-margin budgets; a user override cannot be lower than that sum.

`SCENARIOS=core-smoke` remains available for the stock-worker stress/Parquet smoke. The guest generates two separate profiles: the stock profile has no `worker_binary` or `worker_attestation`, while only `pool-lifecycle` receives the attested custom-fixture profile. Contract tests parse the generated TOML to enforce that boundary.

## Retention and interrupted-run recovery

`KEEP_ON_FAILURE=1` requires `ALLOW_BILLABLE=1`. On failure it retains **both** the nested pool and Terraform resources, retrieves evidence first, records `retained=true`, and prints the exact cleanup command. Successful runs always destroy resources regardless of KEEP.

Recover an interrupted or retained run using its JSON manifest (which contains identifiers, never credentials):

```bash
./scripts/e2e/gcp/cleanup.sh /tmp/genohype-e2e-<run-id>
```

Cleanup revalidates the project/zone/run/pool/account relationship. Guest cleanup preserves its marker whenever guarded destroy or verification fails. Nested deletion requires the exact pool tag, the anchored names `<pool>-coordinator` and `<pool>-worker-0`, the exact zone, and at most one worker plus one coordinator. List/API failures are errors, never empty success. The host requires two stable-empty nested-pool observations before Terraform destroy, then checks run-scoped instances, disks, addresses, firewalls, network/subnet/router, bucket/objects, all three accounts, project IAM members, user-managed keys, and residual Terraform state.

## Local verification

```bash
cargo fmt --all -- --check
cargo test -p genohype-e2e-custom-worker --locked
python3 -m unittest discover -s scripts/e2e/gcp/tests -p 'test_*.py'
find scripts/e2e/gcp -name '*.sh' -print0 | xargs -0 -n1 bash -n
find scripts/e2e/gcp -name '*.sh' -print0 | xargs -0 shellcheck
terraform -chdir=scripts/e2e/gcp/terraform fmt -check
terraform -chdir=scripts/e2e/gcp/terraform init -backend=false
terraform -chdir=scripts/e2e/gcp/terraform validate
```
