# Export gnomAD v4.1.1 to Parquet

**Start with the project [installation instructions](../../README.md#installation).** For the pool exports, also review the [minimal GCP pool walkthrough](minimal.md), which explains resource observation, IAP dashboard access, private networking, and cleanup in more detail.

This guide exports the gnomAD v4.1.1 browser sites Hail Table in three progressively larger ways:

1. a small interval on the local machine, with no pool infrastructure;
2. chromosome 22 on a small GCP pool;
3. the complete table on a larger GCP pool.

Source table:

```text
gs://gcp-public-data--gnomad/release/4.1.1/ht/browser/gnomad.browser.v4.1.1.sites.ht
```

> [!WARNING]
> The pool examples create billable Compute Engine resources and write potentially large Parquet outputs to your GCS bucket. The complete export can consume substantial compute, storage, API quota, and time. Start with the local interval and chromosome 22 workflows before attempting it.

## Common setup

Set the source once:

```bash
export INPUT="gs://gcp-public-data--gnomad/release/4.1.1/ht/browser/gnomad.browser.v4.1.1.sites.ht"
```

Genohype's current GCS client uses Google Application Default Credentials, including for public buckets. On a workstation, initialize them with:

```bash
gcloud auth application-default login
genohype info "$INPUT"
```

On a GCE VM, the attached service account is discovered automatically instead; it must have the required storage permissions.

`genohype info` reads table metadata without scanning the complete dataset. Confirm that it reports the expected `locus, alleles` keyed variant table and note its partition count before continuing. At the time this guide was validated, v4.1.1 reported 9,694 source partitions.

All examples below use the default `full` Parquet schema. For browser-focused experiments, `--width browser-minimal` can reduce a **local filtered export**, but that projection is not currently supported for full-table, sharded, or pool exports.

---

## 1. Export a small interval locally

This workflow reads GCS directly from the local machine and creates no GCP VM, disk, or firewall resources.

Choose a small interval near the end of chromosome 22:

```bash
export INTERVAL="chr22:50000000-50010000"
export LOCAL_OUTPUT="output/gnomad-v4.1.1-chr22-50m-50.01m.parquet"
mkdir -p "$(dirname "$LOCAL_OUTPUT")"
```

First inspect a few matching variants:

```bash
genohype query "$INPUT" --interval "$INTERVAL" --limit 5
```

Then export the interval to one local Parquet file:

```bash
time genohype export parquet \
  "$INPUT" \
  "$LOCAL_OUTPUT" \
  --interval "$INTERVAL" \
  --benchmark
```

Check the file:

```bash
ls -lh "$LOCAL_OUTPUT"
file "$LOCAL_OUTPUT"
```

As a clean-room baseline, the published v0.1.0 Linux release exported 5,166 rows and produced a 4.47 MiB file for this interval. Runtime and compressed size can vary by machine and library version, but the fixed source and interval should retain the same biological row count.

If DuckDB is installed, verify the row count and sample records:

```bash
duckdb -c "SELECT count(*) AS variants FROM read_parquet('$LOCAL_OUTPUT');"
duckdb -c "SELECT * FROM read_parquet('$LOCAL_OUTPUT') LIMIT 5;"
```

To test the narrower browser projection instead, use a different output file:

```bash
genohype export parquet \
  "$INPUT" \
  "output/gnomad-v4.1.1-browser-minimal.parquet" \
  --interval "$INTERVAL" \
  --width browser-minimal
```

### What this creates

Only the local files under `output/` are created. No `pool create` command is used, so this workflow provisions no Compute Engine instances, disks, or firewall rules.

---

## Pool prerequisites

The next two workflows require:

- a GCP project with billing and Compute Engine enabled;
- permission to create and delete VMs and firewall rules;
- a writable GCS bucket for Parquet output, with object-creation permission for the service account attached to pool VMs;
- `genohype-worker` from a release installation, or a source-built worker from `make worker`;
- enough regional CPU quota for the selected worker count and machine type.

Set the project, zone, and an existing output bucket:

```bash
export PROJECT="$(gcloud config get-value project)"
export ZONE="us-central1-a"
export OUTPUT_BUCKET="gs://YOUR-WRITABLE-BUCKET"

gcloud services enable compute.googleapis.com --project "$PROJECT"
gcloud storage ls "$OUTPUT_BUCKET"
command -v genohype-worker
```

Check whether the project has a default VPC:

```bash
gcloud compute networks describe default --project "$PROJECT"
```

If it does not, select an existing network and regional subnet:

```bash
gcloud compute networks list --project "$PROJECT"
gcloud compute networks subnets list --project "$PROJECT" \
  --filter="region:${ZONE%-*}"
```

Add the selected values under `[defaults]` in each pool profile below:

```toml
network = "YOUR_VPC"
subnet = "YOUR_SUBNET"
```

Use an output bucket near the source data and workers when possible. You can inspect bucket locations with:

```bash
gcloud storage buckets describe gs://gcp-public-data--gnomad \
  --format='value(location)'
gcloud storage buckets describe "$OUTPUT_BUCKET" \
  --format='value(location)'
```

The output prefixes below include a timestamp to prevent accidental mixing with a previous run:

```bash
export RUN_ID="$(date -u +%Y%m%dt%H%M%Sz)"
```

## Observe a pool and its output

For either pool workflow, keep these views open in separate terminals while `pool create` or `pool submit` runs.

### Compute Engine resources

```bash
while true; do
  clear
  date
  gcloud compute instances list \
    --project "$PROJECT" \
    --filter="name~'^${POOL}-'" \
    --format='table(name,status,zone.basename(),machineType.basename(),scheduling.provisioningModel,networkInterfaces[0].networkIP)'
  echo
  gcloud compute disks list \
    --project "$PROJECT" \
    --filter="name~'^${POOL}-'" \
    --format='table(name,status,sizeGb,zone.basename())'
  sleep 3
done
```

### Pool CLI

```bash
genohype --config "$GENOHYPE_CONFIG" pool status "$POOL"
genohype --config "$GENOHYPE_CONFIG" pool workers "$POOL"
genohype --config "$GENOHYPE_CONFIG" pool events "$POOL" --follow
```

### Dashboard through IAP

```bash
gcloud compute ssh "${POOL}-coordinator" \
  --project "$PROJECT" \
  --zone "$ZONE" \
  --tunnel-through-iap \
  -- -N -L 3000:localhost:3000
```

Then open [http://localhost:3000/dashboard](http://localhost:3000/dashboard). IAP is optional; it requires the IAM and SSH firewall policy described in the [minimal pool walkthrough](minimal.md#1-prerequisites). If port 3000 is occupied locally, forward `3001:localhost:3000` instead.

### GCS output

Set `OUTPUT_PREFIX` to the current workflow's output and run:

```bash
while true; do
  clear
  date
  gcloud storage ls "${OUTPUT_PREFIX}/**" 2>/dev/null | wc -l
  gcloud storage du -s "$OUTPUT_PREFIX" 2>/dev/null || true
  sleep 10
done
```

---

## 2. Export chromosome 22 on a small pool

This example uses one coordinator and two spot workers. The coordinator is always on-demand; `spot = true` applies to workers.

```bash
export POOL="gnomad-chr22-${RUN_ID}"
export OUTPUT_PREFIX="${OUTPUT_BUCKET%/}/genohype/gnomad-v4.1.1/chr22/${RUN_ID}"
export GENOHYPE_CONFIG="$(mktemp)"

cat > "$GENOHYPE_CONFIG" <<EOF
[defaults]
project = "$PROJECT"
zone = "$ZONE"

[pools.$POOL]
machine_type = "n1-standard-4"
starting_workers = 2
workers = 2
spot = true
with_coordinator = true
# If your project does not use the default Compute Engine service account,
# set one that can read the source and create objects in OUTPUT_BUCKET:
# service_account = "genohype-pool@YOUR_PROJECT.iam.gserviceaccount.com"
EOF
```

Start the resource observer, then create the pool in another terminal:

```bash
genohype --config "$GENOHYPE_CONFIG" pool create "$POOL" --wait
```

Confirm the coordinator and both workers are healthy, then start the dashboard tunnel and event observer:

```bash
genohype --config "$GENOHYPE_CONFIG" pool list "$POOL"
genohype --config "$GENOHYPE_CONFIG" pool workers "$POOL"
```

Submit chromosome 22 using its GRCh38 extent:

```bash
time genohype --config "$GENOHYPE_CONFIG" pool submit "$POOL" \
  -- \
  export parquet \
  "$INPUT" \
  "$OUTPUT_PREFIX" \
  --interval "chr22:1-50818468"
```

Pool exports write one `part-XXXXX.parquet` object per processed source partition. Do not add local-only `--shard-count` or `--per-partition` flags to a pool submission.

The current distributed interval implementation queues the source table's partitions and filters rows against chromosome 22 on workers. Consequently, the output can contain valid Parquet files with zero matching rows for partitions outside the interval. Compacting those files into a different layout is a separate downstream step.

Inspect the output:

```bash
gcloud storage ls "${OUTPUT_PREFIX}/**" | head
gcloud storage ls "${OUTPUT_PREFIX}/**" | wc -l
gcloud storage du -s "$OUTPUT_PREFIX"
```

Optionally copy a part locally and inspect it:

```bash
export SAMPLE_PART="$(gcloud storage ls "${OUTPUT_PREFIX}/**.parquet" | head -1)"
gcloud storage cp "$SAMPLE_PART" /tmp/gnomad-chr22-sample.parquet
duckdb -c "SELECT count(*) FROM read_parquet('/tmp/gnomad-chr22-sample.parquet');"
```

If the sampled part reports zero rows, it belongs to a source partition outside chromosome 22; the file is still a valid output of the current distributed interval workflow.

### Clean up the chromosome 22 pool

Stop the dashboard tunnel and observers, then delete the VMs:

```bash
genohype --config "$GENOHYPE_CONFIG" pool destroy "$POOL"
```

Verify instances and boot disks are gone, and remove the best-effort per-pool firewall rule if Genohype created it:

```bash
gcloud compute instances list --project "$PROJECT" --filter="name~'^${POOL}-'"
gcloud compute disks list --project "$PROJECT" --filter="name~'^${POOL}-'"

if gcloud compute firewall-rules describe "allow-hail-coord-int-${POOL}" \
  --project "$PROJECT" >/dev/null 2>&1; then
  gcloud compute firewall-rules delete "allow-hail-coord-int-${POOL}" \
    --project "$PROJECT" --quiet
fi

rm -f "$GENOHYPE_CONFIG"
```

The Parquet objects remain in your output bucket. Delete them only when you no longer need the result:

```bash
# Destructive: review the prefix before uncommenting.
# gcloud storage rm --recursive "$OUTPUT_PREFIX"
```

---

## 3. Export the complete dataset on a larger pool

Treat this as a production-sized operation. The example starts with eight `c3-highcpu-22` spot workers, but that is a starting point—not a universal recommendation. Machine availability, CPU quota, desired completion time, and observed memory pressure should determine the final shape.

Before provisioning, review quota and confirm the destination prefix:

```bash
export REGION="${ZONE%-*}"
gcloud compute regions describe "$REGION" \
  --project "$PROJECT" \
  --format='table(quotas.metric,quotas.limit,quotas.usage)'

export POOL="gnomad-full-${RUN_ID}"
export OUTPUT_PREFIX="${OUTPUT_BUCKET%/}/genohype/gnomad-v4.1.1/full/${RUN_ID}"
echo "$OUTPUT_PREFIX"
```

Create the profile:

```bash
export GENOHYPE_CONFIG="$(mktemp)"
cat > "$GENOHYPE_CONFIG" <<EOF
[defaults]
project = "$PROJECT"
zone = "$ZONE"

[pools.$POOL]
machine_type = "c3-highcpu-22"
starting_workers = 8
workers = 8
spot = true
with_coordinator = true
# Optionally set an explicitly authorized VM service account:
# service_account = "genohype-pool@YOUR_PROJECT.iam.gserviceaccount.com"
EOF
```

Check that the selected machine type is offered in the zone:

```bash
gcloud compute machine-types describe c3-highcpu-22 \
  --project "$PROJECT" --zone "$ZONE"
```

Start the VM/disk observer, then create the pool:

```bash
genohype --config "$GENOHYPE_CONFIG" pool create "$POOL" --wait
genohype --config "$GENOHYPE_CONFIG" pool workers "$POOL"
```

Connect the dashboard and event stream as shown above. Submit the full export without an interval:

```bash
time genohype --config "$GENOHYPE_CONFIG" pool submit "$POOL" \
  -- \
  export parquet \
  "$INPUT" \
  "$OUTPUT_PREFIX"
```

During the run, watch:

- completed versus total partitions in the dashboard;
- worker heartbeat, CPU, memory, and throughput;
- task failures and retries with `pool failures` and `pool events --follow`;
- spot preemption in the GCP instance view;
- object count and output size in GCS.

If workers are consistently saturated and quota permits, scale up without replacing the coordinator:

```bash
genohype --config "$GENOHYPE_CONFIG" pool scale "$POOL" --workers 12
```

If memory pressure is high, adding more high-CPU workers may not solve it; stop and select a machine type with more memory per active partition. Avoid scaling down during the first full export because removing active workers intentionally exercises task timeout and retry behavior.

After completion, inspect the final prefix:

```bash
gcloud storage ls "${OUTPUT_PREFIX}/**" | wc -l
gcloud storage du -s "$OUTPUT_PREFIX"
genohype --config "$GENOHYPE_CONFIG" pool status "$POOL"
genohype --config "$GENOHYPE_CONFIG" pool failures "$POOL"
```

### Clean up the full-export pool

Destroy compute resources as soon as the job completes or fails:

```bash
genohype --config "$GENOHYPE_CONFIG" pool destroy "$POOL"
```

Then verify and remove the per-pool firewall rule:

```bash
gcloud compute instances list --project "$PROJECT" --filter="name~'^${POOL}-'"
gcloud compute disks list --project "$PROJECT" --filter="name~'^${POOL}-'"

if gcloud compute firewall-rules describe "allow-hail-coord-int-${POOL}" \
  --project "$PROJECT" >/dev/null 2>&1; then
  gcloud compute firewall-rules delete "allow-hail-coord-int-${POOL}" \
    --project "$PROJECT" --quiet
fi

rm -f "$GENOHYPE_CONFIG"
```

Keep the GCS output for validation and downstream use, or remove the exact versioned prefix when it is no longer needed.

## Final validation checklist

For each export, record:

- Genohype version (`genohype --version`)
- source URI and exact interval, if any
- pool profile, worker count, and machine type
- output URI
- Parquet object count and total bytes
- coordinator summary and any task failures
- whether all instances, disks, tunnels, and per-pool firewall rules were cleaned up

Do not compare the number of Parquet objects with the number of variants: pool outputs are partition files, and interval exports may include empty files. Validate biological row counts with DuckDB, Polars, Spark, or another Parquet reader after the export.
