# Publication-safe Parquet export (v1)

> [!CAUTION]
> **Safe export is pre-production. Do not run a production, bulk, or public-data pilot; create, scale, update, or destroy pools; or write or delete GCS objects without explicit operator approval for the exact project, bucket prefixes, pool, worker artifact, and cleanup plan.** Local planning and local/mock tests are the only generally authorized workflow. No production pilot has been approved.

Safe export is separate from the legacy `export parquet` path. It seals source and worker identities, dispatches one fenced task per Hail row partition, publishes immutable content-addressed Parquet and receipts, publishes `_SUCCESS` last, and validates by following exact identities without listing a prefix. Local/mock qualification covers protocol behavior; it does not qualify GCS generation, IAM, multipart/copy semantics, or a production deployment.

## Compatibility matrix

| Contract | Safe export v1 |
|---|---|
| Input and destination | GCS Hail table to GCS output only |
| Read mode | Sequential, complete partitions |
| Sharding | One Hail row partition per Parquet part |
| Writer | ZSTD, 100,000-row groups, 4,096-row converter batches; fixed v1 properties |
| Worker | Exact stock x86-64 Linux `genohype` binary, commit, build identity, SHA-256, and feature set |
| Ordering | Existing Hail key order; no remapping |
| Unsupported | Filters, projection, remapping, autoscale, force, legacy/custom workers, S3, HTTP, and local destinations |
| Historical codecs | Sequential LZ4HC decode has bounded qualification only; historical index decode is not supported evidence |

## 1. Manifest

Create a sequential, create-new manifest and retain both outputs:

```bash
genohype manifest inventory gs://SOURCE_BUCKET/path/table.ht \
  --access sequential \
  --output source-manifest.json \
  --digest-output source-manifest.json.sha256
```

Verify and archive the exact local bytes and digest receipt. Inventory is read-only: it neither uploads the manifest nor authorizes an export. GCS inventory itself reads cloud metadata and therefore requires the approval described above.

After approval, upload `source-manifest.json` **create-only** to the exact immutable manifest URI chosen for the spec. Never edit or re-upload different bytes at the same identity. Genohype does not provide an implicit upload step.

## 2. Spec

Seal into a new local file. The worker commit is the seven-hex value advertised by the stock worker; its build identity must be `<genohype-package-version>:<git7>`. The ordered list of `--worker-feature` values must exactly describe the deployed build (published full builds include every enabled feature, not only `gcp` and `validation`).

```bash
genohype safe-export seal \
  --table gs://SOURCE_BUCKET/path/table.ht \
  --manifest source-manifest.json \
  --manifest-uri gs://CONTROL_BUCKET/safe-export/manifests/MANIFEST_ID.json \
  --snapshot-timestamp 2026-08-13T12:00:00Z \
  --output-root gs://OUTPUT_BUCKET/releases/table \
  --worker-binary ./genohype-linux-x86_64 \
  --worker-binary-uri gs://CONTROL_BUCKET/safe-export/bin/WORKER_SHA/genohype \
  --worker-git-commit abc1234 \
  --worker-build-identity 0.1.0:abc1234 \
  --worker-feature gcp \
  --worker-feature validation \
  --output safe-export-spec.json
# Add every other enabled feature for the actual worker build.
```

`seal` digest-checks the local manifest, opens the canonical GCS table under that source snapshot, derives table/globals/Arrow/writer descriptors, binds row-source identities in partition order, verifies the stock Linux worker shape and advertised identity, hashes its exact bytes, and refuses overwrite. Thus sealing reads approved source objects even though its manifest, worker binary, and output spec are local files. Local spec storage does **not** make the export a local or authorized cloud operation.

For bounded local/mock qualification only, `--metadata-evidence-bundle LOCAL_JSON` may supply schema-v1 decoded table/globals evidence bound to the exact canonical `gs://` table URI and manifest digest. This seam performs no cloud reads, does not relax any sealed GCS URI or worker identity rule, and is not production authority. Production sealing must omit it and use the snapshot-conditioned GCS reads above.

Record the command, inventory timestamp, manifest digest, export ID, source/manifest/output URIs, worker git/build/binary SHA-256, feature set, and operator approval with the archived files.

## 3. Submit

Use detached JSON acknowledgement and retain its exact job ID:

```bash
genohype pool submit POOL \
  --detach --output json --batch-size 1 \
  -- export safe-parquet gs://SOURCE_BUCKET/path/table.ht \
     gs://OUTPUT_BUCKET/releases/table \
     --spec safe-export-spec.json
```

Do not use `--force`, `--autoscale`, `--auto-stop`, `--redeploy-binary`, or any worker-update option. The input, output, batch size, worker identity, and spec must match exactly.

## 4. Observe

Use the retained job ID, never the coordinator's current-job guess:

```bash
genohype pool safe-export-status POOL --job-id JOB_ID
genohype pool workers POOL
genohype pool events POOL
genohype pool failures POOL
```

The typed safe status reports exact job/export IDs, durable phase (`running`, `finalizing`, `completed`, `failed`, `cancelled`, or `superseded`), expected/imported/accepted/missing/current/failed counts, exact release and `_SUCCESS` identities, and a fail-closed error. `complete: true` means conditioned `_SUCCESS` readback and the SQLite completion transaction both succeeded.

Never infer safe-export success from idle state, generic `pool status` counters, custom-task receipts, object listing, `release.json`, or SQLite alone.

## 5. Resume

After the old job is durably terminal, or after coordinator replacement has fenced its session, resubmit the **identical spec** without force. A new job/session imports only receipts and outputs that validate at their exact identities, then queues only missing partitions.

- An output without its exact receipt is missing work.
- A receipt with malformed, conflicting, absent, or wrong-generation output evidence is fatal; it is never downgraded to missing.
- A stale completion from the old session has no publication authority.

## 6. Validate

Starting from the spec-derived `_SUCCESS` path, independently traverse the exact release, receipt, and output identities:

```bash
genohype safe-export validate \
  --spec safe-export-spec.json \
  --output safe-export-validation.json
```

The validator does not read coordinator SQLite and does not list any prefix. Its deterministic report checks canonical documents, complete coverage, source/spec/schema/build identities, receipt and aggregate digests, totals, and conditioned output size/SHA/generation/ETag evidence.

For local/mock or archived-evidence tests only, `--evidence-bundle LOCAL_JSON` supplies schema-v1 exact object paths, versions, ETags, and hex bytes to the same verifier. The bundle store rejects listing and mutation. A bundle is not cloud authority and does not approve production use.

An additional DuckDB or PyArrow read of explicitly downloaded Parquet objects can provide cross-implementation evidence, but download and cloud access require approval and are not part of validator success.

## 7. Cleanup

Local temporary files may be removed only after the spec, commands, status, and validation evidence are archived. During diagnosis or resume, never delete staging objects, canonical outputs, receipts, `release.json`, or `_SUCCESS`.

Remote deletion/GC and pool destruction require the same explicit approval plus an exact prefix/resource cleanup plan. There is no automatic staging GC in this stage.

## Failure decisions

| Observation | Decision |
|---|---|
| Source or spec identity mismatch | Stop. Preserve manifest, spec, command, and error; re-inventory/reseal only as a new reviewed export. |
| Stale completion | Preserve events/status. Do not force or manually publish; allow only the current fenced attempt. |
| Receipt absent and output absent/present | Treat the partition as missing; exact resubmit only after the old run is terminal. Never synthesize a receipt. |
| Receipt malformed or conflicting | Stop and preserve all evidence. Do not delete, overwrite, or downgrade it to missing. |
| Receipt references missing or wrong-generation output | Stop and preserve evidence. Do not resubmit until the conflict is understood. |
| `release.json` or `_SUCCESS` create-only conflict | Stop. Preserve both observed and expected identities; do not overwrite. |
| Coordinator interrupted | Confirm old session is durably failed/fenced, retain the exact spec, then exact-resubmit to import valid evidence. |
| Status is idle/generic-complete but safe status is not complete | Treat as incomplete. Use exact safe status and standalone validation only. |
