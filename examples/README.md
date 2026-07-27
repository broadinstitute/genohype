# Genohype Examples

Start with the project [installation instructions](../README.md#installation), then choose a focused example below.

## GCP pools

Practical, end-to-end operational walkthroughs:

- [Minimal GCP pool](gcp-pool/minimal.md) — create one coordinator and one worker, observe GCP resources, connect to the dashboard through IAP, and clean up.
- [Pool stress test](gcp-pool/stress-test.md) — exercise CPU, memory, scheduling, telemetry, and manual scaling on the minimal pool.
- [gnomAD v4.1.1 to Parquet](gcp-pool/gnomad-4.1.1-parquet.md) — export a local interval, chromosome 22 on a small pool, or the complete browser sites table on a larger pool.

For a disposable installation check, [`../scripts/test-clean-install-gcp.sh`](../scripts/test-clean-install-gcp.sh) provisions a fresh x86_64 Ubuntu VM, installs the published release, validates the small gnomAD interval with DuckDB, and deletes the VM.

## CLI

- [`cli/demo.sh`](cli/demo.sh) — a command-by-command CLI demonstration using the datasets under `data/`.

Run it from the repository root so its relative data paths resolve correctly:

```bash
bash examples/cli/demo.sh
```

## Rust

- [`rust/decode_gene_table.rs`](rust/decode_gene_table.rs) — low-level Hail gene-table decoding with `genohype-core`.

This is a standalone source example rather than a registered Cargo example target. It expects the repository's gene-model test data and can be adapted into a downstream crate.

## Interval files

- [`intervals/README.md`](intervals/README.md) — BED, JSON, and text interval-list formats.
- `intervals/test_genes.*` — sample interval files used by the commands in that guide.

## Schemas

- `schemas/genomics_validation.json` — example genomic validation schema.
- `schemas/prep_table.json` — example schema for the preparation table.
