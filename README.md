<br>

<p align="left">
  <img src="docs/branding/genohype-wordmark.png" alt="GENOHYPE" width="360"><br>
  <a href="https://github.com/broadinstitute/genohype/releases/latest"><img src="https://img.shields.io/github/v/release/broadinstitute/genohype?label=release" alt="Latest release"></a>
  <a href="https://github.com/broadinstitute/genohype/actions/workflows/ci.yml"><img src="https://github.com/broadinstitute/genohype/actions/workflows/ci.yml/badge.svg?branch=main" alt="CI status"></a>
  <a href="LICENSE"><img src="https://img.shields.io/github/license/broadinstitute/genohype" alt="License: MIT"></a>
</p>

<br>

Genohype is Rust toolkit for streaming genomic data from Hail tables and VCF files into interoperable files, databases, and applications. The released `genohype` CLI supports inspection, filtering, validation, export, visualization, and distributed GCP processing without requiring Java or a Hail installation. Reusable Rust crates expose the same data-access engine, worker-pool primitives, and MCP interfaces to downstream applications.

> [!IMPORTANT]
> Genohype is pre-1.0 research software. CLI and library interfaces may change, and the [VEP/LOFTEE integration remains experimental](ROADMAP.md#experimental-annotation-status). Full builds support S3 and HTTP data access, but distributed execution currently targets GCP; AWS and HPC execution adapters are not yet available.

## Capabilities

- **Unified data access**: Read Hail tables, VCF files, and BGZF-compressed BED-like files through a shared `DataSource` interface
- **Local and cloud I/O**: Stream from local disk, GCS, S3, or HTTP(S), depending on enabled features
- **Indexed querying**: Use Hail partition metadata and tabix indexes for genomic interval queries when available
- **Interoperable outputs**: Write Parquet, NDJSON, VCF, or Hail tables, and load ClickHouse, PostgreSQL, Elasticsearch, or BigQuery
- **Validation and annotation**: Generate and validate JSON schemas; optionally run the experimental in-process VEP integration
- **Visualization**: Generate Manhattan and locus plots from GWAS results
- **Distributed processing**: Run parallel jobs across GCP VM pools with coordinator, worker, and dashboard support
- **Bounded-memory streaming**: Decode and process rows incrementally rather than loading a complete dataset into memory

## Workspace Components

| Component | Purpose | Distribution status |
|-----------|---------|---------------------|
| `genohype` | Installable CLI for querying, export, visualization, and GCP operations | Published as checksummed macOS and x86-64 Linux binaries |
| `genohype-core` | Data access, codecs, querying, validation, export, and experimental annotation | Reusable Rust crate; pre-1.0 API |
| `genohype-pool` | Generic coordinator/worker and task-execution primitives | Reusable Rust crate; pre-1.0 API |
| `genohype-mcp` | Provider trait, domain types, and generic variant, gene, and region tools | Reusable Rust crate; no standalone MCP binary |
| `ui/` | React assistant components and a CopilotKit-to-MCP bridge | Experimental source packages; not part of the binary release or primary CI |

## Installation

Install the latest prebuilt release on Apple Silicon macOS, Intel macOS, or x86-64 Linux:

```bash
curl -fsSL https://raw.githubusercontent.com/broadinstitute/genohype/main/scripts/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"
genohype --version
```

The installer verifies SHA-256 checksums and installs to `${GENOHYPE_INSTALL_DIR:-$HOME/.local/bin}`. On macOS it also installs the Linux worker binary used by `genohype pool`. To select a specific release:

```bash
curl -fsSL https://raw.githubusercontent.com/broadinstitute/genohype/main/scripts/install.sh \
  | GENOHYPE_VERSION=v0.1.0 sh
```

To inspect the installer before running it:

```bash
curl -fsSLO https://raw.githubusercontent.com/broadinstitute/genohype/main/scripts/install.sh
less install.sh
sh install.sh
```

Published release binaries are built with the `full` CLI feature set. The optional `genohype-server` binary and the JavaScript packages under `ui/` are not distributed as separate release artifacts.

To build the CLI from source instead:

```bash
# Default source build
./scripts/build-dashboard.sh
cargo build --release --locked --package genohype-cli --bin genohype

# Match the feature set used for published CLI releases
cargo build --release --locked --package genohype-cli --bin genohype --features full

# Build the optional HTTP server from source
cargo build --release --locked --package genohype-cli --bin genohype-server --features server
```

## Quick Start

```bash
# View table metadata
genohype info path/to/table.ht

# Query with filters
genohype query path/to/table.ht --where ancestry=EUR --limit 10

# Export to Parquet
genohype export parquet path/to/table.ht output.parquet

# The current GCS client uses Google Application Default Credentials,
# including when reading public buckets.
gcloud auth application-default login
genohype info "gs://gcp-public-data--gnomad/release/4.1/ht/exomes/gnomad.exomes.v4.1.sites.ht"
```

## Commands

### Data Access

| Command | Description |
|---------|-------------|
| `info` | Show metadata, keys, partition layout, schema, and optional globals |
| `summary` | Scan a dataset to calculate row counts and field statistics |
| `query` | Stream rows with optional field and interval filters |
| `vcf index` | Create a tabix index for a BGZF-compressed VCF |
| `cache clear` | Clear locally cached Hail metadata |

### Export

| Command | Description |
|---------|-------------|
| `export parquet` | Export to Parquet |
| `export json` | Export newline-delimited JSON |
| `export vcf` | Export to VCF |
| `export hail` | Export a subset as a Hail table |
| `export clickhouse` | Load a ClickHouse table |
| `export postgres` | Load a PostgreSQL table |
| `export elasticsearch` | Load an Elasticsearch index |
| `export bigquery` | Load a BigQuery table through GCS staging |
| `export cache-build` | Materialize per-gene response-cache objects for browser workloads |

Database commands are available in published full-featured binaries. Source builds must enable their corresponding features.

### Visualization

| Command | Description |
|---------|-------------|
| `manhattan` | Generate a Manhattan plot and JSON sidecar |
| `manhattan-batch` | Process multiple phenotypes |
| `locus` | Render one LocusZoom-style region plot |
| `loci` | Generate locus plots from existing Manhattan output |

### Data Quality and Annotation

| Command | Description |
|---------|-------------|
| `schema generate` | Generate JSON Schema from a dataset |
| `schema validate` | Validate rows against JSON Schema |
| `annotate` | Add experimental VEP consequence predictions |

### Distributed and Operational Commands

| Command | Description |
|---------|-------------|
| `pool create`, `pool scale`, `pool destroy`, `pool list` | Manage GCP worker pools |
| `pool submit`, `pool status`, `pool cancel` | Submit and control distributed jobs |
| `pool workers`, `pool events`, `pool failures`, `pool logs` | Inspect workers and job activity |
| `service` | Run coordinator or worker services directly |
| `clickhouse` | Manage ClickHouse instances on GCP |
| `env` | Manage `.genohype-env` configuration |
| `ingest` | Run feature-gated external-system ingestion workflows |

Run `genohype --help` or `genohype <command> --help` for the complete, version-specific interface.

## Querying

```bash
# Basic query with limit
genohype query data/table.ht --limit 10

# Filter by field value
genohype query data/table.ht --where ancestry=EUR --limit 10

# Multiple filters
genohype query data/table.ht --where ancestry=EUR --where trait_type=binary --limit 10

# Nested field filters
genohype query data/table.ht --where "locus.contig=chr1" --where "locus.position>=55039447"

# Genomic interval filtering
genohype query data/table.ht --interval "chr10:121500000-121600000" --limit 10

# Multiple intervals
genohype query data/table.ht \
  --interval "chr10:121500000-121600000" \
  --interval "chr20:35400000-35500000" \
  --limit 10

# Intervals from file (BED, JSON, or text format)
genohype query data/table.ht --intervals-file regions.bed --limit 10

# JSON output
genohype query data/table.ht --limit 5 --json
```

## Export Examples

### Parquet

```bash
# Basic export
genohype export parquet data/table.ht output.parquet

# With filters
genohype export parquet data/table.ht output.parquet --where ancestry=EUR

# With interval filter
genohype export parquet data/table.ht output.parquet --interval "chr10:121500000-121600000"

# Query with DuckDB
duckdb -c "SELECT * FROM 'output.parquet' LIMIT 5"
```

### NDJSON

```bash
genohype export json data/table.ht output.ndjson --interval "chr10:121500000-121600000"
```

### VCF

```bash
# Export with bgzip compression
genohype export vcf data/variants.vcf.bgz output.vcf.gz --interval "chrX:31097677-31098000" --bgzip
```

### ClickHouse

```bash
genohype export clickhouse \
  data/variants.ht \
  "http://user:pass@localhost:8123" \
  target_table \
  --intervals-file regions.bed
```

### PostgreSQL

```bash
genohype export postgres \
  data/variants.ht \
  "postgres://user:pass@localhost:5432/gnomad" \
  variants \
  --recreate
```

### Elasticsearch

```bash
genohype export elasticsearch \
  data/variants.ht \
  "http://localhost:9200" \
  variants \
  --recreate
```

### BigQuery

```bash
genohype export bigquery \
  data/variants.ht \
  project:dataset.table \
  --bucket staging-bucket \
  --intervals-file regions.bed
```

## Input Formats

### Hail tables

Hail tables (`.ht`) are the primary input format. Genohype reads their metadata, partitioned row data, and indexes directly from local or supported object storage without starting Hail, Spark, or a JVM.

### VCF

Genohype reads `.vcf`, `.vcf.gz`, and `.vcf.bgz` files directly. Interval queries use a tabix index when one is available.

```bash
# View VCF metadata
genohype info data/variants.vcf.bgz

# Query with interval
genohype query data/variants.vcf.bgz --interval "chrX:31097677-31100000" --limit 10

# Generate and apply JSON Schema
genohype schema generate data/variants.vcf.bgz schema.json
genohype schema validate data/variants.vcf.bgz schema.json --sample 10000
```

### BGZF-compressed BED-like data

The shared query engine also accepts `.bed.gz` and `.bed.bgz` files. It infers column names and scalar types, and uses a tabix index for interval queries when available. This path is currently used for BED-like long-read and methylation inputs.

```bash
genohype info data/methylation.bed.gz
genohype query data/methylation.bed.gz --interval "chr1:1000000-1100000" --limit 10
```

Parquet is currently an output format rather than a `DataSource` input. Query exported Parquet with tools such as DuckDB, Polars, or Spark.

## Experimental Variant Annotation

Full-featured builds include an in-process fastVEP integration. It is experimental and requires separately obtained transcript annotations and, optionally, a reference FASTA and supplementary annotations.

```bash
genohype annotate data/variants.vcf.bgz \
  --gff3 path/to/transcripts.gff3.gz \
  --fasta path/to/reference.fa.gz \
  --output annotated.vcf
```

See the [roadmap's annotation status](ROADMAP.md#experimental-annotation-status) for the pinned integration revision and current limitations.

## Distributed Processing (GCP)

Run parallel jobs across GCP VMs. Prebuilt macOS installations include the Linux worker used by pool commands; when building from source, run `make worker` first.

```bash
# 1. Create a coordinator and four spot workers
genohype pool create my-pool \
  --workers 4 \
  --spot true \
  --with-coordinator \
  --wait

# 2. Submit a distributed export
genohype pool submit my-pool -- \
  export parquet gs://bucket/input.ht gs://bucket/output/ --shard-count 100

# 3. Inspect progress and worker activity
genohype pool status my-pool
genohype pool workers my-pool
genohype pool events my-pool

# 4. Clean up all pool VMs
genohype pool destroy my-pool
```

A coordinator-backed pool serves an embedded operations dashboard on port 3000. Pool commands require the `gcloud` CLI, an active project, credentials, and appropriate Compute Engine and storage permissions.

GCS, S3, and HTTP are storage-access features. The implemented distributed execution adapter currently provisions GCP only.

## Interval File Formats

The `--intervals-file` option supports multiple formats:

**BED format** (0-based, half-open):
```
chr1    55039446    55064852    PCSK9
chr2    178525988   178830802   TTN
```

**Text format** (1-based, inclusive):
```
chr1:55039447-55064852
chr2:178525989-178830802
```

**JSON format**:
```json
[
  {"contig": "chr1", "start": 55039447, "end": 55064852},
  {"contig": "chr2", "start": 178525989, "end": 178830802}
]
```

## Feature Flags

Published CLI binaries are built with `full`. The table below describes features on the `genohype-cli` source package.

| Feature | Description | Default source build |
|---------|-------------|----------------------|
| `gcp` | Google Cloud Storage access and GCP pool support | Yes |
| `aws` | Amazon S3 object-storage access | No |
| `http` | HTTP/HTTPS object access | No |
| `validation` | `schema validate` and `schema generate` commands | No |
| `genomic` | Forward the high-level genomic client API from `genohype-core` | No |
| `vep` | Experimental in-process VEP annotation | No |
| `clickhouse` | ClickHouse export and ingestion | No |
| `elasticsearch` | Elasticsearch export | No |
| `postgres` | PostgreSQL export | No |
| `bigquery` | BigQuery export; also enables GCP | No |
| `benchmark` | Compatibility feature; Parquet's `--benchmark` metrics flag is compiled normally | No |
| `server` | Build the optional `genohype-server` HTTP binary | No |
| `full` | Release-facing cloud, validation, database, server, benchmark, and VEP features | No |

```bash
# Add S3 access
cargo build --release --locked --package genohype-cli --bin genohype --features aws

# Enable all object-storage backends and schema commands
cargo build --release --locked --package genohype-cli --bin genohype \
  --features gcp,aws,http,validation

# Match the published CLI feature set
cargo build --release --locked --package genohype-cli --bin genohype --features full
```

## Testing

```bash
cargo test --workspace
cargo test --workspace --all-features

# Build the existing pool dashboard and run the complete local CI suite
./scripts/verify.sh
```

Release maintainers should follow [RELEASING.md](RELEASING.md).

## Using Genohype as Libraries

Downstream Rust applications can import individual workspace crates from a release tag:

```toml
[dependencies]
genohype-core = { git = "https://github.com/broadinstitute/genohype.git", tag = "v0.1.0", features = ["gcp", "validation"] }
genohype-pool = { git = "https://github.com/broadinstitute/genohype.git", tag = "v0.1.0" }
genohype-mcp = { git = "https://github.com/broadinstitute/genohype.git", tag = "v0.1.0" }
```

Pin a tag or immutable revision because these APIs remain pre-1.0. `genohype-mcp` is a library rather than a `genohype mcp` command: applications implement `GenomicDataProvider`, construct `GenomicToolServer`, and expose the resulting tools through their chosen transport. The packages under `ui/` provide an experimental React assistant and CopilotKit bridge but are not part of the published CLI release.

## Ecosystem

Genohype crates currently support applications with distinct genomic access patterns:

- [gnomAD Browser Lite](https://github.com/broadinstitute/gnomad-browser-lite), a pre-1.0 reference browser and federation-QC application, imports `genohype-core`, `genohype-pool`, and `genohype-mcp`.
- [All by All *All of Us* browser](https://github.com/broadinstitute/all-by-all-aou-browser) imports `genohype-core` for Hail-backed access and `genohype-mcp` for genomic tools.
- [gnomAD Long Read](https://github.com/broadinstitute/gnomad-lr), under active development, imports `genohype-core` and `genohype-pool` for long-read loading workflows.

Each downstream repository documents its own maturity, deployment, and supported interfaces.

## Project

- [Roadmap](ROADMAP.md)
- [Contributing guide](CONTRIBUTING.md)
- [Maintainers](MAINTAINERS.md)
- [Security policy](SECURITY.md)
- [Broad Institute Code of Conduct](https://github.com/broadinstitute/.github/blob/main/CODE_OF_CONDUCT.md)

## License

Genohype is available under the [MIT License](LICENSE).
