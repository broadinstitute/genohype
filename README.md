# Genohype

A fast, memory-efficient toolkit for genomic data processing. Read Hail tables and VCF files, export to Parquet/ClickHouse/BigQuery, generate Manhattan plots, and run distributed jobs on GCP.

## Features

- **No Java runtime**: Prebuilt CLI binaries require no JVM or Hail installation
- **Multiple input formats**: Hail tables (.ht), VCF files (.vcf.bgz with tabix)
- **Cloud-native**: Read from local disk, GCS, S3, or HTTP URLs
- **Multiple outputs**: Export to Parquet, VCF, ClickHouse, BigQuery, or Hail format
- **Visualization**: Generate Manhattan plots and locus plots from GWAS results
- **Distributed processing**: Run parallel jobs across GCP VM pools
- **Memory efficient**: Stream datasets of any size with minimal memory

## Installation

Install the latest prebuilt release on Apple Silicon macOS, Intel macOS, or x86-64 Linux:

```bash
curl -fsSL https://raw.githubusercontent.com/broadinstitute/genohype/main/scripts/install.sh | sh
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

To build from source instead:

```bash
# Default build (GCS support)
./scripts/build-dashboard.sh
cargo build --release --locked

# Local files only (fastest compile)
cargo build --release --locked --no-default-features

# Build with all features
cargo build --release --locked --features full
```

## Quick Start

```bash
# View table metadata
genohype info path/to/table.ht

# Query with filters
genohype query path/to/table.ht --where ancestry=EUR --limit 10

# Export to Parquet
genohype export parquet path/to/table.ht output.parquet

# Query cloud tables directly
genohype info "gs://gcp-public-data--gnomad/release/4.1/ht/exomes/gnomad.exomes.v4.1.sites.ht"
```

## Commands

### Data Exploration

| Command | Description |
|---------|-------------|
| `info` | Show table metadata without scanning data (fast) |
| `summary` | Full scan to calculate row counts and field statistics |
| `query` | Stream rows with optional filtering |

### Export

| Command | Description |
|---------|-------------|
| `export parquet` | Convert to Parquet format |
| `export vcf` | Export to VCF format |
| `export hail` | Export to Hail table format (useful for subsetting) |
| `export clickhouse` | Export to ClickHouse database |
| `export bigquery` | Export to BigQuery |

### Visualization

| Command | Description |
|---------|-------------|
| `manhattan` | Generate Manhattan plots from GWAS results |
| `manhattan-batch` | Batch process multiple phenotypes |
| `loci` | Generate LocusZoom-style locus plots |

### Schema

| Command | Description |
|---------|-------------|
| `schema generate` | Generate JSON schema from table |
| `schema validate` | Validate table data against JSON schema |

### Distributed Processing

| Command | Description |
|---------|-------------|
| `pool create` | Create a distributed worker pool on GCP |
| `pool submit` | Submit a job to the worker pool |
| `pool destroy` | Destroy a worker pool |
| `pool list` | List instances in a pool |

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

### BigQuery

```bash
genohype export bigquery \
  data/variants.ht \
  project:dataset.table \
  --bucket staging-bucket \
  --intervals-file regions.bed
```

## VCF Support

genohype can read and query VCF files directly, with support for tabix indexing.

```bash
# View VCF metadata
genohype info data/variants.vcf.bgz

# Query with interval (uses tabix index if available)
genohype query data/variants.vcf.bgz --interval "chrX:31097677-31100000" --limit 10

# Generate schema from VCF
genohype schema generate data/variants.vcf.bgz

# Validate VCF with sampling
genohype schema validate data/variants.vcf.bgz schema.json --sample 10000
```

## Distributed Processing (GCP)

Run parallel exports across multiple GCP VMs. Prebuilt macOS installations include the Linux worker used by the pool commands; when building from source, run `make worker` first.

```bash
# 1. Create a pool of spot VMs
genohype pool create my-pool --workers 4 --spot

# 2. Submit a distributed job
genohype pool submit my-pool -- \
    export parquet gs://bucket/input.ht gs://bucket/output/ --shard-count 100

# 3. Clean up
genohype pool destroy my-pool
```

Requires `gcloud` CLI configured with appropriate project and credentials.

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

| Feature | Description | Default |
|---------|-------------|---------|
| `gcp` | Google Cloud Storage support | Yes |
| `validation` | `schema validate` and `schema generate` commands | Yes |
| `aws` | Amazon S3 support | No |
| `http` | HTTP/HTTPS URL support | No |
| `clickhouse` | `export clickhouse` command | No |
| `bigquery` | `export bigquery` command (requires gcp) | No |
| `server` | `genohype-server` HTTP binary | No |
| `full` | All features | No |

```bash
# Add S3 support
cargo build --release --features aws

# Full cloud support (GCS + S3 + HTTP)
cargo build --release --features gcp,aws,http

# Everything
cargo build --release --features full
```

## Testing

```bash
cargo test --workspace
cargo test --workspace --all-features

# Build the existing pool dashboard and run the complete local CI suite
./scripts/verify.sh
```

Release maintainers should follow [RELEASING.md](RELEASING.md).

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           GENOHYPE DATA FLOW                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  INPUT SOURCES                  CORE ENGINE                 OUTPUT TARGETS  │
│  ─────────────                  ───────────                 ──────────────  │
│                                                                             │
│  ┌───────────┐                                              ┌───────────┐  │
│  │Hail Table │──┐                                        ┌─►│  stdout   │  │
│  │  (.ht)    │  │                                        │  │  (JSON)   │  │
│  └───────────┘  │                                        │  └───────────┘  │
│                 │    ┌─────────────────────────────┐     │                  │
│  ┌───────────┐  │    │        QueryEngine          │     │  ┌───────────┐  │
│  │ VCF File  │──┼───►│  ┌───────────────────────┐  │─────┼─►│  Parquet  │  │
│  │(.vcf.bgz) │  │    │  │   DataSource Trait    │  │     │  │ (.parquet)│  │
│  └───────────┘  │    │  │  - row_type()         │  │     │  └───────────┘  │
│                 │    │  │  - query_iter()       │  │     │                  │
│  ┌───────────┐  │    │  │  - key_fields()       │  │     │  ┌───────────┐  │
│  │  Remote   │──┘    │  └───────────────────────┘  │     ├─►│ClickHouse │  │
│  │(gs://,s3://)      │                             │     │  │  (HTTP)   │  │
│  └───────────┘       │  ┌───────────────────────┐  │     │  └───────────┘  │
│                      │  │   Index (optional)    │  │     │                  │
│                      │  │  - Partition bounds   │  │     │  ┌───────────┐  │
│                      │  │  - Tabix (VCF)        │  │     └─►│ BigQuery  │  │
│                      │  └───────────────────────┘  │        │(GCS+Load) │  │
│                      └─────────────────────────────┘        └───────────┘  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Key Principles:**
- **DataSource Abstraction** - Unified interface for Hail tables and VCF files
- **Streaming by Default** - Memory-efficient processing of arbitrarily large datasets
- **Parquet as Intermediate** - Bridge between row-oriented sources and columnar targets
- **Consistent CLI** - Same `--where`/`--limit`/`--interval` options work across all commands

## License

Genohype is available under the [MIT License](LICENSE).
