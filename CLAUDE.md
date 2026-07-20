# Genohype

Fast, memory-efficient toolkit for genomic data processing. Read Hail tables and VCF files, export to Parquet/ClickHouse/BigQuery, generate Manhattan plots, and run distributed jobs on GCP.

## Build

```bash
# Default build (GCS support)
cargo build

# Local files only (fastest compile, ~229 crates)
cargo build --no-default-features

# Build with all features (~408 crates)
cargo build --features full

# Build the HTTP server
cargo build --bin genohype-server --features server
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
| `benchmark` | `--benchmark` flag for parquet export (CPU/mem/IO metrics) | No |
| `full` | All features | No |

### Examples

```bash
# Add S3 support
cargo build --features aws

# Full cloud support (GCS + S3 + HTTP)
cargo build --features gcp,aws,http

# Everything
cargo build --features full
```

## Cross-Compilation (for GCP distributed processing)

To run distributed jobs on GCP VMs, you need a Linux binary. On macOS:

```bash
# Install dependencies (one-time)
brew install zig
cargo install cargo-zigbuild
rustup target add x86_64-unknown-linux-gnu

# Build Linux worker binary
make worker
# Output: target/release/genohype-worker
```

## Commands

- `info` - Show basic table metadata
- `inspect` - Show detailed table information
- `summary` - Show comprehensive table summary with statistics
- `query` - Query with optional filters
- `convert` - Convert to Parquet format
- `schema validate` - Validate table against JSON schema
- `schema generate` - Generate JSON schema from table
- `export clickhouse` - Export to ClickHouse (requires `clickhouse` feature)
- `export bigquery` - Export to BigQuery (requires `bigquery` feature)
- `export vcf` - Export to VCF format (from MatrixTable-derived tables)
- `export hail` - Export to Hail table format
- `pool create` - Create a distributed worker pool on GCP
- `pool submit` - Submit a job to the worker pool
- `pool destroy` - Destroy a worker pool
- `pool list` - List instances in a pool

## Distributed Processing (GCP)

Run parallel exports across multiple GCP VMs:

```bash
# 1. Build Linux worker binary
make worker

# 2. Create a pool of spot VMs
genohype pool create my-pool --workers 4 --spot

# 3. Submit a distributed job
genohype pool submit my-pool -- \
    export parquet gs://bucket/input.ht gs://bucket/output/ --shard-count 100

# 4. Clean up
genohype pool destroy my-pool
```

Requires `gcloud` CLI configured with appropriate project/credentials.

## ClickHouse

For local ClickHouse testing, see the Docker container running on `localhost:8123` (user: `default`, password: `test`).

## Test

```bash
cargo test --workspace --locked
cargo test --workspace --all-features --locked
```

Test VCFs and Hail tables can be found in `./data/`.
