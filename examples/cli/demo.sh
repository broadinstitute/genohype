#!/bin/bash
# genohype Demo Script
# Run commands one at a time in a tmux split

# Build release binary
cargo build --release --bin genohype

# ============================================
# INFO - Show table metadata (fast, no scan)
# ============================================

# View metadata for analysis table
genohype info data/analysis-meta.ht

# View metadata for variant table with locus field
genohype info data/variants.ht

# ============================================
# SUMMARY - Full scan with statistics (slower)
# ============================================

# Get row count and field statistics
genohype summary data/analysis-meta.ht

# ============================================
# QUERY - Stream rows with filtering
# ============================================

# Basic query with limit
genohype query data/analysis-meta.ht --limit 3

# Filter by field value
genohype query data/analysis-meta.ht --where ancestry=EUR --limit 3

# Multiple filters combined
genohype query data/analysis-meta.ht --where ancestry=EUR --where trait_type=binary --limit 3

# Query with JSON output
genohype query data/analysis-meta.ht --where ancestry=AFR --limit 2 --json

# ============================================
# GCS - Query from Google Cloud Storage
# ============================================

# Query public gnomAD table (requires GCS auth)
genohype info "gs://gcp-public-data--gnomad/release/4.1/ht/exomes/gnomad.exomes.v4.1.sites.ht"

# Query with nested field filters
genohype query "gs://gcp-public-data--gnomad/release/4.1/ht/exomes/gnomad.exomes.v4.1.sites.ht" \
  --where "locus.contig=chr1" \
  --where "locus.position>=55039447" \
  --where "locus.position<=55064852" \
  --limit 3

# ============================================
# QUERY - Genomic interval filtering
# ============================================

# Single interval
genohype query data/variants.ht --interval "chr10:121500000-121600000" --limit 5

# Filter by ancestry within interval
genohype query data/variants.ht --interval "chr10:121500000-121600000" --where ancestry_group=eur --limit 3

# Multiple intervals
genohype query data/variants.ht \
  --interval "chr10:121500000-121600000" \
  --interval "chr20:35400000-35500000" \
  --limit 5

# ============================================
# EXPORT - Convert to other formats
# ============================================

# Export to Parquet with filters
genohype export parquet \
  data/analysis-meta.ht \
  /tmp/analysis-eur.parquet \
  --where ancestry=EUR

# Query the parquet file with DuckDB
duckdb -c "SELECT phenoname, ancestry, n_cases, n_controls FROM '/tmp/analysis-eur.parquet' LIMIT 5"

# Export subset to new Hail table
genohype export hail \
  data/variants.ht \
  /tmp/variants-chr10.ht \
  --interval "chr10:121500000-121600000"

# Verify exported table with Hail
uv run --with hail python -c "import hail as hl; hl.init(quiet=True); ht = hl.read_table('/tmp/variants-chr10.ht'); ht.describe()"

# Export filtered VCF with bgzip compression
genohype export vcf \
  "./data/gnomad.exomes.v4.1.sites.chrX.vcf.bgz" \
  /tmp/filtered.vcf.gz \
  --interval "chrX:31097677-31098000" \
  --bgzip

# Inspect the exported VCF
gzcat /tmp/filtered.vcf.gz | head -30

# ============================================
# SCHEMA - Generate and validate schemas
# ============================================

# Generate JSON schema from table
genohype schema generate data/analysis-meta.ht

# Save schema to file
genohype schema generate data/analysis-meta.ht /tmp/analysis-schema.json

# Validate table against schema
genohype schema validate data/analysis-meta.ht /tmp/analysis-schema.json

# Validation failure (wrong schema for table)
genohype schema validate data/variants.ht /tmp/analysis-schema.json --limit 3

# Validate cloud table with random sampling
genohype schema generate "gs://axaou-central/ms/axaou/v8/515c3dc3/genome_variant_annotations_hds/ht/prep_table.ht" /tmp/cloud-schema.json

genohype schema validate "gs://axaou-central/ms/axaou/v8/515c3dc3/genome_variant_annotations_hds/ht/prep_table.ht" /tmp/cloud-schema.json --sample 100

# VCF info (works on VCF files too)
genohype info "./data/gnomad.exomes.v4.1.sites.chrX.vcf.bgz"

# Validate VCF
genohype schema generate "./data/gnomad.exomes.v4.1.sites.chrX.vcf.bgz" /tmp/vcf-schema.json

genohype schema validate "./data/gnomad.exomes.v4.1.sites.chrX.vcf.bgz" /tmp/vcf-schema.json --sample 10000

# Query VCF with interval using tabix index (~2kb region in DMD gene)
genohype query "./data/gnomad.exomes.v4.1.sites.chrX.vcf.bgz" --interval "chrX:31097677-31100000" 

# ============================================
# CLICKHOUSE - Export to ClickHouse database
# ============================================

# Supply credentials through your environment; do not commit them to this script.
: "${CLICKHOUSE_URL:?Set CLICKHOUSE_URL to your ClickHouse HTTP URL}"

# Export variants using intervals file (requires --features clickhouse)
genohype export clickhouse \
  data/variants.ht \
  "$CLICKHOUSE_URL" \
  demo_variants \
  --intervals-file data/demo_intervals.txt

# Query the exported data
curl -s "$CLICKHOUSE_URL" --data "SELECT locus.1 as contig, locus.2 as position, ref, alt, ancestry_group, allele_frequency FROM demo_variants LIMIT 10 FORMAT Pretty"

# Aggregate by ancestry
curl -s "$CLICKHOUSE_URL" --data "SELECT ancestry_group, count() as variants, round(avg(allele_frequency), 6) as avg_af FROM demo_variants GROUP BY ancestry_group FORMAT Pretty"

# ============================================
# BIGQUERY - Export to BigQuery (requires --features bigquery)
# ============================================

# Export with interval filter to BigQuery
genohype export bigquery \
  data/variants.ht \
  aou-neale-gwas-browser:hail_test.demo_variants \
  --bucket axaou-central-tmp \
  --intervals-file data/demo_intervals.txt

# Query the exported data in BigQuery
bq query --use_legacy_sql=false "SELECT locus.contig, locus.position, ref, alt, ancestry_group, allele_frequency FROM hail_test.demo_variants LIMIT 10"

# ============================================
# Cleanup
# ============================================

rm -f /tmp/analysis-eur.parquet /tmp/filtered.vcf.gz
rm -rf /tmp/variants-chr10.ht /tmp/analysis-schema.json
