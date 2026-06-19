//! Phase-4 materialized gene-view cache builder (the `gcs-cache` arm).
//!
//! The browser hot set is *enumerable* (~20k protein-coding genes) and gnomAD
//! releases are infrequent, so the full gene-view API response for every gene is
//! precomputed offline and served as an O(1) lookup with no query engine. This
//! module is the offline producer: it iterates the gnomAD genes table, runs the
//! per-gene variant scan over the variants table, and writes one blob per gene to
//! `{output_prefix}/{gene_id}.json` (e.g. `gs://<bucket>/cache/variants/ENSG….json`).
//!
//! # Pinned blob contract
//!
//! Each blob is the **full** [`CacheGeneVariantsResponse`] (`{ gene, variants,
//! total }`), byte-compatible with `gnomad-browser-lite`'s
//! `models::api::GeneVariantsResponse`. This one shape serves three consumers
//! identically and MUST stay in lockstep with them:
//!
//! 1. the `gcs-cache` Rust backend `parse_blob`
//!    (`gnomad-browser-lite/backend/src/backend/gcs_cache.rs`) — `get_variants`
//!    returns `.variants`, `get_gene` returns `.gene`;
//! 2. the Phase-7 axis-3 browser-direct reader, which `fetch()`es the same blob
//!    and needs `.gene` to render the page;
//! 3. the differential oracle `oracle_gcs_cache_vs_hail`
//!    (`gnomad-browser-lite/backend/src/oracle.rs`), the authoritative real-data
//!    cross-check (deferred to the final execution pass).
//!
//! Storing a bare `Vec<Variant>` (the pre-Phase-4 shape) would make `get_gene`
//! fall back to cold Hail on every cached gene-view and break the browser
//! contract — see the contract note in `gcs_cache.rs`.
//!
//! # Field-mapping parity
//!
//! The Hail-row → API-shape mapping here mirrors `extract_variant` / `extract_gene`
//! in `gnomad-browser-lite/backend/src/backend/hail.rs`. Both sides share the same
//! decode primitives ([`crate::genomic::extract`]), so the only divergence risk is
//! this mapping — it is covered by the synthetic-fixture unit tests below, and the
//! real-data oracle is the final authority. **Known parity caveat:**
//! `gene.constraint` is left `None` here (the live `HailBackend::get_gene`
//! populates it from a separate constraint map); closing that gap is a final-pass
//! item recorded in `gnomad-bench/scripts/ingest.sh` (Execution runbook).

use crate::codec::EncodedValue;
use crate::error::Result;
use crate::io::{is_cloud_path, OutputWriter};
use crate::query::{IntervalList, QueryEngine};
use serde::Serialize;
use std::io::Write;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Decode helpers — inlined copies of `crate::genomic::extract` (which is behind
// the optional `genomic` feature) so the cache builder is always buildable. They
// match those definitions exactly, which is what keeps this mapping in lockstep
// with `hail.rs` (which uses the `genomic::extract` originals).
// ---------------------------------------------------------------------------

/// Field of an `EncodedValue::Struct` by exact name.
fn get_field<'a>(value: &'a EncodedValue, name: &str) -> Option<&'a EncodedValue> {
    if let EncodedValue::Struct(fields) = value {
        return fields.iter().find(|(k, _)| k == name).map(|(_, v)| v);
    }
    None
}

/// Nested field via dot notation (e.g. `"interval.start.contig"`).
fn get_nested_field<'a>(value: &'a EncodedValue, path: &str) -> Option<&'a EncodedValue> {
    let mut current = value;
    for part in path.split('.') {
        current = get_field(current, part)?;
    }
    Some(current)
}

/// String from a `Binary` value (the gnomAD HT representation of text).
fn as_string(value: &EncodedValue) -> Option<String> {
    match value {
        EncodedValue::Binary(b) => String::from_utf8(b.clone()).ok(),
        _ => None,
    }
}

/// i32 from an integer value (widening from `Int64` when it fits).
fn as_i32(value: &EncodedValue) -> Option<i32> {
    match value {
        EncodedValue::Int32(i) => Some(*i),
        EncodedValue::Int64(i) => (*i).try_into().ok(),
        _ => None,
    }
}

// ===========================================================================
// Blob contract structs — keep field names/types byte-compatible with
// gnomad-browser-lite/backend/src/models/api.rs.
// ===========================================================================

/// Mirror of `api::Exon`.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct CacheExon {
    pub feature_type: String,
    pub start: i64,
    pub stop: i64,
}

/// Mirror of `api::Transcript`.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct CacheTranscript {
    pub transcript_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop: Option<i64>,
    pub exons: Vec<CacheExon>,
}

/// Mirror of `api::Gene`. `constraint` is intentionally `Option<Value>` (always
/// `None` from this builder — see the parity caveat in the module docs) so the
/// serialized shape still matches and a future change can populate it.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct CacheGene {
    pub gene_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gene_symbol: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gencode_symbol: Option<String>,
    pub chrom: String,
    pub start: i64,
    pub stop: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub strand: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub canonical_transcript_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transcripts: Option<Vec<CacheTranscript>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exons: Option<Vec<CacheExon>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub constraint: Option<serde_json::Value>,
}

/// Mirror of `api::Variant`.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct CacheVariant {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub variant_id: Option<String>,
    pub pos: i64,
    pub chrom: String,
    pub alleles: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rsids: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consequence: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hgvsc: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hgvsp: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gene_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gene_symbol: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transcript_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lof: Option<String>,
    pub ac: i64,
    pub an: i64,
    pub af: f64,
    pub allele_freq: f64,
}

/// Mirror of `api::GeneVariantsResponse` — the unit written to `{gene_id}.json`.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct CacheGeneVariantsResponse {
    pub gene: CacheGene,
    pub variants: Vec<CacheVariant>,
    pub total: usize,
}

// ===========================================================================
// Hail-row → API-shape mapping (mirror of hail.rs extract_gene / extract_variant)
// ===========================================================================

fn extract_array<T>(
    row: &EncodedValue,
    field: &str,
    f: fn(&EncodedValue) -> Option<T>,
) -> Option<Vec<T>> {
    if let Some(EncodedValue::Array(arr)) = get_field(row, field) {
        let items: Vec<T> = arr.iter().filter_map(f).collect();
        if items.is_empty() {
            None
        } else {
            Some(items)
        }
    } else {
        None
    }
}

fn extract_exon(v: &EncodedValue) -> Option<CacheExon> {
    let feature_type = get_field(v, "feature_type")
        .and_then(as_string)
        .unwrap_or_else(|| "exon".to_string());
    let start = get_field(v, "start").and_then(as_i32)? as i64;
    let stop = get_field(v, "stop").and_then(as_i32)? as i64;
    Some(CacheExon {
        feature_type,
        start,
        stop,
    })
}

fn extract_transcript(v: &EncodedValue) -> Option<CacheTranscript> {
    let transcript_id = get_field(v, "transcript_id").and_then(as_string)?;
    let start = get_field(v, "start").and_then(as_i32).map(|i| i as i64);
    let stop = get_field(v, "stop").and_then(as_i32).map(|i| i as i64);
    let exons = extract_array(v, "exons", extract_exon).unwrap_or_default();
    Some(CacheTranscript {
        transcript_id,
        start,
        stop,
        exons,
    })
}

/// Map a decoded genes-HT row to the API [`CacheGene`] shape.
/// Mirror of `hail.rs::extract_gene`.
pub fn extract_gene(row: &EncodedValue) -> Option<CacheGene> {
    let gene_id = get_field(row, "gene_id").and_then(as_string)?;
    let gene_symbol = get_field(row, "symbol")
        .or_else(|| get_field(row, "gene_symbol"))
        .and_then(as_string);
    let gencode_symbol = get_field(row, "gencode_symbol").and_then(as_string);

    let chrom = get_field(row, "chrom")
        .and_then(as_string)
        .or_else(|| get_nested_field(row, "interval.start.contig").and_then(as_string))?;
    let start = get_field(row, "start")
        .and_then(as_i32)
        .or_else(|| get_nested_field(row, "interval.start.position").and_then(as_i32))
        .unwrap_or(0) as i64;
    let stop = get_field(row, "stop")
        .and_then(as_i32)
        .or_else(|| get_nested_field(row, "interval.end.position").and_then(as_i32))
        .unwrap_or(0) as i64;

    let strand = get_field(row, "strand").and_then(as_string);
    let canonical_transcript_id = get_field(row, "canonical_transcript_id")
        .and_then(as_string)
        .or_else(|| get_field(row, "mane_select_transcript_id").and_then(as_string));

    let transcripts = extract_array(row, "transcripts", extract_transcript);
    let exons = extract_array(row, "exons", extract_exon);

    Some(CacheGene {
        gene_id,
        gene_symbol,
        gencode_symbol,
        chrom,
        start,
        stop,
        strand,
        canonical_transcript_id,
        transcripts,
        exons,
        constraint: None,
    })
}

fn extract_alleles(row: &EncodedValue) -> Option<Vec<String>> {
    if let Some(EncodedValue::Array(arr)) = get_field(row, "alleles") {
        let alleles: Vec<String> = arr.iter().filter_map(|a| a.as_string()).collect();
        if alleles.is_empty() {
            None
        } else {
            Some(alleles)
        }
    } else {
        None
    }
}

fn extract_string_array(row: &EncodedValue, field: &str) -> Option<Vec<String>> {
    if let Some(EncodedValue::Array(arr)) = get_field(row, field) {
        let strings: Vec<String> = arr.iter().filter_map(|a| a.as_string()).collect();
        if strings.is_empty() {
            None
        } else {
            Some(strings)
        }
    } else {
        None
    }
}

/// Synthesize `contig-pos-ref-alt` (chr-prefix stripped) when `variant_id` is
/// absent. Mirror of `hail.rs::synthesize_variant_id`.
fn synthesize_variant_id(contig: &str, pos: i64, alleles: &[String]) -> Option<String> {
    if alleles.len() < 2 {
        return None;
    }
    let chrom = contig.strip_prefix("chr").unwrap_or(contig);
    Some(format!("{}-{}-{}", chrom, pos, alleles.join("-")))
}

/// Strip a transcript prefix from HGVS notation (`ENST….5:c.-180T>G` → `c.-180T>G`).
fn strip_hgvs_prefix(hgvs: Option<String>) -> Option<String> {
    hgvs.map(|s| match s.rfind(':') {
        Some(i) => s[i + 1..].to_string(),
        None => s,
    })
}

fn get_first_number(v: &EncodedValue) -> Option<f64> {
    match v {
        EncodedValue::Int32(i) => Some(*i as f64),
        EncodedValue::Int64(i) => Some(*i as f64),
        EncodedValue::Float32(f) => Some(*f as f64),
        EncodedValue::Float64(f) => Some(*f),
        EncodedValue::Array(arr) => arr.first().and_then(get_first_number),
        _ => None,
    }
}

fn get_ac_an_af(freq_val: &EncodedValue) -> Option<(i64, i64, f64)> {
    let to_i64 = |v: &EncodedValue| match v {
        EncodedValue::Int32(i) => Some(*i as i64),
        EncodedValue::Int64(i) => Some(*i),
        EncodedValue::Float64(f) => Some(*f as i64),
        _ => None,
    };
    let ac = get_field(freq_val, "ac")
        .or_else(|| get_field(freq_val, "AC"))
        .and_then(to_i64)?;
    let an = get_field(freq_val, "an")
        .or_else(|| get_field(freq_val, "AN"))
        .and_then(to_i64)?;
    let af = if an > 0 { ac as f64 / an as f64 } else { 0.0 };
    Some((ac, an, af))
}

/// Extract `(ac, an, af)`, preferring gnomAD-native exome/genome freq, then a flat
/// `freq` array, then VCF `info` AC/AN/AF. Mirror of `hail.rs::extract_freq`.
fn extract_freq(row: &EncodedValue) -> (i64, i64, f64) {
    for dataset in &["exome", "genome"] {
        if let Some(dataset_val) = get_field(row, dataset) {
            if let Some(freq) = get_field(dataset_val, "freq") {
                if let Some(all_freq) = get_field(freq, "all") {
                    if let Some(result) = get_ac_an_af(all_freq) {
                        return result;
                    }
                }
                if let Some(result) = get_ac_an_af(freq) {
                    return result;
                }
            }
        }
    }

    if let Some(EncodedValue::Array(freq_arr)) = get_field(row, "freq") {
        if let Some(first) = freq_arr.first() {
            if let Some(result) = get_ac_an_af(first) {
                return result;
            }
        }
    }

    if let Some(info) = get_field(row, "info") {
        let ac = get_field(info, "AC")
            .and_then(get_first_number)
            .map(|v| v as i64)
            .unwrap_or(0);
        let an = get_field(info, "AN")
            .and_then(get_first_number)
            .map(|v| v as i64)
            .unwrap_or(0);
        let af = get_field(info, "AF")
            .and_then(get_first_number)
            .unwrap_or_else(|| if an > 0 { ac as f64 / an as f64 } else { 0.0 });
        if ac > 0 || an > 0 {
            return (ac, an, af);
        }
    }

    (0, 0, 0.0)
}

type Consequence = (
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
);

/// Extract `(consequence, hgvsc, hgvsp, gene_id, gene_symbol, transcript_id, lof)`
/// from the canonical (or first) transcript consequence, falling back to a fastVEP
/// `vep` array. Mirror of `hail.rs::extract_canonical_consequence`.
fn extract_canonical_consequence(row: &EncodedValue) -> Consequence {
    let is_true = |v: &EncodedValue| matches!(v, EncodedValue::Boolean(true));

    if let Some(EncodedValue::Array(tcs)) = get_field(row, "transcript_consequences") {
        let tc = tcs
            .iter()
            .find(|tc| get_field(tc, "is_canonical").map(is_true).unwrap_or(false))
            .or_else(|| tcs.first());
        if let Some(tc) = tc {
            return (
                get_field(tc, "major_consequence").and_then(as_string),
                get_field(tc, "hgvsc").and_then(as_string),
                get_field(tc, "hgvsp").and_then(as_string),
                get_field(tc, "gene_id").and_then(as_string),
                get_field(tc, "gene_symbol").and_then(as_string),
                get_field(tc, "transcript_id").and_then(as_string),
                get_field(tc, "lof").and_then(as_string),
            );
        }
    }

    if let Some(EncodedValue::Array(veps)) = get_field(row, "vep") {
        let entry = veps
            .iter()
            .find(|v| get_field(v, "canonical").map(is_true).unwrap_or(false))
            .or_else(|| veps.first());
        if let Some(entry) = entry {
            return (
                get_field(entry, "consequence").and_then(as_string),
                strip_hgvs_prefix(get_field(entry, "hgvsc").and_then(as_string)),
                strip_hgvs_prefix(get_field(entry, "hgvsp").and_then(as_string)),
                get_field(entry, "gene_id").and_then(as_string),
                get_field(entry, "gene_symbol").and_then(as_string),
                get_field(entry, "transcript_id").and_then(as_string),
                get_field(entry, "lof").and_then(as_string),
            );
        }
    }

    (None, None, None, None, None, None, None)
}

/// Map a decoded variants-HT row to the API [`CacheVariant`] shape.
/// Mirror of `hail.rs::extract_variant`.
pub fn extract_variant(row: &EncodedValue) -> Option<CacheVariant> {
    let locus = get_field(row, "locus")?;
    let contig = as_string(get_field(locus, "contig")?)?;
    let pos = as_i32(get_field(locus, "position")?)? as i64;

    let alleles = extract_alleles(row)?;
    let variant_id = get_field(row, "variant_id")
        .and_then(as_string)
        .or_else(|| synthesize_variant_id(&contig, pos, &alleles));
    let rsids = extract_string_array(row, "rsids")
        .or_else(|| get_field(row, "rsid").and_then(as_string).map(|s| vec![s]));

    let (consequence, hgvsc, hgvsp, gene_id, gene_symbol, transcript_id, lof) =
        extract_canonical_consequence(row);
    let (ac, an, af) = extract_freq(row);

    Some(CacheVariant {
        variant_id,
        pos,
        chrom: contig,
        alleles,
        rsids,
        consequence,
        hgvsc,
        hgvsp,
        gene_id,
        gene_symbol,
        transcript_id,
        lof,
        ac,
        an,
        af,
        allele_freq: af,
    })
}

// ===========================================================================
// Per-gene blob construction + cache-build orchestration
// ===========================================================================

/// Interval strings for a gene's `[start, stop]` in both chr-prefixed and bare
/// contig forms, so the scan matches HT (`chr1`) and VCF (`1`) sources alike.
/// Mirror of `hail.rs::dual_contig_intervals`.
fn dual_contig_intervals(chrom: &str, start: i64, stop: i64) -> Vec<String> {
    let alt = match chrom.strip_prefix("chr") {
        Some(bare) => bare.to_string(),
        None => format!("chr{chrom}"),
    };
    vec![
        format!("{chrom}:{start}-{stop}"),
        format!("{alt}:{start}-{stop}"),
    ]
}

/// Build one gene's full [`CacheGeneVariantsResponse`] by scanning `variants` over
/// the gene's interval. The result is what gets serialized to `{gene_id}.json`.
pub fn build_gene_blob(
    gene: CacheGene,
    variants: &QueryEngine,
) -> Result<CacheGeneVariantsResponse> {
    let interval_strs = dual_contig_intervals(&gene.chrom, gene.start, gene.stop);
    let intervals = IntervalList::from_strings(&interval_strs)?;

    let variant_list: Vec<CacheVariant> = variants
        .query_iter_with_intervals(&[], Some(Arc::new(intervals)))?
        .filter_map(|res| res.ok().and_then(|row| extract_variant(&row)))
        .collect();

    Ok(CacheGeneVariantsResponse {
        total: variant_list.len(),
        gene,
        variants: variant_list,
    })
}

/// Write a single gene's blob to `{output_prefix}/{gene_id}.json` (local or
/// `gs://`), returning the number of bytes written.
fn write_blob(output_prefix: &str, response: &CacheGeneVariantsResponse) -> Result<usize> {
    let prefix = output_prefix.trim_end_matches('/');
    let path = format!("{prefix}/{}.json", response.gene.gene_id);
    let bytes = serde_json::to_vec(response)?;
    let mut writer = OutputWriter::new(&path)?;
    writer.write_all(&bytes)?;
    writer.finish()?;
    Ok(bytes.len())
}

/// Aggregate counters for one cache-build invocation. The acceptance check is
/// `blobs_written == genes_seen` (every gene materialized exactly one blob).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct CacheBuildStats {
    /// Genes the build attempted (matched the optional `gene_ids` filter).
    pub genes_seen: usize,
    /// Blobs successfully written (one per gene, including 0-variant genes).
    pub blobs_written: usize,
    /// Genes whose interval scan returned zero variants (still written).
    pub genes_no_variants: usize,
    /// Total variants serialized across all blobs.
    pub total_variants: usize,
    /// Total JSON bytes written (cache size — itself a Phase-4 result).
    pub total_bytes: usize,
}

/// Whether a gene's `[start, stop]` body overlaps any of `intervals`.
///
/// The genes Hail table is keyed by `gene_id`, so a Hail interval scan can't
/// slice it by locus — callers that want to scope the cache to a region (smoke/
/// subset) must filter post-decode. Genes carry an UNPREFIXED contig (`"21"`)
/// while intervals are usually chr-prefixed (`"chr21"`), so both are tested.
pub fn gene_overlaps(intervals: &IntervalList, gene: &CacheGene) -> bool {
    let start = gene.start as i32;
    let stop = gene.stop as i32;
    let chrom = gene.chrom.as_str();
    let bare = chrom.strip_prefix("chr").unwrap_or(chrom);
    let prefixed = format!("chr{bare}");
    intervals.overlaps(chrom, start, stop)
        || intervals.overlaps(bare, start, stop)
        || intervals.overlaps(&prefixed, start, stop)
}

/// Build the materialized gene-view cache.
///
/// Opens the genes and variants tables, then for each gene (optionally restricted
/// to `gene_ids` — the per-task chunk the pool worker receives, AND/OR scoped to
/// `intervals` — the active scale's region) builds and writes
/// `{output_prefix}/{gene_id}.json`. When `gene_ids` is `None` the whole genes
/// table is iterated (then filtered by `intervals` if given).
///
/// For a local `output_prefix` the directory is created if needed; for a `gs://`
/// prefix the blobs are uploaded directly.
pub fn build_cache(
    genes_path: &str,
    variants_path: &str,
    output_prefix: &str,
    gene_ids: Option<&[String]>,
    intervals: Option<&IntervalList>,
) -> Result<CacheBuildStats> {
    if !is_cloud_path(output_prefix) {
        std::fs::create_dir_all(output_prefix.trim_end_matches('/'))?;
    }

    let mut genes_engine = QueryEngine::open_path(genes_path)?;
    let variants_engine = QueryEngine::open_path(variants_path)?;

    let mut stats = CacheBuildStats::default();

    match gene_ids {
        // Targeted chunk: point-lookup each gene_id (the pool-task path).
        Some(ids) => {
            for gene_id in ids {
                let key = EncodedValue::Struct(vec![(
                    "gene_id".to_string(),
                    EncodedValue::Binary(gene_id.as_bytes().to_vec()),
                )]);
                let Some(row) = genes_engine.lookup(&key)? else {
                    continue;
                };
                let Some(gene) = extract_gene(&row) else {
                    continue;
                };
                if let Some(iv) = intervals {
                    if !gene_overlaps(iv, &gene) {
                        continue;
                    }
                }
                accumulate(&mut stats, output_prefix, gene, &variants_engine)?;
            }
        }
        // Full table scan (whole-cache build / local fixture validation).
        None => {
            let rows: Vec<EncodedValue> = genes_engine
                .query_iter(&[])?
                .filter_map(|r| r.ok())
                .collect();
            for row in &rows {
                let Some(gene) = extract_gene(row) else {
                    continue;
                };
                if let Some(iv) = intervals {
                    if !gene_overlaps(iv, &gene) {
                        continue;
                    }
                }
                accumulate(&mut stats, output_prefix, gene, &variants_engine)?;
            }
        }
    }

    Ok(stats)
}

/// Build one gene's blob, write it, and fold the result into `stats`.
fn accumulate(
    stats: &mut CacheBuildStats,
    output_prefix: &str,
    gene: CacheGene,
    variants_engine: &QueryEngine,
) -> Result<()> {
    stats.genes_seen += 1;
    let response = build_gene_blob(gene, variants_engine)?;
    if response.total == 0 {
        stats.genes_no_variants += 1;
    }
    stats.total_variants += response.total;
    let bytes = write_blob(output_prefix, &response)?;
    stats.total_bytes += bytes;
    stats.blobs_written += 1;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bin(s: &str) -> EncodedValue {
        EncodedValue::Binary(s.as_bytes().to_vec())
    }

    fn locus(contig: &str, pos: i32) -> EncodedValue {
        EncodedValue::Struct(vec![
            ("contig".to_string(), bin(contig)),
            ("position".to_string(), EncodedValue::Int32(pos)),
        ])
    }

    /// A gnomAD-native variant row: locus + alleles + variant_id + exome.freq.all
    /// + a canonical transcript consequence.
    fn variant_row() -> EncodedValue {
        let freq_all = EncodedValue::Struct(vec![
            ("ac".to_string(), EncodedValue::Int32(3)),
            ("an".to_string(), EncodedValue::Int32(1000)),
        ]);
        let freq = EncodedValue::Struct(vec![("all".to_string(), freq_all)]);
        let exome = EncodedValue::Struct(vec![("freq".to_string(), freq)]);
        let tc = EncodedValue::Struct(vec![
            ("is_canonical".to_string(), EncodedValue::Boolean(true)),
            ("major_consequence".to_string(), bin("missense_variant")),
            ("gene_id".to_string(), bin("ENSG1")),
            ("gene_symbol".to_string(), bin("FAKE")),
            ("transcript_id".to_string(), bin("ENST1")),
        ]);
        EncodedValue::Struct(vec![
            ("locus".to_string(), locus("chr1", 150)),
            (
                "alleles".to_string(),
                EncodedValue::Array(vec![bin("A"), bin("C")]),
            ),
            ("variant_id".to_string(), bin("1-150-A-C")),
            (
                "transcript_consequences".to_string(),
                EncodedValue::Array(vec![tc]),
            ),
            ("exome".to_string(), exome),
        ])
    }

    fn gene_row() -> EncodedValue {
        let exon = EncodedValue::Struct(vec![
            ("feature_type".to_string(), bin("CDS")),
            ("start".to_string(), EncodedValue::Int32(120)),
            ("stop".to_string(), EncodedValue::Int32(180)),
        ]);
        EncodedValue::Struct(vec![
            ("gene_id".to_string(), bin("ENSG1")),
            ("symbol".to_string(), bin("FAKE")),
            ("chrom".to_string(), bin("chr1")),
            ("start".to_string(), EncodedValue::Int32(100)),
            ("stop".to_string(), EncodedValue::Int32(200)),
            ("strand".to_string(), bin("+")),
            ("canonical_transcript_id".to_string(), bin("ENST1")),
            ("exons".to_string(), EncodedValue::Array(vec![exon])),
        ])
    }

    #[test]
    fn extract_variant_maps_native_fields() {
        let v = extract_variant(&variant_row()).unwrap();
        assert_eq!(v.variant_id.as_deref(), Some("1-150-A-C"));
        assert_eq!(v.chrom, "chr1");
        assert_eq!(v.pos, 150);
        assert_eq!(v.alleles, vec!["A".to_string(), "C".to_string()]);
        assert_eq!(v.consequence.as_deref(), Some("missense_variant"));
        assert_eq!(v.gene_symbol.as_deref(), Some("FAKE"));
        assert_eq!(v.ac, 3);
        assert_eq!(v.an, 1000);
        assert!((v.af - 0.003).abs() < 1e-9);
        assert_eq!(v.af, v.allele_freq);
    }

    #[test]
    fn extract_variant_synthesizes_missing_id() {
        // A row lacking variant_id: id is synthesized from locus + alleles.
        let row = EncodedValue::Struct(vec![
            ("locus".to_string(), locus("chr7", 42)),
            (
                "alleles".to_string(),
                EncodedValue::Array(vec![bin("G"), bin("T")]),
            ),
        ]);
        let v = extract_variant(&row).unwrap();
        assert_eq!(v.variant_id.as_deref(), Some("7-42-G-T"));
    }

    #[test]
    fn extract_gene_maps_fields_and_exons() {
        let g = extract_gene(&gene_row()).unwrap();
        assert_eq!(g.gene_id, "ENSG1");
        assert_eq!(g.gene_symbol.as_deref(), Some("FAKE"));
        assert_eq!(g.chrom, "chr1");
        assert_eq!(g.start, 100);
        assert_eq!(g.stop, 200);
        assert_eq!(g.strand.as_deref(), Some("+"));
        assert_eq!(g.canonical_transcript_id.as_deref(), Some("ENST1"));
        let exons = g.exons.unwrap();
        assert_eq!(exons.len(), 1);
        assert_eq!(exons[0].feature_type, "CDS");
        assert!(g.constraint.is_none());
    }

    /// Guards the pinned contract: a serialized blob must match the
    /// `GeneVariantsResponse` JSON shape — `gene`/`variants`/`total` at the top
    /// level, gene metadata carried in `.gene` (so the cache backend serves
    /// `get_gene` without a cold query), and skip-if-none fields omitted.
    #[test]
    fn blob_serializes_to_pinned_shape() {
        let gene = extract_gene(&gene_row()).unwrap();
        let variants = vec![extract_variant(&variant_row()).unwrap()];
        let response = CacheGeneVariantsResponse {
            total: variants.len(),
            gene,
            variants,
        };
        let json = serde_json::to_value(&response).unwrap();
        assert!(json.get("gene").is_some());
        assert!(json.get("variants").is_some());
        assert_eq!(json["total"], 1);
        assert_eq!(json["gene"]["gene_id"], "ENSG1");
        assert_eq!(json["gene"]["chrom"], "chr1");
        // constraint is None → omitted (matches api::Gene serialization).
        assert!(json["gene"].get("constraint").is_none());
        assert_eq!(json["variants"][0]["variant_id"], "1-150-A-C");
        assert_eq!(json["variants"][0]["af"], 0.003);
    }
}
