//! Bounded scan summaries and canonical manifest publication for legacy Manhattan plots.

use genohype_association::{
    AssociationPointV1, AssociationThresholdV1, AssociationVisualizationManifestV1,
    AssociationVisualizationResourcesV1, GenomeLayoutV1, PlotKindV1, ResourceDescriptorV1, TopK,
    WarningCode, COORDINATE_CONVENTION_V1, DEFAULT_TOP_K, SCHEMA_VERSION_V1,
};
use genohype_core::codec::EncodedValue;
use genohype_core::{HailError, Result};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;

/// One partition's bounded, mergeable scan result.
pub(crate) struct LegacyPartitionSummary {
    pub partition_index: usize,
    pub row_count: u64,
    pub valid_row_count: u64,
    pub skipped_row_count: u64,
    pub rendered_point_count: u64,
    pub significant_hit_count: u64,
    top_hits: TopK,
    warnings: BTreeSet<String>,
    source_hasher: Sha256,
}

impl LegacyPartitionSummary {
    pub fn new(partition_index: usize) -> Result<Self> {
        let source_hasher = source_partition_hasher(partition_index);
        Ok(Self {
            partition_index,
            row_count: 0,
            valid_row_count: 0,
            skipped_row_count: 0,
            rendered_point_count: 0,
            significant_hit_count: 0,
            top_hits: TopK::new(DEFAULT_TOP_K).map_err(invalid_format)?,
            warnings: BTreeSet::new(),
            source_hasher,
        })
    }

    /// Fingerprint every decoded source row, including rows later skipped by validation.
    pub fn observe_source_row(&mut self, row: &EncodedValue) -> Result<()> {
        self.row_count = checked_add(self.row_count, 1, "source row count")?;
        hash_encoded_value(&mut self.source_hasher, row);
        Ok(())
    }

    pub fn observe_skipped_row(&mut self) -> Result<()> {
        self.skipped_row_count = checked_add(self.skipped_row_count, 1, "skipped row count")?;
        Ok(())
    }

    pub fn observe_valid_point(
        &mut self,
        point: AssociationPointV1,
        warnings: Vec<WarningCode>,
        rendered: bool,
        significant: bool,
    ) -> Result<()> {
        self.valid_row_count = checked_add(self.valid_row_count, 1, "valid row count")?;
        if rendered {
            self.rendered_point_count =
                checked_add(self.rendered_point_count, 1, "rendered point count")?;
        }
        if significant {
            self.significant_hit_count =
                checked_add(self.significant_hit_count, 1, "significant hit count")?;
        }
        for warning in warnings {
            self.warnings.insert(warning.as_str().to_string());
        }
        // The adapter already normalized the point. TopK revalidates it and remains O(K).
        self.top_hits.insert(point).map_err(invalid_format)?;
        Ok(())
    }
}

pub(crate) struct LegacyScanSummary {
    pub row_count: u64,
    pub valid_row_count: u64,
    pub skipped_row_count: u64,
    pub rendered_point_count: u64,
    pub significant_hit_count: u64,
    pub top_hits: Vec<AssociationPointV1>,
    pub warnings: Vec<String>,
    pub source_fingerprint: String,
}

/// Merge partition summaries in partition order so scheduling cannot affect output.
pub(crate) fn merge_partition_summaries(
    mut partitions: Vec<LegacyPartitionSummary>,
) -> Result<LegacyScanSummary> {
    partitions.sort_by_key(|summary| summary.partition_index);
    if partitions
        .windows(2)
        .any(|pair| pair[0].partition_index == pair[1].partition_index)
    {
        return Err(HailError::InvalidFormat(
            "duplicate legacy Manhattan partition summary".into(),
        ));
    }

    let mut row_count = 0;
    let mut valid_row_count = 0;
    let mut skipped_row_count = 0;
    let mut rendered_point_count = 0;
    let mut significant_hit_count = 0;
    let mut top_hits = TopK::new(DEFAULT_TOP_K).map_err(invalid_format)?;
    let mut warnings = BTreeSet::new();
    let mut source_hasher = Sha256::new();
    source_hasher.update(b"genohype.association.source.v1\0");
    source_hasher.update((partitions.len() as u64).to_be_bytes());

    for partition in partitions {
        row_count = checked_add(row_count, partition.row_count, "source row count")?;
        valid_row_count = checked_add(
            valid_row_count,
            partition.valid_row_count,
            "valid row count",
        )?;
        skipped_row_count = checked_add(
            skipped_row_count,
            partition.skipped_row_count,
            "skipped row count",
        )?;
        rendered_point_count = checked_add(
            rendered_point_count,
            partition.rendered_point_count,
            "rendered point count",
        )?;
        significant_hit_count = checked_add(
            significant_hit_count,
            partition.significant_hit_count,
            "significant hit count",
        )?;
        warnings.extend(partition.warnings);
        top_hits.merge(partition.top_hits).map_err(invalid_format)?;

        source_hasher.update((partition.partition_index as u64).to_be_bytes());
        source_hasher.update(partition.row_count.to_be_bytes());
        source_hasher.update(partition.source_hasher.finalize());
    }

    if valid_row_count.checked_add(skipped_row_count) != Some(row_count) {
        return Err(HailError::InvalidFormat(
            "legacy Manhattan scan counts are inconsistent".into(),
        ));
    }

    Ok(LegacyScanSummary {
        row_count,
        valid_row_count,
        skipped_row_count,
        rendered_point_count,
        significant_hit_count,
        top_hits: top_hits.into_sorted_vec(),
        warnings: warnings.into_iter().collect(),
        source_fingerprint: format!("sha256:{:x}", source_hasher.finalize()),
    })
}

pub(crate) fn source_ordinal(partition_index: usize, row_index: usize) -> Result<u64> {
    let partition = u32::try_from(partition_index).map_err(|_| {
        HailError::InvalidFormat("partition index exceeds source ordinal capacity".into())
    })?;
    let row = u32::try_from(row_index).map_err(|_| {
        HailError::InvalidFormat("partition row index exceeds source ordinal capacity".into())
    })?;
    Ok((u64::from(partition) << 32) | u64::from(row))
}

pub(crate) fn write_manifest(
    output_base: &str,
    layout: GenomeLayoutV1,
    threshold: f64,
    scan: LegacyScanSummary,
    png_bytes: &[u8],
) -> Result<String> {
    let raster_path = format!("{output_base}.png");
    let raster_url = Path::new(&raster_path)
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| HailError::InvalidFormat("output raster has no UTF-8 basename".into()))?
        .to_string();
    let plot_id = Path::new(output_base)
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .ok_or_else(|| HailError::InvalidFormat("output prefix has no UTF-8 basename".into()))?
        .to_string();
    let layout_hash = layout.layout_hash("GRCh38");
    let raster_checksum = format!("sha256:{:x}", Sha256::digest(png_bytes));

    let manifest = AssociationVisualizationManifestV1 {
        schema_version: SCHEMA_VERSION_V1.into(),
        plot_id: plot_id.clone(),
        kind: PlotKindV1::Manhattan,
        title: format!("Manhattan plot for {plot_id}"),
        assembly: "GRCh38".into(),
        coordinate_convention: COORDINATE_CONVENTION_V1.into(),
        source_fingerprint: scan.source_fingerprint,
        genohype_version: env!("CARGO_PKG_VERSION").into(),
        created_at: canonical_utc_now(),
        layout,
        layout_hash,
        thresholds: vec![AssociationThresholdV1 {
            name: "genome_wide_significance".into(),
            pvalue: threshold,
        }],
        row_count: scan.row_count,
        valid_row_count: scan.valid_row_count,
        skipped_row_count: scan.skipped_row_count,
        rendered_point_count: scan.rendered_point_count,
        significant_hit_count: scan.significant_hit_count,
        top_hits: scan.top_hits,
        warnings: scan.warnings,
        resources: AssociationVisualizationResourcesV1 {
            raster: ResourceDescriptorV1 {
                url: raster_url,
                media_type: "image/png".into(),
                byte_size: Some(png_bytes.len() as u64),
                checksum: Some(raster_checksum),
                expires_at: None,
            },
            region_api: None,
            table_api: None,
        },
    };
    manifest.validate().map_err(invalid_format)?;

    let mut bytes = serde_json::to_vec_pretty(&manifest).map_err(invalid_format)?;
    bytes.push(b'\n');
    let manifest_path = format!("{output_base}.manifest.json");
    atomic_write(Path::new(&manifest_path), &bytes)?;
    Ok(manifest_path)
}

fn checked_add(left: u64, right: u64, name: &str) -> Result<u64> {
    left.checked_add(right)
        .ok_or_else(|| HailError::InvalidFormat(format!("{name} overflowed")))
}

fn invalid_format(error: impl std::fmt::Display) -> HailError {
    HailError::InvalidFormat(error.to_string())
}

pub(crate) fn source_partition_hasher(partition_index: usize) -> Sha256 {
    let mut source_hasher = Sha256::new();
    source_hasher.update(b"genohype.association.source-partition.v1\0");
    source_hasher.update((partition_index as u64).to_be_bytes());
    source_hasher
}

pub(crate) fn hash_encoded_value(hasher: &mut Sha256, value: &EncodedValue) {
    match value {
        EncodedValue::Null => hasher.update([0]),
        EncodedValue::Binary(bytes) => {
            hasher.update([1]);
            hash_bytes(hasher, bytes);
        }
        EncodedValue::Struct(fields) => {
            hasher.update([2]);
            hasher.update((fields.len() as u64).to_be_bytes());
            for (name, value) in fields {
                hash_bytes(hasher, name.as_bytes());
                hash_encoded_value(hasher, value);
            }
        }
        EncodedValue::Int32(value) => {
            hasher.update([3]);
            hasher.update(value.to_be_bytes());
        }
        EncodedValue::Int64(value) => {
            hasher.update([4]);
            hasher.update(value.to_be_bytes());
        }
        EncodedValue::Float32(value) => {
            hasher.update([5]);
            hasher.update(value.to_bits().to_be_bytes());
        }
        EncodedValue::Float64(value) => {
            hasher.update([6]);
            hasher.update(value.to_bits().to_be_bytes());
        }
        EncodedValue::Boolean(value) => {
            hasher.update([7, u8::from(*value)]);
        }
        EncodedValue::Array(values) => {
            hasher.update([8]);
            hasher.update((values.len() as u64).to_be_bytes());
            for value in values {
                hash_encoded_value(hasher, value);
            }
        }
    }
}

fn hash_bytes(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update((bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
}

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| HailError::InvalidFormat("manifest path has no UTF-8 basename".into()))?;
    let temporary = parent.join(format!(".{name}.{}.tmp", uuid::Uuid::new_v4()));

    let result = (|| -> Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn canonical_utc_now() -> String {
    // Civil-date conversion from Unix days; avoids locale/process dependencies.
    let seconds = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    let days = seconds.div_euclid(86_400);
    let day_seconds = seconds.rem_euclid(86_400);
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let mut year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    let hour = day_seconds / 3_600;
    let minute = day_seconds % 3_600 / 60;
    let second = day_seconds % 60;
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
}
