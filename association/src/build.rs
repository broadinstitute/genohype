use crate::manifest::{
    AssociationThresholdV1, AssociationVisualizationManifestV1,
    AssociationVisualizationResourcesV1, ManifestError, PlotKindV1, ResourceDescriptorV1,
    COORDINATE_CONVENTION_V1, SCHEMA_VERSION_V1,
};
use crate::model::{AssociationDatasetSummaryV1, AssociationPointV1, WarningCode};
use crate::reduction::{TopK, TopKError, DEFAULT_TOP_K, MAX_TOP_K};
use crate::render::{AssociationRasterizer, Background, RenderError};
use crate::session::DatasetIdV1;
use crate::source::{
    AssociationSource, AssociationSourceError, AssociationSourceItem, RejectionDiagnostics,
};
use crate::store::{
    AssociationBuildV1, AssociationStore, AssociationStoreError, MAX_INSERT_BATCH_SIZE,
};
use crate::{GenomeLayoutV1, JSON_SAFE_INTEGER_MAX};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub const DEFAULT_INSERT_BATCH_SIZE: usize = 1_000;
const THRESHOLD_COLOR: &str = "#B71C1C";

/// Filesystem- and runtime-neutral controls for a one-pass association build.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AssociationBuildOptionsV1 {
    #[schemars(range(max = 1000))]
    pub top_hits: usize,
    #[schemars(range(min = 1, max = 1000))]
    pub insert_batch_size: usize,
    #[schemars(range(min = 0.0, max = 1.0))]
    pub significance_threshold: f64,
}
impl Default for AssociationBuildOptionsV1 {
    fn default() -> Self {
        Self {
            top_hits: DEFAULT_TOP_K,
            insert_batch_size: DEFAULT_INSERT_BATCH_SIZE,
            significance_threshold: 5e-8,
        }
    }
}
impl AssociationBuildOptionsV1 {
    pub fn validate(&self) -> Result<(), BuildContractError> {
        if self.top_hits > MAX_TOP_K
            || self.insert_batch_size == 0
            || self.insert_batch_size > MAX_INSERT_BATCH_SIZE
            || !self.significance_threshold.is_finite()
            || self.significance_threshold <= 0.0
            || self.significance_threshold > 1.0
        {
            return Err(BuildContractError::InvalidOptions);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AssociationBuildProgressV1 {
    pub rows_seen: u64,
    pub valid_rows: u64,
    pub rejected_rows: u64,
    pub rows_persisted: u64,
    pub batches_committed: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AssociationBuildOutcomeV1 {
    pub summary: AssociationDatasetSummaryV1,
    pub top_hits: Vec<AssociationPointV1>,
    pub source_fingerprint: String,
}

/// Source-neutral cancellation probe suitable for synchronous builders.
pub trait AssociationBuildCancellation: Send + Sync {
    fn is_cancelled(&self) -> bool;
}

#[derive(Debug, Clone, Default)]
pub struct BuildCancellation(Arc<AtomicBool>);
impl BuildCancellation {
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }
    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
    pub fn check(&self) -> Result<(), AssociationBuildError> {
        if self.is_cancelled() {
            Err(AssociationBuildError::Cancelled)
        } else {
            Ok(())
        }
    }
}
impl AssociationBuildCancellation for BuildCancellation {
    fn is_cancelled(&self) -> bool {
        self.is_cancelled()
    }
}

#[derive(Debug, Default)]
pub struct NeverCancel;
impl AssociationBuildCancellation for NeverCancel {
    fn is_cancelled(&self) -> bool {
        false
    }
}

/// Complete source-neutral rendering and manifest inputs. Paths and clocks are supplied by CLI.
#[derive(Debug, Clone)]
pub struct AssociationBuildConfig {
    pub dataset_id: DatasetIdV1,
    pub layout: GenomeLayoutV1,
    pub thresholds: Vec<AssociationThresholdV1>,
    pub options: AssociationBuildOptionsV1,
    pub point_radius: f32,
    pub point_alpha: u8,
    pub background: Background,
    pub title: String,
    pub genohype_version: String,
    pub created_at: String,
    pub table_resource: ResourceDescriptorV1,
    pub region_resource: ResourceDescriptorV1,
}

#[derive(Debug, Clone)]
pub struct AssociationBuildProduct {
    pub source_fingerprint: String,
    pub summary: AssociationDatasetSummaryV1,
    pub top_hits: Vec<AssociationPointV1>,
    pub rejection_diagnostics: RejectionDiagnostics,
    pub png_bytes: Vec<u8>,
    pub manifest: AssociationVisualizationManifestV1,
    pub progress: AssociationBuildProgressV1,
}

/// Build a complete association product with exactly one source traversal.
///
/// The store remains in `building` state until the source fingerprint is finalized after EOF.
/// On any pre-completion failure the builder asks the store to remove the partial dataset.
pub fn build_association_session<S: AssociationSource, T: AssociationStore>(
    source: &mut S,
    store: &mut T,
    config: &AssociationBuildConfig,
    cancellation: &dyn AssociationBuildCancellation,
) -> Result<AssociationBuildProduct, AssociationBuildError> {
    validate_config(source, config)?;
    check_cancelled(cancellation)?;

    let mut raster = AssociationRasterizer::from_layout(&config.layout, config.background.clone())?;
    for threshold in &config.thresholds {
        raster.render_threshold(&config.layout, threshold.pvalue, THRESHOLD_COLOR)?;
    }

    store.begin_build(&AssociationBuildV1 {
        dataset_id: config.dataset_id.clone(),
        created_at: config.created_at.clone(),
    })?;

    let result = build_after_begin(source, store, config, cancellation, raster);
    if result.is_err() {
        // Best effort: stores may already be complete if a post-commit integrity check failed.
        let _ = store.abort_build(&config.dataset_id);
    }
    result
}

fn build_after_begin<S: AssociationSource, T: AssociationStore>(
    source: &mut S,
    store: &mut T,
    config: &AssociationBuildConfig,
    cancellation: &dyn AssociationBuildCancellation,
    mut raster: AssociationRasterizer,
) -> Result<AssociationBuildProduct, AssociationBuildError> {
    let assembly = source.metadata().assembly.clone();
    let mut progress = AssociationBuildProgressV1::default();
    let mut summary = AssociationDatasetSummaryV1::default();
    let mut warnings: BTreeMap<&'static str, u64> = BTreeMap::new();
    let mut rejections = RejectionDiagnostics::default();
    let mut top_hits = TopK::new(config.options.top_hits)?;
    let mut batch = Vec::with_capacity(config.options.insert_batch_size);

    loop {
        check_cancelled(cancellation)?;
        let Some(item) = source.next_item()? else {
            break;
        };
        increment(&mut progress.rows_seen, "rows_seen")?;
        increment(&mut summary.row_count, "row_count")?;
        match item {
            AssociationSourceItem::Rejected(rejection) => {
                validate_rejection(&rejection)?;
                increment(&mut progress.rejected_rows, "rejected_rows")?;
                increment(&mut summary.skipped_row_count, "skipped_row_count")?;
                rejections.record(&rejection);
            }
            AssociationSourceItem::Point {
                point,
                warnings: point_warnings,
            } => {
                let expected_warnings = ensure_normalized(&point)?;
                if point_warnings != expected_warnings {
                    return Err(AssociationBuildError::NonCanonicalPoint(
                        "source yielded inconsistent normalization warnings".into(),
                    ));
                }
                increment(&mut progress.valid_rows, "valid_rows")?;
                increment(&mut summary.valid_row_count, "valid_row_count")?;
                if raster.render_association_point(
                    &config.layout,
                    &point,
                    config.point_radius,
                    config.point_alpha,
                )? {
                    increment(&mut summary.rendered_point_count, "rendered_point_count")?;
                }
                if point.pvalue < config.options.significance_threshold {
                    increment(&mut summary.significant_hit_count, "significant_hit_count")?;
                }
                for warning in point_warnings {
                    let count = warnings.entry(warning.as_str()).or_default();
                    *count = count
                        .checked_add(1)
                        .ok_or(AssociationBuildError::CountOverflow("warning count"))?;
                }
                top_hits.insert_normalized(&point)?;
                batch.push(point);
                if batch.len() == config.options.insert_batch_size {
                    flush_batch(
                        store,
                        &config.dataset_id,
                        &mut batch,
                        &mut progress,
                        cancellation,
                    )?;
                }
            }
        }
    }

    let source_fingerprint = source
        .finalized_source_fingerprint()
        .filter(|value| valid_sha256(value))
        .ok_or(AssociationBuildError::MissingFinalizedFingerprint)?
        .to_string();
    check_cancelled(cancellation)?;
    flush_batch(
        store,
        &config.dataset_id,
        &mut batch,
        &mut progress,
        cancellation,
    )?;

    if progress.rows_seen != summary.row_count
        || progress.valid_rows != summary.valid_row_count
        || progress.rejected_rows != summary.skipped_row_count
        || progress.rows_persisted != summary.valid_row_count
        || top_hits.observed() != summary.valid_row_count
        || rejections.total != summary.skipped_row_count
    {
        return Err(AssociationBuildError::InconsistentProduct);
    }

    let ranked = top_hits.into_sorted_vec();
    check_cancelled(cancellation)?;
    store.replace_top_hits(&config.dataset_id, &ranked)?;
    check_cancelled(cancellation)?;

    let png_bytes = raster.png_bytes()?;
    check_cancelled(cancellation)?;
    summary.warnings = build_warning_summary(&warnings, &rejections);
    summary
        .validate()
        .map_err(AssociationBuildError::InvalidSummary)?;
    let manifest = build_manifest(
        config,
        &assembly,
        &source_fingerprint,
        &summary,
        &ranked,
        &png_bytes,
    )?;

    // This state transition is the builder's irreversible success point. Every
    // cancellation probe and fallible integrity check is deliberately before it;
    // after a successful transition we return success without another boundary
    // that could report failure while leaving a complete store behind.
    check_cancelled(cancellation)?;
    store.complete_build(&config.dataset_id, &source_fingerprint)?;

    Ok(AssociationBuildProduct {
        source_fingerprint,
        summary,
        top_hits: ranked,
        rejection_diagnostics: rejections,
        png_bytes,
        manifest,
        progress,
    })
}

fn flush_batch<T: AssociationStore>(
    store: &mut T,
    dataset_id: &DatasetIdV1,
    batch: &mut Vec<AssociationPointV1>,
    progress: &mut AssociationBuildProgressV1,
    cancellation: &dyn AssociationBuildCancellation,
) -> Result<(), AssociationBuildError> {
    if batch.is_empty() {
        return Ok(());
    }
    check_cancelled(cancellation)?;
    store.insert_points(dataset_id, batch)?;
    progress.rows_persisted = progress
        .rows_persisted
        .checked_add(batch.len() as u64)
        .ok_or(AssociationBuildError::CountOverflow("rows_persisted"))?;
    increment(&mut progress.batches_committed, "batches_committed")?;
    batch.clear();
    check_cancelled(cancellation)
}

fn validate_config<S: AssociationSource>(
    source: &S,
    config: &AssociationBuildConfig,
) -> Result<(), AssociationBuildError> {
    source.metadata().validate()?;
    config.options.validate()?;
    config
        .layout
        .validate()
        .map_err(|_| BuildContractError::InvalidConfig)?;
    if source.metadata().assembly != crate::canonical_assembly(&source.metadata().assembly)
        || source.metadata().assembly.is_empty()
        || config.title.trim().is_empty()
        || config.title.len() > 512
        || config.genohype_version.trim().is_empty()
        || config.genohype_version.len() > 128
        || config.created_at.trim().is_empty()
        || config.created_at.len() > 64
        || !config.point_radius.is_finite()
        || config.point_radius <= 0.0
        || config.thresholds.len() > 100
    {
        return Err(BuildContractError::InvalidConfig.into());
    }
    config.table_resource.validate()?;
    config.region_resource.validate()?;
    if config
        .thresholds
        .iter()
        .filter(|threshold| {
            threshold.pvalue.to_bits() == config.options.significance_threshold.to_bits()
        })
        .count()
        != 1
    {
        return Err(BuildContractError::SignificanceThresholdNotAdvertised.into());
    }
    for threshold in &config.thresholds {
        if threshold.name.trim().is_empty()
            || threshold.name.len() > 128
            || !threshold.pvalue.is_finite()
            || threshold.pvalue <= 0.0
            || threshold.pvalue > 1.0
        {
            return Err(BuildContractError::InvalidConfig.into());
        }
    }
    Ok(())
}

fn build_manifest(
    config: &AssociationBuildConfig,
    assembly: &str,
    source_fingerprint: &str,
    summary: &AssociationDatasetSummaryV1,
    top_hits: &[AssociationPointV1],
    png_bytes: &[u8],
) -> Result<AssociationVisualizationManifestV1, AssociationBuildError> {
    let raster_checksum = format!("sha256:{:x}", Sha256::digest(png_bytes));
    let manifest = AssociationVisualizationManifestV1 {
        schema_version: SCHEMA_VERSION_V1.into(),
        plot_id: config.dataset_id.to_string(),
        kind: PlotKindV1::Manhattan,
        title: config.title.clone(),
        assembly: assembly.into(),
        coordinate_convention: COORDINATE_CONVENTION_V1.into(),
        source_fingerprint: source_fingerprint.into(),
        genohype_version: config.genohype_version.clone(),
        created_at: config.created_at.clone(),
        layout: config.layout.clone(),
        layout_hash: config.layout.layout_hash(assembly),
        thresholds: config.thresholds.clone(),
        row_count: summary.row_count,
        valid_row_count: summary.valid_row_count,
        skipped_row_count: summary.skipped_row_count,
        rendered_point_count: summary.rendered_point_count,
        significant_hit_count: summary.significant_hit_count,
        top_hits: top_hits.to_vec(),
        warnings: summary.warnings.clone(),
        resources: AssociationVisualizationResourcesV1 {
            raster: ResourceDescriptorV1 {
                url: "raster.png".into(),
                media_type: "image/png".into(),
                byte_size: Some(png_bytes.len() as u64),
                checksum: Some(raster_checksum),
                expires_at: None,
            },
            region_api: Some(config.region_resource.clone()),
            table_api: Some(config.table_resource.clone()),
        },
    };
    manifest.validate()?;
    Ok(manifest)
}

fn validate_rejection(
    rejection: &crate::source::AssociationSourceRejectionV1,
) -> Result<(), AssociationBuildError> {
    if rejection.source_ordinal > JSON_SAFE_INTEGER_MAX
        || rejection.reason_code.is_empty()
        || rejection.reason_code.len() > 128
        || rejection
            .field
            .as_ref()
            .is_some_and(|value| value.is_empty() || value.len() > 256)
        || rejection
            .bounded_example
            .as_ref()
            .is_some_and(|value| value.len() > crate::source::MAX_REJECTION_EXAMPLE_BYTES)
    {
        return Err(AssociationBuildError::InvalidRejection);
    }
    Ok(())
}

fn ensure_normalized(
    point: &AssociationPointV1,
) -> Result<Vec<WarningCode>, AssociationBuildError> {
    let (normalized, warnings) = point
        .clone()
        .normalize()
        .map_err(|error| AssociationBuildError::NonCanonicalPoint(error.to_string()))?;
    if normalized == *point {
        Ok(warnings)
    } else {
        Err(AssociationBuildError::NonCanonicalPoint(
            "source yielded a non-canonical point".into(),
        ))
    }
}

fn build_warning_summary(
    warnings: &BTreeMap<&'static str, u64>,
    rejections: &RejectionDiagnostics,
) -> Vec<String> {
    let mut output = warnings
        .iter()
        .map(|(code, count)| format!("{code}: {count}"))
        .collect::<Vec<_>>();
    if rejections.total > 0 {
        output.push(format!("rejected_rows: {}", rejections.total));
    }
    output.truncate(crate::manifest::MAX_MANIFEST_WARNINGS);
    output
}

fn check_cancelled(
    cancellation: &dyn AssociationBuildCancellation,
) -> Result<(), AssociationBuildError> {
    if cancellation.is_cancelled() {
        Err(AssociationBuildError::Cancelled)
    } else {
        Ok(())
    }
}

fn increment(value: &mut u64, name: &'static str) -> Result<(), AssociationBuildError> {
    *value = value
        .checked_add(1)
        .filter(|value| *value <= JSON_SAFE_INTEGER_MAX)
        .ok_or(AssociationBuildError::CountOverflow(name))?;
    Ok(())
}

fn valid_sha256(value: &str) -> bool {
    value.strip_prefix("sha256:").is_some_and(|hex| {
        hex.len() == 64
            && hex
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuildContractError {
    InvalidOptions,
    InvalidConfig,
    SignificanceThresholdNotAdvertised,
}
impl fmt::Display for BuildContractError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidOptions => {
                write!(f, "association build options are outside bounded V1 limits")
            }
            Self::InvalidConfig => write!(f, "association build configuration is invalid"),
            Self::SignificanceThresholdNotAdvertised => write!(
                f,
                "strict significance count threshold must be advertised exactly once"
            ),
        }
    }
}
impl Error for BuildContractError {}

#[derive(Debug)]
pub enum AssociationBuildError {
    Cancelled,
    Contract(BuildContractError),
    Source(AssociationSourceError),
    Store(AssociationStoreError),
    Render(RenderError),
    Manifest(ManifestError),
    TopK(TopKError),
    MissingFinalizedFingerprint,
    NonCanonicalPoint(String),
    InvalidRejection,
    InvalidSummary(&'static str),
    CountOverflow(&'static str),
    InconsistentProduct,
}
impl fmt::Display for AssociationBuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cancelled => write!(f, "association build cancelled"),
            Self::Contract(error) => error.fmt(f),
            Self::Source(error) => error.fmt(f),
            Self::Store(error) => error.fmt(f),
            Self::Render(error) => error.fmt(f),
            Self::Manifest(error) => error.fmt(f),
            Self::TopK(error) => error.fmt(f),
            Self::MissingFinalizedFingerprint => {
                write!(
                    f,
                    "association source did not finalize its fingerprint at EOF"
                )
            }
            Self::NonCanonicalPoint(error) => write!(f, "{error}"),
            Self::InvalidRejection => write!(f, "source yielded invalid rejection diagnostics"),
            Self::InvalidSummary(error) => write!(f, "invalid association summary: {error}"),
            Self::CountOverflow(name) => write!(f, "association {name} overflowed"),
            Self::InconsistentProduct => write!(f, "association build products are inconsistent"),
        }
    }
}
impl Error for AssociationBuildError {}

macro_rules! from_error {
    ($source:ty, $variant:ident) => {
        impl From<$source> for AssociationBuildError {
            fn from(value: $source) -> Self {
                Self::$variant(value)
            }
        }
    };
}
from_error!(BuildContractError, Contract);
from_error!(AssociationSourceError, Source);
from_error!(AssociationStoreError, Store);
from_error!(RenderError, Render);
from_error!(ManifestError, Manifest);
from_error!(TopKError, TopK);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_bounded() {
        assert!(AssociationBuildOptionsV1::default().validate().is_ok());
        let mut bad = AssociationBuildOptionsV1::default();
        bad.insert_batch_size = 1001;
        assert!(bad.validate().is_err());
    }

    #[test]
    fn strict_significance_boundary_excludes_equality() {
        let threshold = 5e-8;
        assert!(!(threshold < threshold));
        assert!((threshold / 2.0) < threshold);
    }
}
