//! Versioned contracts and deterministic semantic reduction for locus drilldown.
//!
//! Public coordinates are signed 64-bit, 1-based, and inclusive.  The raster
//! remains dense: every source point is drawn in source order.  Only the
//! browser-facing semantic overlay is reduced by `pixel_strata_v1`.

use crate::layout::{
    canonical_assembly, canonical_contig, validate_raster_dimensions, LayoutError,
};
use crate::manifest::{
    validate_schema_version, ManifestError, ResourceDescriptorV1, COORDINATE_CONVENTION_V1,
};
use crate::reduction::compare_association_points;
use crate::scale::YScale;
use crate::session::DatasetIdV1;
use crate::{AssociationPointV1, JSON_SAFE_INTEGER_MAX};
use schemars::{schema_for, JsonSchema, Schema};
use serde::{de, Deserialize, Deserializer, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt;

pub const ASSOCIATION_REGION_SCHEMA_VERSION_V1: &str = "1.0";
pub const REGION_QUANTUM_BASES_V1: i64 = 1_000;
pub const REGION_WIDTH_QUANTUM_V1: u32 = 50;
pub const MIN_REGION_WIDTH: u32 = 50;
pub const MAX_REGION_WIDTH: u32 = 4_096;
pub const MIN_REGION_HEIGHT: u32 = 64;
pub const MAX_REGION_HEIGHT: u32 = 2_048;
pub const MIN_REGION_DPR: u8 = 1;
pub const MAX_REGION_DPR: u8 = 4;
pub const MAX_REGION_OVERLAY_POINTS: usize = 1_000;
pub const MAX_REGION_OVERLAY_TEXT_BYTES: usize = 1_048_576;
pub const MAX_GENE_FEATURES: usize = 1_000;
pub const MAX_GENE_TEXT_BYTES: usize = 1_048_576;
/// V1 ordinary points occupy at most 125 genomic x strata by eight canonical
/// significance strata. One canonical best point represents each occupied bin.
pub const REGION_X_STRATA_V1: u32 = 125;
pub const REGION_Y_STRATA_V1: u8 = 8;
pub const REGION_DENSE_RULE_V1: &str = "pixel_strata_v1";
pub const LOCUS_RENDERER_VERSION_V1: &str = "locus_rasterizer_v1";

/// Exact Sequence Ontology terms treated as protein-coding consequences in V1.
///
/// Matching is deliberately exact and case-sensitive after trimming. Product
/// labels such as `pLoF`, `missense`, and arbitrary free text are not silently
/// promoted to coding status.
pub const CODING_CONSEQUENCE_V1: &[&str] = &[
    "coding_sequence_variant",
    "frameshift_variant",
    "inframe_deletion",
    "inframe_insertion",
    "incomplete_terminal_codon_variant",
    "missense_variant",
    "protein_altering_variant",
    "splice_acceptor_variant",
    "splice_donor_variant",
    "start_lost",
    "start_retained_variant",
    "stop_gained",
    "stop_lost",
    "stop_retained_variant",
    "synonymous_variant",
    "transcript_ablation",
    "transcript_amplification",
];

pub fn coding_consequence_v1(value: Option<&str>) -> bool {
    value.is_some_and(|value| CODING_CONSEQUENCE_V1.binary_search(&value.trim()).is_ok())
}

/// Canonical quantized region geometry. `width` and `height` are logical CSS
/// pixels; `physical_*` are the exact raster dimensions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct RegionLayoutV1 {
    #[schemars(length(min = 1, max = 4096))]
    pub contig: String,
    #[schemars(range(min = 1, max = 9007199254740991_i64))]
    pub contig_length: i64,
    #[schemars(range(min = 1, max = 9007199254740991_i64))]
    pub requested_start: i64,
    #[schemars(range(min = 1, max = 9007199254740991_i64))]
    pub requested_end: i64,
    #[schemars(range(min = 1, max = 9007199254740991_i64))]
    pub effective_start: i64,
    #[schemars(range(min = 1, max = 9007199254740991_i64))]
    pub effective_end: i64,
    #[schemars(range(min = 50, max = 4096_u32))]
    pub requested_width: u32,
    #[schemars(range(min = 50, max = 4100_u32))]
    pub width: u32,
    #[schemars(range(min = 64, max = 2048_u32))]
    pub height: u32,
    #[schemars(range(min = 1, max = 4_u8))]
    pub dpr: u8,
    #[schemars(range(min = 1, max = 16384_u32))]
    pub physical_width: u32,
    #[schemars(range(min = 1, max = 16384_u32))]
    pub physical_height: u32,
    pub y_scale: YScale,
}

impl RegionLayoutV1 {
    pub fn new(
        contig: impl Into<String>,
        contig_length: i64,
        requested_start: i64,
        requested_end: i64,
        width: u32,
        height: u32,
        dpr: u8,
    ) -> Result<Self, RegionContractError> {
        let contig = canonical_contig(&contig.into()).ok_or(RegionContractError::InvalidContig)?;
        if contig_length < 1 || contig_length as u64 > JSON_SAFE_INTEGER_MAX {
            return Err(RegionContractError::InvalidContigLength);
        }
        if requested_start < 1 || requested_end < requested_start || requested_end > contig_length {
            return Err(RegionContractError::InvalidBounds);
        }
        if !(MIN_REGION_WIDTH..=MAX_REGION_WIDTH).contains(&width)
            || !(MIN_REGION_HEIGHT..=MAX_REGION_HEIGHT).contains(&height)
            || !(MIN_REGION_DPR..=MAX_REGION_DPR).contains(&dpr)
        {
            return Err(RegionContractError::InvalidViewport);
        }

        let effective_start = requested_start
            .checked_sub(1)
            .and_then(|value| value.checked_div(REGION_QUANTUM_BASES_V1))
            .and_then(|value| value.checked_mul(REGION_QUANTUM_BASES_V1))
            .and_then(|value| value.checked_add(1))
            .ok_or(RegionContractError::ArithmeticOverflow)?;
        let end_blocks = requested_end
            .checked_div(REGION_QUANTUM_BASES_V1)
            .and_then(|blocks| {
                blocks.checked_add(i64::from(requested_end % REGION_QUANTUM_BASES_V1 != 0))
            })
            .ok_or(RegionContractError::ArithmeticOverflow)?;
        let effective_end = end_blocks
            .checked_mul(REGION_QUANTUM_BASES_V1)
            .ok_or(RegionContractError::ArithmeticOverflow)?
            .min(contig_length);
        let requested_width = width;
        let width = width
            .checked_add(REGION_WIDTH_QUANTUM_V1 - 1)
            .and_then(|value| value.checked_div(REGION_WIDTH_QUANTUM_V1))
            .and_then(|value| value.checked_mul(REGION_WIDTH_QUANTUM_V1))
            .ok_or(RegionContractError::ArithmeticOverflow)?;
        let physical_width = width
            .checked_mul(u32::from(dpr))
            .ok_or(RegionContractError::ArithmeticOverflow)?;
        let physical_height = height
            .checked_mul(u32::from(dpr))
            .ok_or(RegionContractError::ArithmeticOverflow)?;
        validate_raster_dimensions(physical_width, physical_height)
            .map_err(RegionContractError::RasterDimensions)?;

        Ok(Self {
            contig,
            contig_length,
            requested_start,
            requested_end,
            effective_start,
            effective_end,
            requested_width,
            width,
            height,
            dpr,
            physical_width,
            physical_height,
            y_scale: YScale::new(physical_height),
        })
    }

    pub fn validate(&self) -> Result<(), RegionContractError> {
        let expected = Self::new(
            self.contig.clone(),
            self.contig_length,
            self.requested_start,
            self.requested_end,
            self.requested_width,
            self.height,
            self.dpr,
        )?;
        if &expected != self {
            return Err(RegionContractError::NonCanonicalLayout);
        }
        Ok(())
    }

    /// Map inclusive endpoints exactly into physical `[0, width - 1]`.
    pub fn x_for(&self, position: i64) -> Option<f32> {
        if position < self.effective_start || position > self.effective_end {
            return None;
        }
        if self.effective_start == self.effective_end || self.physical_width == 1 {
            return Some(0.0);
        }
        let numerator = (position - self.effective_start) as f64;
        let denominator = (self.effective_end - self.effective_start) as f64;
        Some((numerator / denominator * f64::from(self.physical_width - 1)) as f32)
    }

    pub fn y_for(&self, significance: f64) -> f32 {
        self.y_scale.y_for(significance)
    }

    pub fn layout_hash(&self, assembly: &str) -> String {
        let mut digest = Sha256::new();
        digest.update(b"genohype.association.region-layout.v1\0");
        update_string(&mut digest, &canonical_assembly(assembly));
        update_string(&mut digest, &self.contig);
        for value in [
            self.contig_length,
            self.requested_start,
            self.requested_end,
            self.effective_start,
            self.effective_end,
        ] {
            digest.update(value.to_be_bytes());
        }
        digest.update(self.requested_width.to_be_bytes());
        digest.update(self.width.to_be_bytes());
        digest.update(self.height.to_be_bytes());
        digest.update([self.dpr]);
        digest.update(self.physical_width.to_be_bytes());
        digest.update(self.physical_height.to_be_bytes());
        digest.update(self.y_scale.log_threshold.to_bits().to_be_bytes());
        digest.update(self.y_scale.linear_fraction.to_bits().to_be_bytes());
        digest.update(self.y_scale.max_neg_log_p.to_bits().to_be_bytes());
        format!("sha256:{:x}", digest.finalize())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AssociationRegionResourcesV1 {
    pub raster: ResourceDescriptorV1,
    pub overlay: ResourceDescriptorV1,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AssociationRegionCountsV1 {
    #[schemars(range(max = 9007199254740991_u64))]
    pub source_point_count: u64,
    #[schemars(range(max = 9007199254740991_u64))]
    pub rasterized_point_count: u64,
    #[schemars(range(max = 9007199254740991_u64))]
    pub significant_point_count: u64,
    #[schemars(range(max = 9007199254740991_u64))]
    pub coding_point_count: u64,
    #[schemars(range(max = 9007199254740991_u64))]
    pub selected_point_count: u64,
    #[schemars(range(max = 9007199254740991_u64))]
    pub overlay_point_count: u64,
}

/// Complete accounting for dense raster input and bounded semantic output.
/// Category counts may overlap; `protected_union_count` is their exact union.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RegionAggregationDisclosureV1 {
    #[schemars(length(min = 1, max = 64))]
    pub rule_id: String,
    #[schemars(range(max = 9007199254740991_u64))]
    pub input_point_count: u64,
    #[schemars(range(max = 9007199254740991_u64))]
    pub protected_selected_count: u64,
    #[schemars(range(max = 9007199254740991_u64))]
    pub protected_significant_count: u64,
    #[schemars(range(max = 9007199254740991_u64))]
    pub protected_coding_count: u64,
    #[schemars(range(max = 9007199254740991_u64))]
    pub protected_union_count: u64,
    #[schemars(range(max = 9007199254740991_u64))]
    pub ordinary_input_count: u64,
    #[schemars(range(max = 9007199254740991_u64))]
    pub ordinary_budget: u64,
    #[schemars(range(max = 9007199254740991_u64))]
    pub rendered_ordinary_count: u64,
    #[schemars(range(max = 9007199254740991_u64))]
    pub omitted_ordinary_count: u64,
    #[schemars(range(max = 9007199254740991_u64))]
    pub rasterized_point_count: u64,
    #[schemars(range(max = 9007199254740991_u64))]
    pub overlay_point_count: u64,
}

impl RegionAggregationDisclosureV1 {
    pub fn validate(&self) -> Result<(), RegionContractError> {
        let values = [
            self.input_point_count,
            self.protected_selected_count,
            self.protected_significant_count,
            self.protected_coding_count,
            self.protected_union_count,
            self.ordinary_input_count,
            self.ordinary_budget,
            self.rendered_ordinary_count,
            self.omitted_ordinary_count,
            self.rasterized_point_count,
            self.overlay_point_count,
        ];
        if values
            .into_iter()
            .any(|value| value > JSON_SAFE_INTEGER_MAX)
            || self.rule_id != REGION_DENSE_RULE_V1
            || self.protected_union_count + self.ordinary_input_count != self.input_point_count
            || self.rendered_ordinary_count + self.omitted_ordinary_count
                != self.ordinary_input_count
            || self.rendered_ordinary_count > self.ordinary_budget
            || self.rasterized_point_count != self.input_point_count
            || self.overlay_point_count != self.protected_union_count + self.rendered_ordinary_count
        {
            return Err(RegionContractError::InconsistentCounts);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AssociationRegionRenderManifestV1 {
    #[serde(deserialize_with = "deserialize_region_v1_schema_version")]
    #[schemars(regex(pattern = "^1\\.[0-9]+$"))]
    pub schema_version: String,
    pub dataset_id: DatasetIdV1,
    #[schemars(length(min = 1, max = 512))]
    pub title: String,
    #[schemars(length(min = 1, max = 64))]
    pub assembly: String,
    pub coordinate_convention: String,
    #[schemars(length(min = 1, max = 128))]
    pub source_fingerprint: String,
    pub layout: RegionLayoutV1,
    #[schemars(length(min = 71, max = 71), regex(pattern = "^sha256:[0-9a-f]{64}$"))]
    pub layout_hash: String,
    #[schemars(length(min = 71, max = 71), regex(pattern = "^sha256:[0-9a-f]{64}$"))]
    pub render_hash: String,
    pub significance_threshold: f64,
    pub counts: AssociationRegionCountsV1,
    pub aggregation: RegionAggregationDisclosureV1,
    pub resources: AssociationRegionResourcesV1,
}

impl AssociationRegionRenderManifestV1 {
    pub fn expected_render_hash(&self) -> String {
        region_render_hash(
            &self.layout_hash,
            &self.source_fingerprint,
            self.significance_threshold,
        )
    }

    pub fn validate(&self) -> Result<(), RegionContractError> {
        validate_schema_version(&self.schema_version).map_err(RegionContractError::Manifest)?;
        if self.title.trim().is_empty()
            || self.assembly.trim().is_empty()
            || self.source_fingerprint.trim().is_empty()
            || self.coordinate_convention != COORDINATE_CONVENTION_V1
            || self.title.len() > 512
            || self.assembly.len() > 64
            || self.source_fingerprint.len() > 128
        {
            return Err(RegionContractError::InvalidMetadata);
        }
        validate_threshold(self.significance_threshold)?;
        self.layout.validate()?;
        let expected_layout = self.layout.layout_hash(&self.assembly);
        if self.layout_hash != expected_layout {
            return Err(RegionContractError::LayoutHashMismatch);
        }
        if self.render_hash != self.expected_render_hash() {
            return Err(RegionContractError::RenderHashMismatch);
        }
        self.aggregation.validate()?;
        if self.counts.source_point_count != self.aggregation.input_point_count
            || self.counts.rasterized_point_count != self.aggregation.rasterized_point_count
            || self.counts.significant_point_count != self.aggregation.protected_significant_count
            || self.counts.coding_point_count != self.aggregation.protected_coding_count
            || self.counts.selected_point_count != self.aggregation.protected_selected_count
            || self.counts.overlay_point_count != self.aggregation.overlay_point_count
        {
            return Err(RegionContractError::InconsistentCounts);
        }
        self.resources
            .raster
            .validate()
            .map_err(RegionContractError::Manifest)?;
        self.resources
            .overlay
            .validate()
            .map_err(RegionContractError::Manifest)?;
        if self.resources.raster.media_type != "image/png"
            || self.resources.overlay.media_type != "application/json"
        {
            return Err(RegionContractError::InvalidResourceMediaType);
        }
        Ok(())
    }
}

pub fn region_render_hash(layout_hash: &str, source_fingerprint: &str, threshold: f64) -> String {
    let mut digest = Sha256::new();
    digest.update(b"genohype.association.region-render.v1\0");
    update_string(&mut digest, layout_hash);
    update_string(&mut digest, source_fingerprint);
    digest.update(threshold.to_bits().to_be_bytes());
    update_string(&mut digest, REGION_DENSE_RULE_V1);
    update_string(&mut digest, LOCUS_RENDERER_VERSION_V1);
    format!("sha256:{:x}", digest.finalize())
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AssociationRegionOverlayPointV1 {
    pub point: AssociationPointV1,
    /// Exact selection identity is `point.source_ordinal`.
    pub selected: bool,
    /// Strictly `pvalue < threshold`, with p-value zero included as underflow.
    pub significant: bool,
    /// Derived only from the fixed `coding_consequence_v1` SO vocabulary.
    pub coding: bool,
    pub ordinary_representative: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum GeneStrandV1 {
    Plus,
    Minus,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct GeneFeatureV1 {
    #[schemars(length(min = 1, max = 4096))]
    pub id: String,
    #[schemars(length(max = 4096))]
    pub symbol: Option<String>,
    #[schemars(length(min = 1, max = 4096))]
    pub contig: String,
    #[schemars(range(min = 1, max = 9007199254740991_i64))]
    pub start: i64,
    #[schemars(range(min = 1, max = 9007199254740991_i64))]
    pub end: i64,
    pub strand: GeneStrandV1,
}

impl GeneFeatureV1 {
    pub fn normalize(mut self, request: &GeneRegionRequestV1) -> Result<Self, GeneReferenceError> {
        self.contig = canonical_contig(&self.contig).ok_or(GeneReferenceError::InvalidFeature)?;
        if self.id.trim().is_empty()
            || self.id.len() > 4_096
            || self
                .symbol
                .as_ref()
                .is_some_and(|value| value.len() > 4_096)
            || self.contig != request.contig
            || self.start < 1
            || self.end < self.start
            || self.start > request.end
            || self.end < request.start
        {
            return Err(GeneReferenceError::InvalidFeature);
        }
        self.start = self.start.max(request.start);
        self.end = self.end.min(request.end);
        Ok(self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum GeneAnnotationStateV1 {
    Unavailable,
    Available,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct GeneAnnotationsV1 {
    pub state: GeneAnnotationStateV1,
    #[schemars(length(max = 1000))]
    pub features: Vec<GeneFeatureV1>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 1024))]
    pub message: Option<String>,
}

impl GeneAnnotationsV1 {
    pub fn validate(&self, layout: &RegionLayoutV1) -> Result<(), RegionContractError> {
        if self.features.len() > MAX_GENE_FEATURES
            || self
                .message
                .as_ref()
                .is_some_and(|value| value.len() > 1_024)
            || (self.state != GeneAnnotationStateV1::Available && !self.features.is_empty())
        {
            return Err(RegionContractError::InvalidGenes);
        }
        let request = GeneRegionRequestV1 {
            assembly: String::new(),
            contig: layout.contig.clone(),
            start: layout.effective_start,
            end: layout.effective_end,
        };
        let mut text_bytes = self.message.as_ref().map_or(0, String::len);
        let mut previous: Option<&GeneFeatureV1> = None;
        for feature in &self.features {
            let normalized = feature
                .clone()
                .normalize(&request)
                .map_err(|_| RegionContractError::InvalidGenes)?;
            if normalized != *feature {
                return Err(RegionContractError::InvalidGenes);
            }
            text_bytes = text_bytes
                .saturating_add(feature.id.len())
                .saturating_add(feature.symbol.as_ref().map_or(0, String::len))
                .saturating_add(feature.contig.len());
            if previous.is_some_and(|prior| gene_cmp(prior, feature).is_gt()) {
                return Err(RegionContractError::InvalidGenes);
            }
            previous = Some(feature);
        }
        if text_bytes > MAX_GENE_TEXT_BYTES {
            return Err(RegionContractError::InvalidGenes);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AssociationRegionOverlayV1 {
    #[serde(deserialize_with = "deserialize_region_v1_schema_version")]
    #[schemars(regex(pattern = "^1\\.[0-9]+$"))]
    pub schema_version: String,
    #[schemars(length(min = 71, max = 71), regex(pattern = "^sha256:[0-9a-f]{64}$"))]
    pub layout_hash: String,
    #[schemars(length(min = 71, max = 71), regex(pattern = "^sha256:[0-9a-f]{64}$"))]
    pub render_hash: String,
    pub layout: RegionLayoutV1,
    pub significance_threshold: f64,
    #[schemars(length(max = 1000))]
    pub points: Vec<AssociationRegionOverlayPointV1>,
    pub gene_annotations: GeneAnnotationsV1,
    pub aggregation: RegionAggregationDisclosureV1,
}

impl AssociationRegionOverlayV1 {
    pub fn validate(&self) -> Result<(), RegionContractError> {
        validate_schema_version(&self.schema_version).map_err(RegionContractError::Manifest)?;
        self.layout.validate()?;
        validate_threshold(self.significance_threshold)?;
        self.aggregation.validate()?;
        if self.points.len() > MAX_REGION_OVERLAY_POINTS
            || self.points.len() as u64 != self.aggregation.overlay_point_count
        {
            return Err(RegionContractError::InconsistentCounts);
        }
        if !is_sha256_digest(&self.layout_hash) || !is_sha256_digest(&self.render_hash) {
            return Err(RegionContractError::RenderHashMismatch);
        }
        let mut point_text_bytes = 0_usize;
        let mut source_ordinals = HashSet::with_capacity(self.points.len());
        let mut previous: Option<&AssociationPointV1> = None;
        for entry in &self.points {
            let (point, _) = entry
                .point
                .clone()
                .normalize()
                .map_err(|_| RegionContractError::InvalidOverlayPoint)?;
            point_text_bytes = point_text_bytes.saturating_add(point_text_len(&point));
            if !source_ordinals.insert(point.source_ordinal)
                || previous.is_some_and(|prior| compare_association_points(prior, &point).is_gt())
                || point != entry.point
                || point.contig != self.layout.contig
                || self.layout.x_for(point.position).is_none()
                || entry.significant != is_significant(&point, self.significance_threshold)
                || entry.coding != coding_consequence_v1(point.consequence.as_deref())
                || entry.ordinary_representative
                    == (entry.selected || entry.significant || entry.coding)
            {
                return Err(RegionContractError::InvalidOverlayPoint);
            }
            previous = Some(&entry.point);
        }
        if point_text_bytes > MAX_REGION_OVERLAY_TEXT_BYTES {
            return Err(RegionContractError::OverlayTextLimit);
        }
        self.gene_annotations.validate(&self.layout)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct GeneRegionRequestV1 {
    pub assembly: String,
    pub contig: String,
    pub start: i64,
    pub end: i64,
}

pub trait GeneReferenceProvider: Send + Sync {
    fn genes_overlapping(
        &self,
        request: &GeneRegionRequestV1,
    ) -> Result<Vec<GeneFeatureV1>, GeneReferenceError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GeneReferenceError {
    Unavailable,
    AssemblyMismatch,
    InvalidFeature,
    LimitExceeded,
    ProviderFailure,
}

impl fmt::Display for GeneReferenceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "gene reference query failed: {:?}", self)
    }
}
impl Error for GeneReferenceError {}

/// Build a bounded deterministic semantic overlay. Input iteration order and
/// batch boundaries do not affect the result.
pub fn reduce_region_overlay<I>(
    layout: &RegionLayoutV1,
    points: I,
    selected_ordinals: &HashSet<u64>,
    significance_threshold: f64,
    gene_annotations: GeneAnnotationsV1,
) -> Result<AssociationRegionOverlayV1, RegionContractError>
where
    I: IntoIterator<Item = AssociationPointV1>,
{
    validate_threshold(significance_threshold)?;
    let mut protected = Vec::new();
    let mut buckets: HashMap<(u32, u8), AssociationPointV1> = HashMap::new();
    let mut input = 0_u64;
    let mut selected_count = 0_u64;
    let mut significant_count = 0_u64;
    let mut coding_count = 0_u64;
    let mut ordinary_count = 0_u64;

    for point in points {
        let (point, _) = point
            .normalize()
            .map_err(|_| RegionContractError::InvalidOverlayPoint)?;
        if point.contig != layout.contig || layout.x_for(point.position).is_none() {
            return Err(RegionContractError::InvalidOverlayPoint);
        }
        input = input
            .checked_add(1)
            .ok_or(RegionContractError::ArithmeticOverflow)?;
        let selected = selected_ordinals.contains(&point.source_ordinal);
        let significant = is_significant(&point, significance_threshold);
        let coding = coding_consequence_v1(point.consequence.as_deref());
        selected_count += u64::from(selected);
        significant_count += u64::from(significant);
        coding_count += u64::from(coding);
        if selected || significant || coding {
            protected.push(AssociationRegionOverlayPointV1 {
                point,
                selected,
                significant,
                coding,
                ordinary_representative: false,
            });
            if protected.len() > MAX_REGION_OVERLAY_POINTS {
                return Err(RegionContractError::ProtectedOverlayLimit {
                    count: protected.len(),
                    maximum: MAX_REGION_OVERLAY_POINTS,
                });
            }
        } else {
            ordinary_count += 1;
            let x = layout
                .x_for(point.position)
                .expect("point was range checked");
            let x_bucket_count = layout.physical_width.min(REGION_X_STRATA_V1);
            let x_bucket = ((f64::from(x) * f64::from(x_bucket_count)
                / f64::from(layout.physical_width))
            .floor() as u32)
                .min(x_bucket_count - 1);
            let clipped = point.neg_log10_p.clamp(0.0, layout.y_scale.max_neg_log_p);
            let stratum = ((clipped / layout.y_scale.max_neg_log_p * f64::from(REGION_Y_STRATA_V1))
                .floor() as u8)
                .min(REGION_Y_STRATA_V1 - 1);
            buckets
                .entry((x_bucket, stratum))
                .and_modify(|current| {
                    if compare_association_points(&point, current).is_lt() {
                        *current = point.clone();
                    }
                })
                .or_insert(point);
        }
    }

    let ordinary_budget = MAX_REGION_OVERLAY_POINTS - protected.len();
    let mut representatives: Vec<_> = buckets.into_values().collect();
    representatives.sort_by(compare_association_points);
    representatives.truncate(ordinary_budget);
    let rendered_ordinary = representatives.len() as u64;
    protected.extend(
        representatives
            .into_iter()
            .map(|point| AssociationRegionOverlayPointV1 {
                point,
                selected: false,
                significant: false,
                coding: false,
                ordinary_representative: true,
            }),
    );
    protected.sort_by(|left, right| compare_association_points(&left.point, &right.point));

    let disclosure = RegionAggregationDisclosureV1 {
        rule_id: REGION_DENSE_RULE_V1.into(),
        input_point_count: input,
        protected_selected_count: selected_count,
        protected_significant_count: significant_count,
        protected_coding_count: coding_count,
        protected_union_count: input - ordinary_count,
        ordinary_input_count: ordinary_count,
        ordinary_budget: ordinary_budget as u64,
        rendered_ordinary_count: rendered_ordinary,
        omitted_ordinary_count: ordinary_count - rendered_ordinary,
        rasterized_point_count: input,
        overlay_point_count: protected.len() as u64,
    };
    disclosure.validate()?;
    if protected
        .iter()
        .map(|entry| point_text_len(&entry.point))
        .sum::<usize>()
        > MAX_REGION_OVERLAY_TEXT_BYTES
    {
        return Err(RegionContractError::OverlayTextLimit);
    }
    gene_annotations.validate(layout)?;
    Ok(AssociationRegionOverlayV1 {
        schema_version: ASSOCIATION_REGION_SCHEMA_VERSION_V1.into(),
        layout_hash: String::new(),
        render_hash: String::new(),
        layout: layout.clone(),
        significance_threshold,
        points: protected,
        gene_annotations,
        aggregation: disclosure,
    })
}

pub fn normalize_gene_features(
    request: &GeneRegionRequestV1,
    features: Vec<GeneFeatureV1>,
) -> Result<Vec<GeneFeatureV1>, GeneReferenceError> {
    if features.len() > MAX_GENE_FEATURES {
        return Err(GeneReferenceError::LimitExceeded);
    }
    let canonical = canonical_contig(&request.contig).ok_or(GeneReferenceError::InvalidFeature)?;
    let request = GeneRegionRequestV1 {
        assembly: canonical_assembly(&request.assembly),
        contig: canonical,
        start: request.start,
        end: request.end,
    };
    if request.start < 1 || request.end < request.start {
        return Err(GeneReferenceError::InvalidFeature);
    }
    let mut text_bytes = 0_usize;
    let mut normalized = Vec::with_capacity(features.len());
    for feature in features {
        let feature = feature.normalize(&request)?;
        text_bytes = text_bytes
            .checked_add(feature.id.len() + feature.contig.len())
            .and_then(|value| value.checked_add(feature.symbol.as_ref().map_or(0, String::len)))
            .ok_or(GeneReferenceError::LimitExceeded)?;
        if text_bytes > MAX_GENE_TEXT_BYTES {
            return Err(GeneReferenceError::LimitExceeded);
        }
        normalized.push(feature);
    }
    normalized.sort_by(gene_cmp);
    Ok(normalized)
}

fn is_significant(point: &AssociationPointV1, threshold: f64) -> bool {
    point.pvalue == 0.0 || point.pvalue < threshold
}

fn point_text_len(point: &AssociationPointV1) -> usize {
    [
        Some(point.contig.as_str()),
        point.ref_allele.as_deref(),
        point.alt_allele.as_deref(),
        point.gene_symbol.as_deref(),
        point.consequence.as_deref(),
        point.series.as_deref(),
    ]
    .into_iter()
    .flatten()
    .map(str::len)
    .sum()
}

fn is_sha256_digest(value: &str) -> bool {
    value.len() == 71
        && value.starts_with("sha256:")
        && value[7..]
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn validate_threshold(value: f64) -> Result<(), RegionContractError> {
    if value.is_finite() && value > 0.0 && value <= 1.0 {
        Ok(())
    } else {
        Err(RegionContractError::InvalidThreshold)
    }
}

fn gene_cmp(left: &GeneFeatureV1, right: &GeneFeatureV1) -> std::cmp::Ordering {
    left.start
        .cmp(&right.start)
        .then_with(|| left.end.cmp(&right.end))
        .then_with(|| left.id.cmp(&right.id))
        .then_with(|| left.symbol.cmp(&right.symbol))
}

fn update_string(digest: &mut Sha256, value: &str) {
    digest.update((value.len() as u64).to_be_bytes());
    digest.update(value.as_bytes());
}

fn deserialize_region_v1_schema_version<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let version = String::deserialize(deserializer)?;
    validate_schema_version(&version).map_err(de::Error::custom)?;
    Ok(version)
}

pub fn generate_region_render_manifest_schema() -> Schema {
    schema_for!(AssociationRegionRenderManifestV1)
}

pub fn generate_region_overlay_schema() -> Schema {
    schema_for!(AssociationRegionOverlayV1)
}

#[derive(Debug, PartialEq, Eq)]
pub enum RegionContractError {
    InvalidContig,
    InvalidContigLength,
    InvalidBounds,
    InvalidViewport,
    ArithmeticOverflow,
    RasterDimensions(LayoutError),
    NonCanonicalLayout,
    InvalidMetadata,
    InvalidThreshold,
    LayoutHashMismatch,
    RenderHashMismatch,
    InconsistentCounts,
    InvalidResourceMediaType,
    InvalidOverlayPoint,
    OverlayTextLimit,
    InvalidGenes,
    ProtectedOverlayLimit { count: usize, maximum: usize },
    Manifest(ManifestError),
}

impl fmt::Display for RegionContractError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ProtectedOverlayLimit { count, maximum } => write!(
                f,
                "protected overlay contains {count} points; maximum is {maximum}; narrow the interval"
            ),
            other => write!(f, "invalid association region contract: {other:?}"),
        }
    }
}
impl Error for RegionContractError {}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn point(
        position: i64,
        pvalue: f64,
        significance: f64,
        consequence: Option<&str>,
        ordinal: u64,
    ) -> AssociationPointV1 {
        AssociationPointV1 {
            contig: "1".into(),
            position,
            ref_allele: Some("A".into()),
            alt_allele: Some("G".into()),
            pvalue,
            neg_log10_p: significance,
            beta: None,
            standard_error: None,
            allele_frequency: None,
            allele_count: None,
            gene_symbol: None,
            consequence: consequence.map(str::to_string),
            series: None,
            source_ordinal: ordinal,
        }
    }

    fn layout() -> RegionLayoutV1 {
        RegionLayoutV1::new("chr1", 10_250, 1_001, 9_001, 51, 100, 2).unwrap()
    }

    fn no_genes() -> GeneAnnotationsV1 {
        GeneAnnotationsV1 {
            state: GeneAnnotationStateV1::Unavailable,
            features: vec![],
            message: None,
        }
    }

    #[test]
    fn exact_outward_quantization_and_endpoint_mapping() {
        for (start, expected) in [(1, 1), (1_000, 1), (1_001, 1_001)] {
            let value = RegionLayoutV1::new("1", 10_250, start, start, 50, 64, 1).unwrap();
            assert_eq!(value.effective_start, expected);
        }
        let value = layout();
        assert_eq!(
            (value.effective_start, value.effective_end),
            (1_001, 10_000)
        );
        assert_eq!(
            (value.requested_width, value.width, value.physical_width),
            (51, 100, 200)
        );
        assert_eq!(value.x_for(1_001), Some(0.0));
        assert_eq!(value.x_for(10_000), Some(199.0));
        assert!(value.x_for(1_000).is_none());
    }

    #[test]
    fn end_clamp_width_dpr_and_physical_limits_are_checked() {
        let value = RegionLayoutV1::new("1", 10_250, 10_001, 10_250, 4_096, 64, 1).unwrap();
        assert_eq!(value.effective_end, 10_250);
        assert_eq!(value.requested_width, 4_096);
        assert_eq!(value.width, 4_100);
        assert_eq!(value.physical_width, 4_100);
        assert!(matches!(
            RegionLayoutV1::new("1", 10_250, 10_001, 10_250, 4_096, 64, 4),
            Err(RegionContractError::RasterDimensions(
                LayoutError::DimensionsLimit
            ))
        ));
        assert_eq!(
            RegionLayoutV1::new("1", 1_000_000, 1, 1_000_000, 4_096, 2_048, 4).unwrap_err(),
            RegionContractError::RasterDimensions(LayoutError::DimensionsLimit)
        );
        for width in [0, 49, 4_097, u32::MAX] {
            assert!(RegionLayoutV1::new("1", 100, 1, 100, width, 64, 1).is_err());
        }
        for dpr in [0, 5] {
            assert!(RegionLayoutV1::new("1", 100, 1, 100, 50, 64, dpr).is_err());
        }
    }

    #[test]
    fn canonical_y_and_hashes_cover_every_render_input() {
        let base = layout();
        assert_eq!(base.y_for(0.0), 200.0);
        assert_eq!(base.y_for(10.0), 80.0);
        assert_eq!(base.y_for(350.0), 0.0);
        assert_eq!(base.y_for(999.0), 0.0);
        let hash = base.layout_hash("GRCh38");
        assert_ne!(
            hash,
            RegionLayoutV1::new("1", 10_250, 1_002, 9_001, 51, 100, 2)
                .unwrap()
                .layout_hash("GRCh38")
        );
        assert_ne!(
            hash,
            RegionLayoutV1::new("1", 10_250, 1_001, 9_001, 101, 100, 2)
                .unwrap()
                .layout_hash("GRCh38")
        );
        assert_ne!(
            hash,
            RegionLayoutV1::new("1", 10_250, 1_001, 9_001, 51, 101, 2)
                .unwrap()
                .layout_hash("GRCh38")
        );
        assert_ne!(
            hash,
            RegionLayoutV1::new("1", 10_250, 1_001, 9_001, 51, 100, 1)
                .unwrap()
                .layout_hash("GRCh38")
        );
        assert_ne!(
            region_render_hash(&hash, "sha256:a", 5e-8),
            region_render_hash(&hash, "sha256:a", 1e-6)
        );
        assert_ne!(
            region_render_hash(&hash, "sha256:a", 5e-8),
            region_render_hash(&hash, "sha256:b", 5e-8)
        );
    }

    #[test]
    fn coding_is_only_the_documented_so_vocabulary() {
        assert!(coding_consequence_v1(Some("missense_variant")));
        assert!(coding_consequence_v1(Some("stop_gained")));
        for value in [
            None,
            Some("missense"),
            Some("pLoF"),
            Some("unknown"),
            Some("free text"),
        ] {
            assert!(!coding_consequence_v1(value));
        }
    }

    #[test]
    fn significance_is_strict_and_zero_is_included() {
        let layout = RegionLayoutV1::new("1", 100, 1, 100, 50, 64, 1).unwrap();
        let points = vec![
            point(1, 0.0, 400.0, None, 1),
            point(2, 5e-8, -5e-8_f64.log10(), None, 2),
            point(3, 4e-8, -4e-8_f64.log10(), None, 3),
        ];
        let overlay =
            reduce_region_overlay(&layout, points, &HashSet::new(), 5e-8, no_genes()).unwrap();
        assert_eq!(overlay.aggregation.protected_significant_count, 2);
        assert!(
            overlay
                .points
                .iter()
                .find(|entry| entry.point.source_ordinal == 2)
                .unwrap()
                .ordinary_representative
        );
    }

    #[test]
    fn reduction_is_batch_order_independent_and_discloses_identities() {
        let layout = RegionLayoutV1::new("1", 100, 1, 100, 50, 64, 1).unwrap();
        let points: Vec<_> = (0..1_500)
            .map(|ordinal| {
                let position = ordinal % 100 + 1;
                point(position, 0.5, 0.301, None, ordinal as u64)
            })
            .collect();
        let mut reverse = points.clone();
        reverse.reverse();
        let first =
            reduce_region_overlay(&layout, points, &HashSet::new(), 5e-8, no_genes()).unwrap();
        let second =
            reduce_region_overlay(&layout, reverse, &HashSet::new(), 5e-8, no_genes()).unwrap();
        assert_eq!(first.points, second.points);
        assert_eq!(first.aggregation, second.aggregation);
        assert_eq!(first.aggregation.input_point_count, 1_500);
        assert_eq!(first.aggregation.rasterized_point_count, 1_500);
        assert_eq!(
            first.aggregation.rendered_ordinary_count + first.aggregation.omitted_ordinary_count,
            1_500
        );
        assert_eq!(
            first.aggregation.overlay_point_count,
            first.points.len() as u64
        );
    }

    #[test]
    fn overlay_aggregate_text_is_hard_bounded() {
        let layout = RegionLayoutV1::new("1", 300, 1, 300, 50, 64, 1).unwrap();
        let mut selected = HashSet::new();
        let points = (0..257)
            .map(|index| {
                selected.insert(index);
                let mut value = point(index as i64 + 1, 0.5, 0.301, None, index);
                value.gene_symbol = Some("x".repeat(4_096));
                value
            })
            .collect::<Vec<_>>();
        assert!(matches!(
            reduce_region_overlay(&layout, points, &selected, 5e-8, no_genes()),
            Err(RegionContractError::OverlayTextLimit)
        ));
    }

    #[test]
    fn protected_overflow_is_typed_instead_of_truncated() {
        let layout = RegionLayoutV1::new("1", 2_000, 1, 2_000, 50, 64, 1).unwrap();
        let points = (0..=MAX_REGION_OVERLAY_POINTS)
            .map(|index| point(index as i64 + 1, 0.0, 400.0, None, index as u64));
        assert!(matches!(
            reduce_region_overlay(&layout, points, &HashSet::new(), 5e-8, no_genes()),
            Err(RegionContractError::ProtectedOverlayLimit {
                count: 1001,
                maximum: 1000
            })
        ));
    }

    #[test]
    fn gene_annotation_states_enforce_feature_bounds() {
        let layout = RegionLayoutV1::new("1", 100, 1, 100, 50, 64, 1).unwrap();
        let feature = GeneFeatureV1 {
            id: "g".into(),
            symbol: None,
            contig: "1".into(),
            start: 1,
            end: 100,
            strand: GeneStrandV1::Unknown,
        };
        assert!(GeneAnnotationsV1 {
            state: GeneAnnotationStateV1::Available,
            features: vec![],
            message: None,
        }
        .validate(&layout)
        .is_ok());
        assert!(GeneAnnotationsV1 {
            state: GeneAnnotationStateV1::Error,
            features: vec![],
            message: Some("reference unavailable".into()),
        }
        .validate(&layout)
        .is_ok());
        assert!(GeneAnnotationsV1 {
            state: GeneAnnotationStateV1::Unavailable,
            features: vec![feature.clone()],
            message: None,
        }
        .validate(&layout)
        .is_err());
        assert!(GeneAnnotationsV1 {
            state: GeneAnnotationStateV1::Available,
            features: vec![feature; MAX_GENE_FEATURES + 1],
            message: None,
        }
        .validate(&layout)
        .is_err());
    }

    #[test]
    fn genes_are_alias_normalized_clipped_ordered_and_bounded() {
        let request = GeneRegionRequestV1 {
            assembly: "hg38".into(),
            contig: "chr1".into(),
            start: 100,
            end: 200,
        };
        let genes = normalize_gene_features(
            &request,
            vec![
                GeneFeatureV1 {
                    id: "b".into(),
                    symbol: None,
                    contig: "1".into(),
                    start: 190,
                    end: 250,
                    strand: GeneStrandV1::Minus,
                },
                GeneFeatureV1 {
                    id: "a".into(),
                    symbol: Some("A".into()),
                    contig: "chr1".into(),
                    start: 50,
                    end: 100,
                    strand: GeneStrandV1::Plus,
                },
            ],
        )
        .unwrap();
        assert_eq!((genes[0].start, genes[0].end), (100, 100));
        assert_eq!((genes[1].start, genes[1].end), (190, 200));
        assert!(normalize_gene_features(
            &request,
            vec![GeneFeatureV1 {
                id: "x".into(),
                symbol: None,
                contig: "2".into(),
                start: 100,
                end: 110,
                strand: GeneStrandV1::Unknown
            }]
        )
        .is_err());
    }

    proptest! {
        #[test]
        fn quantized_bounds_enclose_every_valid_request(
            contig_length in 1_i64..=10_000_000,
            first in 1_i64..=10_000_000,
            second in 1_i64..=10_000_000,
        ) {
            let start = first.min(second).min(contig_length);
            let end = first.max(second).min(contig_length).max(start);
            let layout = RegionLayoutV1::new("1", contig_length, start, end, 50, 64, 1).unwrap();
            prop_assert!(layout.effective_start <= start);
            prop_assert!(layout.effective_end >= end);
            prop_assert!(layout.effective_end <= contig_length);
            prop_assert_eq!((layout.effective_start - 1) % REGION_QUANTUM_BASES_V1, 0);
        }
    }
}
