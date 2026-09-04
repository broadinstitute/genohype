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
use schemars::{schema_for, JsonSchema, Schema, SchemaGenerator};
use serde::{de, Deserialize, Deserializer, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt;

pub const ASSOCIATION_REGION_SCHEMA_VERSION_V1: &str = "1.0";
pub const REGION_QUANTUM_BASES_V1: i64 = 1_000;
pub const REGION_WIDTH_QUANTUM_V1: u32 = 50;
pub const MIN_REGION_WIDTH: u32 = 50;
/// Largest logical width whose 50-pixel upward quantization remains within
/// the documented 4,096-pixel logical resource ceiling.
pub const MAX_REGION_WIDTH: u32 = 4_050;
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
pub const LOCUS_STYLE_VERSION_V1: &str = "locus_style_v1";
pub const MAX_REGION_IDENTITY_TEXT_BYTES: usize = 4_096;

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
    #[schemars(range(min = 50, max = 4050_u32))]
    pub requested_width: u32,
    #[schemars(range(min = 50, max = 4050_u32))]
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
        let contig_input = contig.into();
        if contig_input.len() > MAX_REGION_IDENTITY_TEXT_BYTES {
            return Err(RegionContractError::InvalidContig);
        }
        let contig = canonical_contig(&contig_input).ok_or(RegionContractError::InvalidContig)?;
        if contig.len() > MAX_REGION_IDENTITY_TEXT_BYTES {
            return Err(RegionContractError::InvalidContig);
        }
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
        let protected_category_sum = self
            .protected_selected_count
            .checked_add(self.protected_significant_count)
            .and_then(|value| value.checked_add(self.protected_coding_count))
            .ok_or(RegionContractError::InconsistentCounts)?;
        if values
            .into_iter()
            .any(|value| value > JSON_SAFE_INTEGER_MAX)
            || self.rule_id != REGION_DENSE_RULE_V1
            || self.protected_selected_count > self.protected_union_count
            || self.protected_significant_count > self.protected_union_count
            || self.protected_coding_count > self.protected_union_count
            || self.protected_union_count > protected_category_sum
            || self.protected_union_count > MAX_REGION_OVERLAY_POINTS as u64
            || self.protected_union_count + self.ordinary_input_count != self.input_point_count
            || self.ordinary_budget != MAX_REGION_OVERLAY_POINTS as u64 - self.protected_union_count
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

/// Checked identity inputs shared by dense raster and semantic overlay creation.
/// The effective raster hash intentionally excludes unquantized request bounds
/// and requested width so requests with identical work coalesce.
#[derive(Debug, Clone, PartialEq)]
pub struct RegionRenderIdentityV1 {
    dataset_id: DatasetIdV1,
    source_fingerprint: String,
    assembly: String,
    background_id: String,
    layout: RegionLayoutV1,
    significance_threshold: f64,
    layout_hash: String,
    raster_hash: String,
    render_hash: String,
}

impl RegionRenderIdentityV1 {
    pub fn new(
        dataset_id: DatasetIdV1,
        source_fingerprint: impl Into<String>,
        assembly: impl Into<String>,
        background_id: impl Into<String>,
        layout: RegionLayoutV1,
        significance_threshold: f64,
    ) -> Result<Self, RegionContractError> {
        layout.validate()?;
        validate_threshold(significance_threshold)?;
        let source_fingerprint = source_fingerprint.into();
        let assembly_input = assembly.into();
        let background_id = background_id.into();
        if source_fingerprint.trim().is_empty()
            || source_fingerprint.len() > 128
            || assembly_input.len() > 64
            || background_id.len() > 64
        {
            return Err(RegionContractError::InvalidMetadata);
        }
        let assembly = canonical_assembly(&assembly_input);
        if assembly.is_empty() || assembly.len() > 64 || assembly != assembly_input.trim() {
            return Err(RegionContractError::InvalidMetadata);
        }
        validate_background_id(&background_id)?;
        let layout_hash = layout.layout_hash(&assembly);
        let raster_hash = region_raster_hash(
            &dataset_id,
            &source_fingerprint,
            &assembly,
            &layout,
            significance_threshold,
            &background_id,
        );
        let render_hash = region_render_hash(&layout_hash, &raster_hash);
        Ok(Self {
            dataset_id,
            source_fingerprint,
            assembly,
            background_id,
            layout,
            significance_threshold,
            layout_hash,
            raster_hash,
            render_hash,
        })
    }

    pub fn layout(&self) -> &RegionLayoutV1 {
        &self.layout
    }
    pub fn significance_threshold(&self) -> f64 {
        self.significance_threshold
    }
    pub fn layout_hash(&self) -> &str {
        &self.layout_hash
    }
    pub fn raster_hash(&self) -> &str {
        &self.raster_hash
    }
    pub fn render_hash(&self) -> &str {
        &self.render_hash
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
    #[schemars(length(min = 1, max = 64))]
    pub background_id: String,
    #[schemars(length(min = 71, max = 71), regex(pattern = "^sha256:[0-9a-f]{64}$"))]
    pub raster_hash: String,
    #[schemars(length(min = 71, max = 71), regex(pattern = "^sha256:[0-9a-f]{64}$"))]
    pub render_hash: String,
    #[schemars(length(min = 71, max = 71), regex(pattern = "^sha256:[0-9a-f]{64}$"))]
    pub overlay_hash: String,
    #[schemars(schema_with = "significance_threshold_schema")]
    pub significance_threshold: f64,
    pub counts: AssociationRegionCountsV1,
    pub aggregation: RegionAggregationDisclosureV1,
    pub resources: AssociationRegionResourcesV1,
}

impl AssociationRegionRenderManifestV1 {
    pub fn expected_raster_hash(&self) -> String {
        region_raster_hash(
            &self.dataset_id,
            &self.source_fingerprint,
            &self.assembly,
            &self.layout,
            self.significance_threshold,
            &self.background_id,
        )
    }

    pub fn expected_render_hash(&self) -> String {
        region_render_hash(&self.layout_hash, &self.raster_hash)
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
            || canonical_assembly(&self.assembly) != self.assembly
        {
            return Err(RegionContractError::InvalidMetadata);
        }
        validate_background_id(&self.background_id)?;
        validate_threshold(self.significance_threshold)?;
        self.layout.validate()?;
        let expected_layout = self.layout.layout_hash(&self.assembly);
        if self.layout_hash != expected_layout {
            return Err(RegionContractError::LayoutHashMismatch);
        }
        if self.raster_hash != self.expected_raster_hash() {
            return Err(RegionContractError::RasterHashMismatch);
        }
        if self.render_hash != self.expected_render_hash() || !is_sha256_digest(&self.overlay_hash)
        {
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

pub fn region_raster_hash(
    dataset_id: &DatasetIdV1,
    source_fingerprint: &str,
    assembly: &str,
    layout: &RegionLayoutV1,
    threshold: f64,
    background_id: &str,
) -> String {
    let mut digest = Sha256::new();
    digest.update(b"genohype.association.region-raster.v1\0");
    update_string(&mut digest, dataset_id.as_str());
    update_string(&mut digest, source_fingerprint);
    update_string(&mut digest, assembly);
    update_string(&mut digest, &layout.contig);
    for value in [
        layout.contig_length,
        layout.effective_start,
        layout.effective_end,
    ] {
        digest.update(value.to_be_bytes());
    }
    for value in [
        layout.width,
        layout.height,
        layout.physical_width,
        layout.physical_height,
    ] {
        digest.update(value.to_be_bytes());
    }
    digest.update([layout.dpr]);
    digest.update(layout.y_scale.log_threshold.to_bits().to_be_bytes());
    digest.update(layout.y_scale.linear_fraction.to_bits().to_be_bytes());
    digest.update(layout.y_scale.max_neg_log_p.to_bits().to_be_bytes());
    digest.update(threshold.to_bits().to_be_bytes());
    update_string(&mut digest, LOCUS_RENDERER_VERSION_V1);
    update_string(&mut digest, LOCUS_STYLE_VERSION_V1);
    update_string(&mut digest, background_id);
    format!("sha256:{:x}", digest.finalize())
}

/// Exact request/render identity retained separately from the coalescing raster key.
pub fn region_render_hash(layout_hash: &str, raster_hash: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(b"genohype.association.region-render.v1\0");
    update_string(&mut digest, layout_hash);
    update_string(&mut digest, raster_hash);
    format!("sha256:{:x}", digest.finalize())
}

pub fn region_overlay_hash(
    raster_hash: &str,
    selected_ordinals: &[u64],
    gene_annotations: &GeneAnnotationsV1,
) -> String {
    let mut digest = Sha256::new();
    digest.update(b"genohype.association.region-overlay.v1\0");
    update_string(&mut digest, raster_hash);
    update_string(&mut digest, REGION_DENSE_RULE_V1);
    digest.update((selected_ordinals.len() as u64).to_be_bytes());
    for ordinal in selected_ordinals {
        digest.update(ordinal.to_be_bytes());
    }
    update_string(
        &mut digest,
        match gene_annotations.state {
            GeneAnnotationStateV1::Unavailable => "unavailable",
            GeneAnnotationStateV1::Available => "available",
            GeneAnnotationStateV1::Error => "error",
        },
    );
    match &gene_annotations.message {
        Some(message) => {
            digest.update([1]);
            update_string(&mut digest, message);
        }
        None => digest.update([0]),
    }
    digest.update((gene_annotations.features.len() as u64).to_be_bytes());
    for feature in &gene_annotations.features {
        update_string(&mut digest, &feature.id);
        match &feature.symbol {
            Some(symbol) => {
                digest.update([1]);
                update_string(&mut digest, symbol);
            }
            None => digest.update([0]),
        }
        update_string(&mut digest, &feature.contig);
        digest.update(feature.start.to_be_bytes());
        digest.update(feature.end.to_be_bytes());
        update_string(
            &mut digest,
            match feature.strand {
                GeneStrandV1::Plus => "plus",
                GeneStrandV1::Minus => "minus",
                GeneStrandV1::Unknown => "unknown",
            },
        );
    }
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

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
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
        if self.contig.len() > MAX_REGION_IDENTITY_TEXT_BYTES {
            return Err(GeneReferenceError::InvalidFeature);
        }
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
            if previous.is_some_and(|prior| !gene_cmp(prior, feature).is_lt()) {
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
    pub raster_hash: String,
    #[schemars(length(min = 71, max = 71), regex(pattern = "^sha256:[0-9a-f]{64}$"))]
    pub render_hash: String,
    #[schemars(length(min = 71, max = 71), regex(pattern = "^sha256:[0-9a-f]{64}$"))]
    pub overlay_hash: String,
    #[schemars(length(max = 1000), inner(range(max = 9007199254740991_u64)))]
    pub selected_ordinals: Vec<u64>,
    pub layout: RegionLayoutV1,
    #[schemars(schema_with = "significance_threshold_schema")]
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
            || !is_sha256_digest(&self.layout_hash)
            || !is_sha256_digest(&self.raster_hash)
            || self.render_hash != region_render_hash(&self.layout_hash, &self.raster_hash)
        {
            return Err(RegionContractError::RenderHashMismatch);
        }
        if self.selected_ordinals.len() > MAX_REGION_OVERLAY_POINTS {
            return Err(RegionContractError::InvalidOverlayPoint);
        }
        let mut selected_ordinals = HashSet::with_capacity(self.selected_ordinals.len());
        let mut previous_selected = None;
        for ordinal in &self.selected_ordinals {
            if *ordinal > JSON_SAFE_INTEGER_MAX
                || previous_selected.is_some_and(|previous| previous >= *ordinal)
                || !selected_ordinals.insert(*ordinal)
            {
                return Err(RegionContractError::InvalidOverlayPoint);
            }
            previous_selected = Some(*ordinal);
        }

        let mut point_text_bytes = 0_usize;
        let mut source_ordinals = HashSet::with_capacity(self.points.len());
        let mut previous: Option<&AssociationPointV1> = None;
        let mut selected_count = 0_u64;
        let mut significant_count = 0_u64;
        let mut coding_count = 0_u64;
        let mut protected_union_count = 0_u64;
        let mut rendered_ordinary_count = 0_u64;
        for entry in &self.points {
            let (point, _) = entry
                .point
                .clone()
                .normalize()
                .map_err(|_| RegionContractError::InvalidOverlayPoint)?;
            point_text_bytes = point_text_bytes.saturating_add(point_text_len(&point));
            let protected = entry.selected || entry.significant || entry.coding;
            if !source_ordinals.insert(point.source_ordinal)
                || previous.is_some_and(|prior| compare_association_points(prior, &point).is_gt())
                || point != entry.point
                || point.contig != self.layout.contig
                || self.layout.x_for(point.position).is_none()
                || entry.selected != selected_ordinals.contains(&point.source_ordinal)
                || entry.significant != is_significant(&point, self.significance_threshold)
                || entry.coding != coding_consequence_v1(point.consequence.as_deref())
                || entry.ordinary_representative == protected
            {
                return Err(RegionContractError::InvalidOverlayPoint);
            }
            selected_count += u64::from(entry.selected);
            significant_count += u64::from(entry.significant);
            coding_count += u64::from(entry.coding);
            protected_union_count += u64::from(protected);
            rendered_ordinary_count += u64::from(entry.ordinary_representative);
            previous = Some(&entry.point);
        }
        let ordinary_input_count = self
            .aggregation
            .input_point_count
            .checked_sub(protected_union_count)
            .ok_or(RegionContractError::InconsistentCounts)?;
        let omitted_ordinary_count = ordinary_input_count
            .checked_sub(rendered_ordinary_count)
            .ok_or(RegionContractError::InconsistentCounts)?;
        let expected_disclosure = RegionAggregationDisclosureV1 {
            rule_id: REGION_DENSE_RULE_V1.into(),
            input_point_count: self.aggregation.input_point_count,
            protected_selected_count: selected_count,
            protected_significant_count: significant_count,
            protected_coding_count: coding_count,
            protected_union_count,
            ordinary_input_count,
            ordinary_budget: MAX_REGION_OVERLAY_POINTS as u64 - protected_union_count,
            rendered_ordinary_count,
            omitted_ordinary_count,
            rasterized_point_count: self.aggregation.input_point_count,
            overlay_point_count: self.points.len() as u64,
        };
        if self.aggregation != expected_disclosure {
            return Err(RegionContractError::InconsistentCounts);
        }
        if point_text_bytes > MAX_REGION_OVERLAY_TEXT_BYTES {
            return Err(RegionContractError::OverlayTextLimit);
        }
        self.gene_annotations.validate(&self.layout)?;
        if self.overlay_hash
            != region_overlay_hash(
                &self.raster_hash,
                &self.selected_ordinals,
                &self.gene_annotations,
            )
        {
            return Err(RegionContractError::OverlayHashMismatch);
        }
        Ok(())
    }

    pub fn validate_against(
        &self,
        manifest: &AssociationRegionRenderManifestV1,
    ) -> Result<(), RegionContractError> {
        manifest.validate()?;
        self.validate()?;
        if self.layout != manifest.layout
            || self.significance_threshold.to_bits() != manifest.significance_threshold.to_bits()
            || self.layout_hash != manifest.layout_hash
            || self.raster_hash != manifest.raster_hash
            || self.render_hash != manifest.render_hash
            || self.overlay_hash != manifest.overlay_hash
            || self.aggregation != manifest.aggregation
        {
            return Err(RegionContractError::ManifestOverlayMismatch);
        }
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

/// Bounded, association-owned accumulator for a streamed region scan.
///
/// The reducer retains every protected point (selected, significant, or coding)
/// up to the public overlay limit and at most one ordinary representative per
/// V1 pixel stratum. Its memory use is therefore independent of input size.
/// Canonical scan points may be pushed one at a time in any order.
#[derive(Debug)]
pub struct RegionOverlayReducerV1 {
    layout: RegionLayoutV1,
    significance_threshold: f64,
    selected_ordinals: Vec<u64>,
    protected: Vec<AssociationRegionOverlayPointV1>,
    ordinary_buckets: HashMap<(u32, u8), AssociationPointV1>,
    input_count: u64,
    selected_count: u64,
    significant_count: u64,
    coding_count: u64,
    ordinary_count: u64,
}

impl RegionOverlayReducerV1 {
    /// Create a reducer for one checked layout, threshold, and selection set.
    pub fn new(
        layout: &RegionLayoutV1,
        significance_threshold: f64,
        selected_ordinals: &HashSet<u64>,
    ) -> Result<Self, RegionContractError> {
        layout.validate()?;
        validate_threshold(significance_threshold)?;
        if selected_ordinals.len() > MAX_REGION_OVERLAY_POINTS
            || selected_ordinals
                .iter()
                .any(|ordinal| *ordinal > JSON_SAFE_INTEGER_MAX)
        {
            return Err(RegionContractError::InvalidOverlayPoint);
        }
        let mut selected_ordinals: Vec<_> = selected_ordinals.iter().copied().collect();
        selected_ordinals.sort_unstable();
        Ok(Self {
            layout: layout.clone(),
            significance_threshold,
            selected_ordinals,
            protected: Vec::new(),
            ordinary_buckets: HashMap::new(),
            input_count: 0,
            selected_count: 0,
            significant_count: 0,
            coding_count: 0,
            ordinary_count: 0,
        })
    }

    /// Consume one point from a canonical region scan.
    ///
    /// Errors are returned directly so this method can be used as a
    /// `AssociationStore::scan_region` sink. The accumulator must be discarded
    /// after a push error.
    pub fn push(&mut self, point: &AssociationPointV1) -> Result<(), RegionContractError> {
        let (point, _) = point
            .clone()
            .normalize()
            .map_err(|_| RegionContractError::InvalidOverlayPoint)?;
        let x = self
            .layout
            .x_for(point.position)
            .filter(|_| point.contig == self.layout.contig)
            .ok_or(RegionContractError::InvalidOverlayPoint)?;
        let selected = self
            .selected_ordinals
            .binary_search(&point.source_ordinal)
            .is_ok();
        let significant = is_significant(&point, self.significance_threshold);
        let coding = coding_consequence_v1(point.consequence.as_deref());
        let is_protected = selected || significant || coding;

        if is_protected && self.protected.len() == MAX_REGION_OVERLAY_POINTS {
            return Err(RegionContractError::ProtectedOverlayLimit {
                count: MAX_REGION_OVERLAY_POINTS + 1,
                maximum: MAX_REGION_OVERLAY_POINTS,
            });
        }

        let input_count = self
            .input_count
            .checked_add(1)
            .ok_or(RegionContractError::ArithmeticOverflow)?;
        let selected_count = self
            .selected_count
            .checked_add(u64::from(selected))
            .ok_or(RegionContractError::ArithmeticOverflow)?;
        let significant_count = self
            .significant_count
            .checked_add(u64::from(significant))
            .ok_or(RegionContractError::ArithmeticOverflow)?;
        let coding_count = self
            .coding_count
            .checked_add(u64::from(coding))
            .ok_or(RegionContractError::ArithmeticOverflow)?;
        let ordinary_count = self
            .ordinary_count
            .checked_add(u64::from(!is_protected))
            .ok_or(RegionContractError::ArithmeticOverflow)?;

        if is_protected {
            self.protected.push(AssociationRegionOverlayPointV1 {
                point,
                selected,
                significant,
                coding,
                ordinary_representative: false,
            });
        } else {
            let x_bucket_count = self.layout.physical_width.min(REGION_X_STRATA_V1);
            let x_bucket = ((f64::from(x) * f64::from(x_bucket_count)
                / f64::from(self.layout.physical_width))
            .floor() as u32)
                .min(x_bucket_count - 1);
            let clipped = point
                .neg_log10_p
                .clamp(0.0, self.layout.y_scale.max_neg_log_p);
            let stratum = ((clipped / self.layout.y_scale.max_neg_log_p
                * f64::from(REGION_Y_STRATA_V1))
            .floor() as u8)
                .min(REGION_Y_STRATA_V1 - 1);
            self.ordinary_buckets
                .entry((x_bucket, stratum))
                .and_modify(|current| {
                    if compare_association_points(&point, current).is_lt() {
                        *current = point.clone();
                    }
                })
                .or_insert(point);
        }

        self.input_count = input_count;
        self.selected_count = selected_count;
        self.significant_count = significant_count;
        self.coding_count = coding_count;
        self.ordinary_count = ordinary_count;
        Ok(())
    }

    /// Finish the overlay against the exact render identity used by the raster.
    /// Available gene features are normalized, clipped, and sorted here.
    pub fn finish(
        mut self,
        identity: &RegionRenderIdentityV1,
        mut gene_annotations: GeneAnnotationsV1,
    ) -> Result<AssociationRegionOverlayV1, RegionContractError> {
        if identity.layout() != &self.layout
            || identity.significance_threshold().to_bits() != self.significance_threshold.to_bits()
        {
            return Err(RegionContractError::ReducerIdentityMismatch);
        }

        if gene_annotations.state == GeneAnnotationStateV1::Available {
            let request = GeneRegionRequestV1 {
                assembly: identity.assembly.clone(),
                contig: self.layout.contig.clone(),
                start: self.layout.effective_start,
                end: self.layout.effective_end,
            };
            gene_annotations.features =
                normalize_gene_features(&request, gene_annotations.features)
                    .map_err(|_| RegionContractError::InvalidGenes)?;
        }
        gene_annotations.validate(&self.layout)?;

        let ordinary_budget = MAX_REGION_OVERLAY_POINTS - self.protected.len();
        let mut representatives: Vec<_> = self.ordinary_buckets.into_values().collect();
        representatives.sort_by(compare_association_points);
        representatives.truncate(ordinary_budget);
        let rendered_ordinary = representatives.len() as u64;
        self.protected
            .extend(
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
        self.protected
            .sort_by(|left, right| compare_association_points(&left.point, &right.point));

        let protected_union_count = self
            .input_count
            .checked_sub(self.ordinary_count)
            .ok_or(RegionContractError::InconsistentCounts)?;
        let omitted_ordinary_count = self
            .ordinary_count
            .checked_sub(rendered_ordinary)
            .ok_or(RegionContractError::InconsistentCounts)?;
        let disclosure = RegionAggregationDisclosureV1 {
            rule_id: REGION_DENSE_RULE_V1.into(),
            input_point_count: self.input_count,
            protected_selected_count: self.selected_count,
            protected_significant_count: self.significant_count,
            protected_coding_count: self.coding_count,
            protected_union_count,
            ordinary_input_count: self.ordinary_count,
            ordinary_budget: ordinary_budget as u64,
            rendered_ordinary_count: rendered_ordinary,
            omitted_ordinary_count,
            rasterized_point_count: self.input_count,
            overlay_point_count: self.protected.len() as u64,
        };
        disclosure.validate()?;
        if self
            .protected
            .iter()
            .map(|entry| point_text_len(&entry.point))
            .sum::<usize>()
            > MAX_REGION_OVERLAY_TEXT_BYTES
        {
            return Err(RegionContractError::OverlayTextLimit);
        }

        let overlay_hash = region_overlay_hash(
            identity.raster_hash(),
            &self.selected_ordinals,
            &gene_annotations,
        );
        let overlay = AssociationRegionOverlayV1 {
            schema_version: ASSOCIATION_REGION_SCHEMA_VERSION_V1.into(),
            layout_hash: identity.layout_hash().into(),
            raster_hash: identity.raster_hash().into(),
            render_hash: identity.render_hash().into(),
            overlay_hash,
            selected_ordinals: self.selected_ordinals,
            layout: self.layout,
            significance_threshold: self.significance_threshold,
            points: self.protected,
            gene_annotations,
            aggregation: disclosure,
        };
        overlay.validate()?;
        Ok(overlay)
    }
}

/// Build a bounded deterministic semantic overlay. Input iteration order and
/// batch boundaries do not affect the result.
pub fn reduce_region_overlay<I>(
    identity: &RegionRenderIdentityV1,
    points: I,
    selected_ordinals: &HashSet<u64>,
    gene_annotations: GeneAnnotationsV1,
) -> Result<AssociationRegionOverlayV1, RegionContractError>
where
    I: IntoIterator<Item = AssociationPointV1>,
{
    let mut reducer = RegionOverlayReducerV1::new(
        identity.layout(),
        identity.significance_threshold(),
        selected_ordinals,
    )?;
    for point in points {
        reducer.push(&point)?;
    }
    reducer.finish(identity, gene_annotations)
}

pub fn normalize_gene_features(
    request: &GeneRegionRequestV1,
    features: Vec<GeneFeatureV1>,
) -> Result<Vec<GeneFeatureV1>, GeneReferenceError> {
    if features.len() > MAX_GENE_FEATURES {
        return Err(GeneReferenceError::LimitExceeded);
    }
    if request.assembly.len() > 64 || request.contig.len() > MAX_REGION_IDENTITY_TEXT_BYTES {
        return Err(GeneReferenceError::InvalidFeature);
    }
    let canonical = canonical_contig(&request.contig).ok_or(GeneReferenceError::InvalidFeature)?;
    let assembly = canonical_assembly(&request.assembly);
    if assembly.is_empty() || assembly.len() > 64 {
        return Err(GeneReferenceError::InvalidFeature);
    }
    let request = GeneRegionRequestV1 {
        assembly,
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
    if normalized
        .windows(2)
        .any(|pair| gene_cmp(&pair[0], &pair[1]).is_eq())
    {
        return Err(GeneReferenceError::InvalidFeature);
    }
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

fn validate_background_id(value: &str) -> Result<(), RegionContractError> {
    let valid_color = value.strip_prefix("color:#").is_some_and(|hex| {
        hex.len() == 6
            && hex
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'A'..=b'F'))
    });
    if matches!(value, "white" | "transparent") || valid_color {
        Ok(())
    } else {
        Err(RegionContractError::InvalidBackground)
    }
}

fn is_sha256_digest(value: &str) -> bool {
    value.len() == 71
        && value.starts_with("sha256:")
        && value[7..]
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn significance_threshold_schema(generator: &mut SchemaGenerator) -> Schema {
    let mut schema = f64::json_schema(generator);
    schema.insert("exclusiveMinimum".into(), 0.0.into());
    schema.insert("maximum".into(), 1.0.into());
    schema
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
        .then_with(|| left.contig.cmp(&right.contig))
        .then_with(|| left.strand.cmp(&right.strand))
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
    InvalidBackground,
    LayoutHashMismatch,
    RasterHashMismatch,
    RenderHashMismatch,
    OverlayHashMismatch,
    ManifestOverlayMismatch,
    InconsistentCounts,
    InvalidResourceMediaType,
    InvalidOverlayPoint,
    ReducerIdentityMismatch,
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

    fn identity(layout: &RegionLayoutV1, threshold: f64) -> RegionRenderIdentityV1 {
        RegionRenderIdentityV1::new(
            DatasetIdV1::new("123e4567-e89b-42d3-a456-426614174000").unwrap(),
            "sha256:test",
            "GRCh38",
            "white",
            layout.clone(),
            threshold,
        )
        .unwrap()
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
        let value = RegionLayoutV1::new("1", 10_250, 10_001, 10_250, 4_050, 64, 1).unwrap();
        assert_eq!(value.effective_end, 10_250);
        assert_eq!(value.requested_width, 4_050);
        assert_eq!(value.width, 4_050);
        assert_eq!(value.physical_width, 4_050);
        assert!(RegionLayoutV1::new("1", 10_250, 10_001, 10_250, 4_050, 64, 4).is_ok());
        assert_eq!(
            RegionLayoutV1::new("1", 1_000_000, 1, 1_000_000, 4_050, 2_048, 4).unwrap_err(),
            RegionContractError::RasterDimensions(LayoutError::DimensionsLimit)
        );
        for width in [0, 49, 4_051, 4_096, u32::MAX] {
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
        let first = identity(&base, 5e-8);
        let changed_threshold = identity(&base, 1e-6);
        assert_ne!(first.raster_hash(), changed_threshold.raster_hash());
        let equivalent = RegionLayoutV1::new("1", 10_250, 1_002, 9_999, 99, 100, 2).unwrap();
        let equivalent_identity = identity(&equivalent, 5e-8);
        assert_ne!(first.layout_hash(), equivalent_identity.layout_hash());
        assert_eq!(first.raster_hash(), equivalent_identity.raster_hash());
        assert_ne!(first.render_hash(), equivalent_identity.render_hash());
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
        let overlay = reduce_region_overlay(
            &identity(&layout, 5e-8),
            points,
            &HashSet::new(),
            no_genes(),
        )
        .unwrap();
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
        let identity = identity(&layout, 5e-8);
        let first = reduce_region_overlay(&identity, points, &HashSet::new(), no_genes()).unwrap();
        let second =
            reduce_region_overlay(&identity, reverse, &HashSet::new(), no_genes()).unwrap();
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
    fn incremental_reducer_matches_convenience_across_chunks_and_permutations() {
        let layout = RegionLayoutV1::new("1", 100, 1, 100, 50, 64, 1).unwrap();
        let identity = identity(&layout, 5e-8);
        let selected = HashSet::from([19, 1_499]);
        let points: Vec<_> = (0..1_500)
            .map(|ordinal| {
                let position = ordinal % 100 + 1;
                if ordinal % 307 == 0 {
                    point(position, 0.0, 400.0, None, ordinal as u64)
                } else if ordinal % 211 == 0 {
                    point(
                        position,
                        0.5,
                        0.301,
                        Some("missense_variant"),
                        ordinal as u64,
                    )
                } else {
                    point(position, 0.5, 0.301, None, ordinal as u64)
                }
            })
            .collect();
        let expected =
            reduce_region_overlay(&identity, points.clone(), &selected, no_genes()).unwrap();
        let expected_bytes = serde_json::to_vec(&expected).unwrap();

        let mut reversed = points.clone();
        reversed.reverse();
        let mut rotated = points.clone();
        rotated.rotate_left(617);
        for permutation in [points, reversed, rotated] {
            for chunk_size in [1, 7, 333] {
                let mut reducer = RegionOverlayReducerV1::new(&layout, 5e-8, &selected).unwrap();
                for chunk in permutation.chunks(chunk_size) {
                    for point in chunk {
                        reducer.push(point).unwrap();
                    }
                }
                let actual = reducer.finish(&identity, no_genes()).unwrap();
                assert_eq!(serde_json::to_vec(&actual).unwrap(), expected_bytes);
            }
        }
    }

    #[test]
    fn incremental_constructor_push_and_finish_failures_are_typed() {
        let layout = RegionLayoutV1::new("1", 100, 1, 100, 50, 64, 1).unwrap();
        assert!(matches!(
            RegionOverlayReducerV1::new(&layout, 0.0, &HashSet::new()),
            Err(RegionContractError::InvalidThreshold)
        ));
        let mut noncanonical = layout.clone();
        noncanonical.physical_width += 1;
        assert!(matches!(
            RegionOverlayReducerV1::new(&noncanonical, 5e-8, &HashSet::new()),
            Err(RegionContractError::NonCanonicalLayout)
        ));

        let mut reducer = RegionOverlayReducerV1::new(&layout, 5e-8, &HashSet::new()).unwrap();
        let mut wrong_contig = point(1, 0.5, 0.301, None, 1);
        wrong_contig.contig = "2".into();
        let mut sink = |point: &AssociationPointV1| {
            reducer
                .push(point)
                .map_err(|error| crate::AssociationStoreError::Sink(error.to_string()))
        };
        assert!(matches!(
            sink(&wrong_contig),
            Err(crate::AssociationStoreError::Sink(_))
        ));

        let reducer = RegionOverlayReducerV1::new(&layout, 5e-8, &HashSet::new()).unwrap();
        let other_identity = identity(&layout, 1e-6);
        assert!(matches!(
            reducer.finish(&other_identity, no_genes()),
            Err(RegionContractError::ReducerIdentityMismatch)
        ));
    }

    #[test]
    fn finish_normalizes_genes_and_produces_a_valid_manifest_pair() {
        let layout = RegionLayoutV1::new("1", 100, 1, 100, 50, 64, 1).unwrap();
        let identity = identity(&layout, 5e-8);
        let mut reducer = RegionOverlayReducerV1::new(&layout, 5e-8, &HashSet::new()).unwrap();
        reducer.push(&point(1, 0.5, 0.301, None, 1)).unwrap();
        let overlay = reducer
            .finish(
                &identity,
                GeneAnnotationsV1 {
                    state: GeneAnnotationStateV1::Available,
                    features: vec![
                        GeneFeatureV1 {
                            id: "b".into(),
                            symbol: None,
                            contig: "chr1".into(),
                            start: 90,
                            end: 120,
                            strand: GeneStrandV1::Minus,
                        },
                        GeneFeatureV1 {
                            id: "a".into(),
                            symbol: Some("A".into()),
                            contig: "chr1".into(),
                            start: 1,
                            end: 10,
                            strand: GeneStrandV1::Plus,
                        },
                    ],
                    message: None,
                },
            )
            .unwrap();
        assert_eq!(overlay.gene_annotations.features[0].id, "a");
        assert_eq!(overlay.gene_annotations.features[0].start, 1);
        assert_eq!(overlay.gene_annotations.features[1].end, 100);

        let manifest = AssociationRegionRenderManifestV1 {
            schema_version: ASSOCIATION_REGION_SCHEMA_VERSION_V1.into(),
            dataset_id: identity.dataset_id.clone(),
            title: "Incremental region".into(),
            assembly: identity.assembly.clone(),
            coordinate_convention: COORDINATE_CONVENTION_V1.into(),
            source_fingerprint: identity.source_fingerprint.clone(),
            layout: layout.clone(),
            layout_hash: identity.layout_hash().into(),
            background_id: identity.background_id.clone(),
            raster_hash: identity.raster_hash().into(),
            render_hash: identity.render_hash().into(),
            overlay_hash: overlay.overlay_hash.clone(),
            significance_threshold: identity.significance_threshold(),
            counts: AssociationRegionCountsV1 {
                source_point_count: overlay.aggregation.input_point_count,
                rasterized_point_count: overlay.aggregation.rasterized_point_count,
                significant_point_count: overlay.aggregation.protected_significant_count,
                coding_point_count: overlay.aggregation.protected_coding_count,
                selected_point_count: overlay.aggregation.protected_selected_count,
                overlay_point_count: overlay.aggregation.overlay_point_count,
            },
            aggregation: overlay.aggregation.clone(),
            resources: AssociationRegionResourcesV1 {
                raster: ResourceDescriptorV1 {
                    url: "region.png".into(),
                    media_type: "image/png".into(),
                    byte_size: None,
                    checksum: None,
                    expires_at: None,
                },
                overlay: ResourceDescriptorV1 {
                    url: "region.json".into(),
                    media_type: "application/json".into(),
                    byte_size: None,
                    checksum: None,
                    expires_at: None,
                },
            },
        };
        overlay.validate_against(&manifest).unwrap();
    }

    #[test]
    fn million_row_incremental_reduction_keeps_only_bounded_state() {
        let layout = RegionLayoutV1::new("1", 1_000_000, 1, 1_000_000, 4_050, 64, 1).unwrap();
        let identity = identity(&layout, 5e-8);
        let mut reducer = RegionOverlayReducerV1::new(&layout, 5e-8, &HashSet::new()).unwrap();
        for ordinal in 0..1_000_000_u64 {
            reducer
                .push(&point(ordinal as i64 + 1, 0.5, 0.301, None, ordinal))
                .unwrap();
        }
        assert!(reducer.protected.is_empty());
        assert!(
            reducer.ordinary_buckets.len()
                <= REGION_X_STRATA_V1 as usize * REGION_Y_STRATA_V1 as usize
        );
        let overlay = reducer.finish(&identity, no_genes()).unwrap();
        assert_eq!(overlay.aggregation.input_point_count, 1_000_000);
        assert!(overlay.points.len() <= MAX_REGION_OVERLAY_POINTS);
        overlay.validate().unwrap();
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
            reduce_region_overlay(&identity(&layout, 5e-8), points, &selected, no_genes()),
            Err(RegionContractError::OverlayTextLimit)
        ));
    }

    #[test]
    fn protected_overflow_is_typed_instead_of_truncated() {
        let layout = RegionLayoutV1::new("1", 2_000, 1, 2_000, 50, 64, 1).unwrap();
        let points: Vec<_> = (0..=MAX_REGION_OVERLAY_POINTS)
            .map(|index| point(index as i64 + 1, 0.0, 400.0, None, index as u64))
            .collect();
        assert!(matches!(
            reduce_region_overlay(
                &identity(&layout, 5e-8),
                points.clone(),
                &HashSet::new(),
                no_genes()
            ),
            Err(RegionContractError::ProtectedOverlayLimit {
                count: 1001,
                maximum: 1000
            })
        ));

        let mut reducer = RegionOverlayReducerV1::new(&layout, 5e-8, &HashSet::new()).unwrap();
        for point in points.into_iter().take(MAX_REGION_OVERLAY_POINTS) {
            reducer.push(&point).unwrap();
        }
        assert!(matches!(
            reducer.push(&point(1_001, 0.0, 400.0, None, 1_000)),
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

    #[test]
    fn reducer_output_is_valid_and_disclosure_flags_are_authoritative() {
        let layout = RegionLayoutV1::new("1", 100, 1, 100, 50, 64, 1).unwrap();
        let selected = HashSet::from([3]);
        let overlay = reduce_region_overlay(
            &identity(&layout, 5e-8),
            vec![
                point(1, 0.0, 400.0, Some("stop_gained"), 1),
                point(2, 0.5, 0.301, None, 2),
                point(3, 0.5, 0.301, None, 3),
            ],
            &selected,
            no_genes(),
        )
        .unwrap();
        overlay.validate().unwrap();
        for field in [
            "protected_selected_count",
            "protected_significant_count",
            "protected_coding_count",
            "protected_union_count",
            "ordinary_input_count",
            "ordinary_budget",
            "rendered_ordinary_count",
            "omitted_ordinary_count",
            "rasterized_point_count",
            "overlay_point_count",
            "input_point_count",
        ] {
            let mut value = serde_json::to_value(&overlay).unwrap();
            value["aggregation"][field] =
                serde_json::json!(value["aggregation"][field].as_u64().unwrap() + 1);
            let candidate: AssociationRegionOverlayV1 = serde_json::from_value(value).unwrap();
            assert!(candidate.validate().is_err(), "accepted altered {field}");
        }
        for field in [
            "selected",
            "significant",
            "coding",
            "ordinary_representative",
        ] {
            let mut value = serde_json::to_value(&overlay).unwrap();
            let current = value["points"][0][field].as_bool().unwrap();
            value["points"][0][field] = serde_json::json!(!current);
            let candidate: AssociationRegionOverlayV1 = serde_json::from_value(value).unwrap();
            assert!(candidate.validate().is_err(), "accepted altered {field}");
        }
    }

    #[test]
    fn identities_cover_resources_and_overlay_state() {
        let layout = layout();
        let base = identity(&layout, 5e-8);
        let make = |dataset: &str,
                    source: &str,
                    assembly: &str,
                    background: &str,
                    layout: RegionLayoutV1,
                    threshold: f64| {
            RegionRenderIdentityV1::new(
                DatasetIdV1::new(dataset).unwrap(),
                source,
                assembly,
                background,
                layout,
                threshold,
            )
            .unwrap()
        };
        for changed in [
            make(
                "223e4567-e89b-42d3-a456-426614174000",
                "sha256:test",
                "GRCh38",
                "white",
                layout.clone(),
                5e-8,
            ),
            make(
                "123e4567-e89b-42d3-a456-426614174000",
                "sha256:other",
                "GRCh38",
                "white",
                layout.clone(),
                5e-8,
            ),
            make(
                "123e4567-e89b-42d3-a456-426614174000",
                "sha256:test",
                "GRCh37",
                "white",
                layout.clone(),
                5e-8,
            ),
            make(
                "123e4567-e89b-42d3-a456-426614174000",
                "sha256:test",
                "GRCh38",
                "transparent",
                layout.clone(),
                5e-8,
            ),
            make(
                "123e4567-e89b-42d3-a456-426614174000",
                "sha256:test",
                "GRCh38",
                "white",
                RegionLayoutV1::new("1", 10_250, 2_001, 9_001, 51, 100, 2).unwrap(),
                5e-8,
            ),
            make(
                "123e4567-e89b-42d3-a456-426614174000",
                "sha256:test",
                "GRCh38",
                "white",
                RegionLayoutV1::new("1", 10_250, 1_001, 9_001, 101, 100, 2).unwrap(),
                5e-8,
            ),
            make(
                "123e4567-e89b-42d3-a456-426614174000",
                "sha256:test",
                "GRCh38",
                "white",
                RegionLayoutV1::new("1", 10_250, 1_001, 9_001, 51, 101, 2).unwrap(),
                5e-8,
            ),
            make(
                "123e4567-e89b-42d3-a456-426614174000",
                "sha256:test",
                "GRCh38",
                "white",
                RegionLayoutV1::new("1", 10_250, 1_001, 9_001, 51, 100, 1).unwrap(),
                5e-8,
            ),
            make(
                "123e4567-e89b-42d3-a456-426614174000",
                "sha256:test",
                "GRCh38",
                "white",
                layout.clone(),
                1e-6,
            ),
        ] {
            assert_ne!(base.raster_hash(), changed.raster_hash());
        }

        let selected_a = HashSet::from([1]);
        let selected_b = HashSet::from([2]);
        let points = vec![
            point(1_001, 0.5, 0.301, None, 1),
            point(1_002, 0.5, 0.301, None, 2),
        ];
        let a = reduce_region_overlay(&base, points.clone(), &selected_a, no_genes()).unwrap();
        let b = reduce_region_overlay(&base, points, &selected_b, no_genes()).unwrap();
        assert_ne!(a.overlay_hash, b.overlay_hash);
        let genes = GeneAnnotationsV1 {
            state: GeneAnnotationStateV1::Available,
            features: vec![GeneFeatureV1 {
                id: "gene".into(),
                symbol: None,
                contig: "1".into(),
                start: layout.effective_start,
                end: layout.effective_end,
                strand: GeneStrandV1::Plus,
            }],
            message: None,
        };
        let with_genes = reduce_region_overlay(
            &base,
            vec![point(1_001, 0.5, 0.301, None, 1)],
            &selected_a,
            genes,
        )
        .unwrap();
        assert_ne!(a.overlay_hash, with_genes.overlay_hash);
        let mut arbitrary = a.clone();
        arbitrary.overlay_hash = format!("sha256:{}", "0".repeat(64));
        assert_eq!(
            arbitrary.validate(),
            Err(RegionContractError::OverlayHashMismatch)
        );
    }

    #[test]
    fn request_and_gene_identity_text_is_utf8_byte_bounded() {
        let exact = "é".repeat(MAX_REGION_IDENTITY_TEXT_BYTES / 2);
        assert!(RegionLayoutV1::new(&exact, 100, 1, 100, 50, 64, 1).is_ok());
        assert!(RegionLayoutV1::new(format!("{exact}x"), 100, 1, 100, 50, 64, 1).is_err());
        let request = GeneRegionRequestV1 {
            assembly: "a".repeat(64),
            contig: exact.clone(),
            start: 1,
            end: 10,
        };
        let feature = GeneFeatureV1 {
            id: "g".into(),
            symbol: None,
            contig: exact.clone(),
            start: 1,
            end: 10,
            strand: GeneStrandV1::Unknown,
        };
        assert_eq!(
            normalize_gene_features(&request, vec![feature.clone()]).unwrap(),
            vec![feature]
        );
        let too_long_request = GeneRegionRequestV1 {
            assembly: "a".repeat(65),
            ..request.clone()
        };
        assert!(normalize_gene_features(&too_long_request, vec![]).is_err());
        let too_long_contig = format!("{exact}x");
        assert!(normalize_gene_features(
            &GeneRegionRequestV1 {
                contig: too_long_contig.clone(),
                ..request
            },
            vec![GeneFeatureV1 {
                contig: too_long_contig,
                id: "g".into(),
                symbol: None,
                start: 1,
                end: 10,
                strand: GeneStrandV1::Unknown
            }]
        )
        .is_err());
    }

    #[test]
    fn tied_gene_features_have_total_order_and_duplicates_are_rejected() {
        let request = GeneRegionRequestV1 {
            assembly: "GRCh38".into(),
            contig: "1".into(),
            start: 1,
            end: 10,
        };
        let plus = GeneFeatureV1 {
            id: "g".into(),
            symbol: Some("G".into()),
            contig: "1".into(),
            start: 1,
            end: 10,
            strand: GeneStrandV1::Plus,
        };
        let minus = GeneFeatureV1 {
            strand: GeneStrandV1::Minus,
            ..plus.clone()
        };
        let forward = normalize_gene_features(&request, vec![plus.clone(), minus.clone()]).unwrap();
        let reverse = normalize_gene_features(&request, vec![minus, plus.clone()]).unwrap();
        assert_eq!(forward, reverse);
        assert!(normalize_gene_features(&request, vec![plus.clone(), plus]).is_err());
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

        #[test]
        fn gene_normalization_is_permutation_independent(
            inputs in proptest::collection::vec((1_i64..=100, any::<bool>()), 0..100),
        ) {
            let request = GeneRegionRequestV1 {
                assembly: "GRCh38".into(),
                contig: "1".into(),
                start: 1,
                end: 100,
            };
            let features: Vec<_> = inputs
                .into_iter()
                .enumerate()
                .map(|(index, (start, plus))| GeneFeatureV1 {
                    id: format!("gene-{index}"),
                    symbol: None,
                    contig: "1".into(),
                    start,
                    end: start,
                    strand: if plus { GeneStrandV1::Plus } else { GeneStrandV1::Minus },
                })
                .collect();
            let mut reverse = features.clone();
            reverse.reverse();
            prop_assert_eq!(
                normalize_gene_features(&request, features).unwrap(),
                normalize_gene_features(&request, reverse).unwrap()
            );
        }
    }
}
