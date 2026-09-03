//! Domain contracts and bounded rendering primitives for association visualizations.
//!
//! This crate deliberately has no dependency on `genohype-core`: source-specific
//! row decoding belongs in adapters, while normalization, reduction, layout,
//! manifests, and rasterization live here.

pub mod layout;
pub mod manifest;
pub mod model;
pub mod reduction;
pub mod render;
pub mod scale;

pub use layout::{
    canonical_assembly, canonical_contig, canonical_contig_cmp, canonical_contig_lengths, Assembly,
    ContigLayoutV1, GenomeLayoutV1, LayoutError,
};
pub use manifest::{
    generate_manifest_schema, validate_schema_version, AssociationThresholdV1,
    AssociationVisualizationManifestV1, AssociationVisualizationResourcesV1, ManifestError,
    PlotKindV1, ResourceDescriptorV1, COORDINATE_CONVENTION_V1, SCHEMA_VERSION_V1,
};
pub use model::{
    normalize_pvalue, AssociationDatasetSummaryV1, AssociationPointV1, NormalizedPValue,
    PValueError, ValidationError, WarningCode,
};
pub use reduction::{compare_association_points, TopK, TopKError, DEFAULT_TOP_K, MAX_TOP_K};
pub use render::{AssociationRasterizer, Background, RenderError};
pub use scale::{YScale, LINEAR_FRACTION, LOG_THRESHOLD, MAX_NEG_LOG_P};
