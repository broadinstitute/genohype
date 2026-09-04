//! Domain contracts and bounded rendering primitives for association visualizations.
//!
//! This crate deliberately has no dependency on `genohype-core`: source-specific
//! row decoding belongs in adapters, while normalization, reduction, layout,
//! manifests, and rasterization live here.

pub mod build;
pub mod layout;
pub mod manifest;
pub mod model;
pub mod page;
pub mod reduction;
pub mod render;
pub mod scale;
pub mod session;
pub mod source;
pub mod store;

/// Largest integer that can round-trip exactly through a JavaScript JSON consumer.
pub const JSON_SAFE_INTEGER_MAX: u64 = 9_007_199_254_740_991;

pub use build::{
    build_association_session, AssociationBuildCancellation, AssociationBuildConfig,
    AssociationBuildError, AssociationBuildOptionsV1, AssociationBuildOutcomeV1,
    AssociationBuildProduct, AssociationBuildProgressV1, BuildCancellation, BuildContractError,
    NeverCancel,
};
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
    normalize_pvalue, normalize_significance, AssociationDatasetSummaryV1, AssociationPointV1,
    NormalizedPValue, PValueError, ValidationError, WarningCode, MAX_POINT_FIELD_BYTES,
    MAX_POINT_TEXT_BYTES,
};
pub use page::{
    generate_association_page_schema, AssociationPageV1, AssociationRegionRequestV1,
    AssociationSortDirectionV1, AssociationTablePageRequestV1, AssociationTableSortV1, PageError,
    ASSOCIATION_PAGE_SCHEMA_VERSION_V1, DEFAULT_PAGE_SIZE, MAX_PAGE_POINT_TEXT_BYTES,
    MAX_PAGE_SIZE, MAX_SEARCH_BYTES,
};
pub use reduction::{compare_association_points, TopK, TopKError, DEFAULT_TOP_K, MAX_TOP_K};
pub use render::{AssociationRasterizer, Background, RenderError};
pub use scale::{YScale, LINEAR_FRACTION, LOG_THRESHOLD, MAX_NEG_LOG_P};
pub use session::{
    AssociationSessionCompletionV1, CompletedArtifactV1, DatasetIdV1, NormalizedFieldMappingV1,
    SessionBackgroundV1, SessionError, SessionKeyInputsV1, STORE_SCHEMA_VERSION_V1,
};
pub use source::{
    AssociationSource, AssociationSourceError, AssociationSourceFieldV1, AssociationSourceItem,
    AssociationSourceKindV1, AssociationSourceMetadataV1, AssociationSourceRejectionV1,
    RejectionDiagnostics, MAX_REJECTION_EXAMPLES_PER_REASON, MAX_REJECTION_REASON_CATEGORIES,
};
pub use store::{
    AssociationBuildV1, AssociationStore, AssociationStoreError, SqliteAssociationStore,
    MAX_INSERT_BATCH_SIZE,
};
