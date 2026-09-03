/**
 * TypeScript binding for association/schema/association-visualization-v1.schema.json.
 * Keep field names and optionality aligned with the committed Rust-generated schema.
 * V1 minor additions are intentionally represented by the index signatures.
 */

export type PlotKindV1 = 'manhattan' | 'locus';

export interface AssociationPointV1 {
  contig: string;
  position: number;
  pvalue: number;
  neg_log10_p: number;
  source_ordinal: number;
  ref_allele?: string | null;
  alt_allele?: string | null;
  beta?: number | null;
  standard_error?: number | null;
  allele_frequency?: number | null;
  allele_count?: number | null;
  gene_symbol?: string | null;
  consequence?: string | null;
  series?: string | null;
  [minorField: string]: unknown;
}

export interface AssociationThresholdV1 {
  name: string;
  pvalue: number;
  [minorField: string]: unknown;
}

export interface ResourceDescriptorV1 {
  url: string;
  media_type: string;
  byte_size?: number | null;
  checksum?: string | null;
  expires_at?: string | null;
  [minorField: string]: unknown;
}

export interface AssociationVisualizationResourcesV1 {
  raster: ResourceDescriptorV1;
  region_api?: ResourceDescriptorV1 | null;
  table_api?: ResourceDescriptorV1 | null;
  [minorField: string]: unknown;
}

export interface ContigLayoutV1 {
  name: string;
  length: number;
  x_start: number;
  x_end: number;
  color: string;
  [minorField: string]: unknown;
}

export interface YScaleV1 {
  height: number;
  log_threshold: number;
  linear_fraction: number;
  max_neg_log_p: number;
  [minorField: string]: unknown;
}

export interface GenomeLayoutV1 {
  width: number;
  height: number;
  contig_gap: number;
  contigs: ContigLayoutV1[];
  y_scale: YScaleV1;
  [minorField: string]: unknown;
}

export interface AssociationVisualizationManifestV1 {
  schema_version: string;
  plot_id: string;
  kind: PlotKindV1;
  title: string;
  assembly: string;
  coordinate_convention: string;
  source_fingerprint: string;
  genohype_version: string;
  created_at: string;
  layout: GenomeLayoutV1;
  layout_hash: string;
  thresholds: AssociationThresholdV1[];
  row_count: number;
  valid_row_count: number;
  skipped_row_count: number;
  rendered_point_count: number;
  significant_hit_count: number;
  top_hits: AssociationPointV1[];
  warnings: string[];
  resources: AssociationVisualizationResourcesV1;
  [minorField: string]: unknown;
}

export type AssociationResourceName = 'raster' | 'region_api' | 'table_api';
