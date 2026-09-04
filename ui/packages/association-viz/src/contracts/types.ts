/**
 * GENERATED FILE — DO NOT EDIT.
 * Sources: association/schema/association-visualization-v1.schema.json, association/schema/association-page-v1.schema.json
 * association-visualization-v1.schema.json SHA-256: 718b10211efde93506441b17cda5ca94e6bbd926f0ff3666ab33aa71f17aa1dc
 * association-page-v1.schema.json SHA-256: 41fed34cee5fbd5678dc8ffb0b4a6af0a5304b32a49d1b38e23289ea20ce2a00
 * Regenerate with: npm run contracts:generate -w @genohype/association-viz
 */

export interface AssociationPointV1 {
  allele_count?: number | null;
  allele_frequency?: number | null;
  alt_allele?: string | null;
  beta?: number | null;
  consequence?: string | null;
  contig: string;
  gene_symbol?: string | null;
  neg_log10_p: number;
  position: number;
  pvalue: number;
  ref_allele?: string | null;
  series?: string | null;
  source_ordinal: number;
  standard_error?: number | null;
  [minorField: string]: unknown;
}

export interface AssociationThresholdV1 {
  name: string;
  pvalue: number;
  [minorField: string]: unknown;
}

export interface AssociationVisualizationResourcesV1 {
  raster: ResourceDescriptorV1;
  region_api?: ResourceDescriptorV1 | null;
  table_api?: ResourceDescriptorV1 | null;
  [minorField: string]: unknown;
}

export interface ContigLayoutV1 {
  color: string;
  length: number;
  name: string;
  x_end: number;
  x_start: number;
  [minorField: string]: unknown;
}

export interface GenomeLayoutV1 {
  contig_gap: number;
  contigs: ContigLayoutV1[];
  height: number;
  width: number;
  y_scale: YScaleV1;
  [minorField: string]: unknown;
}

export type PlotKindV1 = "manhattan" | "locus";

export interface ResourceDescriptorV1 {
  byte_size?: number | null;
  checksum?: string | null;
  expires_at?: string | null;
  media_type: string;
  url: string;
  [minorField: string]: unknown;
}

export interface YScaleV1 {
  height: number;
  linear_fraction: number;
  log_threshold: number;
  max_neg_log_p: number;
  [minorField: string]: unknown;
}

export type AssociationSortDirectionV1 = "ascending" | "descending";

export type AssociationTableSortV1 = "pvalue" | "locus";

export type DatasetIdV1 = string;

export interface AssociationVisualizationManifestV1 {
  assembly: string;
  coordinate_convention: string;
  created_at: string;
  genohype_version: string;
  kind: PlotKindV1;
  layout: GenomeLayoutV1;
  layout_hash: string;
  plot_id: string;
  rendered_point_count: number;
  resources: AssociationVisualizationResourcesV1;
  row_count: number;
  schema_version: string;
  significant_hit_count: number;
  skipped_row_count: number;
  source_fingerprint: string;
  thresholds: AssociationThresholdV1[];
  title: string;
  top_hits: AssociationPointV1[];
  valid_row_count: number;
  warnings: string[];
  [minorField: string]: unknown;
}

export interface AssociationPageV1 {
  dataset_id: DatasetIdV1;
  direction?: AssociationSortDirectionV1 | null;
  limit: number;
  offset: number;
  points: AssociationPointV1[];
  schema_version: string;
  search?: string | null;
  sort?: AssociationTableSortV1 | null;
  total: number;
  [minorField: string]: unknown;
}

export type AssociationResourceName = "raster" | "region_api" | "table_api";
