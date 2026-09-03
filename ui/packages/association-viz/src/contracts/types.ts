/**
 * GENERATED FILE — DO NOT EDIT.
 * Source: association/schema/association-visualization-v1.schema.json
 * Schema SHA-256: d77ee42ae265bb0ff058abb73df752ac6d4463b3f825a9e148cc3bfbb3c3bb8c
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

export type AssociationResourceName = "raster" | "region_api" | "table_api";
