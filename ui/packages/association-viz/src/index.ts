export type {
  AssociationPointV1,
  AssociationResourceName,
  AssociationThresholdV1,
  AssociationVisualizationManifestV1,
  AssociationVisualizationResourcesV1,
  ContigLayoutV1,
  GenomeLayoutV1,
  PlotKindV1,
  ResourceDescriptorV1,
  YScaleV1,
} from './contracts/types.js';
export {
  ManifestValidationError,
  parseAssociationVisualizationManifest,
  resolveAssociationResourceUrl,
  type ManifestValidationCode,
} from './contracts/parse.js';
export {
  AssociationManhattanViewer,
  type AssociationManhattanViewerProps,
} from './manhattan/AssociationManhattanViewer.js';
export {
  AssociationHitTable,
  type AssociationHitTableProps,
} from './manhattan/AssociationHitTable.js';
export {
  canonicalAssembly,
  canonicalContig,
  compareCanonicalContigs,
  computeDisplayHits,
  computeLayoutHash,
  contigAtNormalizedX,
  deriveCanonicalContigs,
  sha256,
  stableHitKey,
  verifyCanonicalGeometry,
  verifyLayoutHash,
  xForPosition,
  yForNegLog10P,
  type DisplayHit,
} from './manhattan/layout.js';
export {
  createHitIndex,
  useHitDetection,
  type HitDetectionOptions,
  type HitIndex,
  type PositionedHit,
} from './manhattan/useHitDetection.js';
export { YAxis, generateYAxisTicks, type YAxisProps } from './manhattan/YAxis.js';
export { ChromosomeLabels, type ChromosomeLabelsProps } from './manhattan/ChromosomeLabels.js';
