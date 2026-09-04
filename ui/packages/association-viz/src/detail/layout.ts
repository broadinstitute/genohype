import type {
  AssociationPointV1,
  AssociationRegionOverlayPointV1,
  RegionLayoutV1,
} from '../contracts/types.js';
import { stableHitKey, yForNegLog10P } from '../manhattan/layout.js';

/** A semantic region-overlay point positioned in logical (CSS) plot coordinates. */
export interface LocusDisplayPoint extends AssociationPointV1 {
  key: string;
  x: number;
  y: number;
  x_normalized: number;
  y_normalized: number;
  coding: boolean;
  significant: boolean;
  ordinary_representative: boolean;
  absent_representative: boolean;
}

/**
 * Reproduce RegionLayoutV1's inclusive raster geometry, then convert the
 * physical pixel coordinate to the logical CSS/SVG coordinate.
 */
export function xForRegionPosition(layout: RegionLayoutV1, position: number): number | null {
  if (!Number.isSafeInteger(position)
      || position < layout.effective_start
      || position > layout.effective_end) return null;
  if (layout.effective_start === layout.effective_end || layout.physical_width === 1) return 0;
  const fraction = (position - layout.effective_start)
    / (layout.effective_end - layout.effective_start);
  return fraction * (layout.physical_width - 1) / layout.dpr;
}

/** Map canonical significance into the logical CSS/SVG coordinate space. */
export function yForRegionNegLog10P(layout: RegionLayoutV1, value: number): number {
  return yForNegLog10P(layout.y_scale, value) * layout.height;
}

export function positionRegionOverlayPoint(
  entry: AssociationRegionOverlayPointV1,
  layout: RegionLayoutV1,
): LocusDisplayPoint | null {
  const x = xForRegionPosition(layout, entry.point.position);
  if (x === null || entry.point.contig !== layout.contig) return null;
  const y = yForRegionNegLog10P(layout, entry.point.neg_log10_p);
  return {
    ...entry.point,
    key: stableHitKey(entry.point),
    x,
    y,
    x_normalized: x / layout.width,
    y_normalized: y / layout.height,
    coding: entry.coding,
    significant: entry.significant,
    ordinary_representative: entry.ordinary_representative,
    absent_representative: false,
  };
}

/** Position a controlled selection that was not retained in the semantic overlay. */
export function positionAbsentSelectedPoint(
  point: AssociationPointV1,
  layout: RegionLayoutV1,
): LocusDisplayPoint | null {
  const x = xForRegionPosition(layout, point.position);
  if (x === null || point.contig !== layout.contig) return null;
  const y = yForRegionNegLog10P(layout, point.neg_log10_p);
  return {
    ...point,
    key: stableHitKey(point),
    x,
    y,
    x_normalized: x / layout.width,
    y_normalized: y / layout.height,
    coding: false,
    significant: false,
    ordinary_representative: false,
    absent_representative: true,
  };
}

/** Require the encoded PNG's natural size, not its responsive CSS size. */
export function validateRegionRasterDimensions(
  layout: RegionLayoutV1,
  naturalWidth: number,
  naturalHeight: number,
): void {
  if (naturalWidth !== layout.physical_width || naturalHeight !== layout.physical_height) {
    throw new Error(
      `raster dimensions ${naturalWidth}×${naturalHeight} do not match manifest physical dimensions ${layout.physical_width}×${layout.physical_height} (DPR ${layout.dpr})`,
    );
  }
}
