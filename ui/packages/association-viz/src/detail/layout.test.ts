import { describe, expect, it } from 'vitest';
import { validateAssociationRegionPair } from '../contracts/parse.js';
import type { RegionLayoutV1 } from '../contracts/types.js';
import { rawGoldenRegionManifest, rawGoldenRegionOverlay } from '../test/fixture.js';
import {
  positionRegionOverlayPoint,
  validateRegionRasterDimensions,
  xForRegionPosition,
  yForRegionNegLog10P,
} from './layout.js';

function pair() {
  return validateAssociationRegionPair(rawGoldenRegionManifest(), rawGoldenRegionOverlay());
}

function simpleLayout(dpr: number): RegionLayoutV1 {
  return {
    contig: '1',
    contig_length: 1_000,
    requested_start: 1,
    requested_end: 101,
    effective_start: 1,
    effective_end: 101,
    requested_width: 100,
    width: 100,
    height: 50,
    dpr,
    physical_width: 100 * dpr,
    physical_height: 50 * dpr,
    y_scale: {
      height: 50 * dpr,
      log_threshold: 10,
      linear_fraction: 0.6,
      max_neg_log_p: 350,
    },
  };
}

describe('region detail geometry', () => {
  it('maps first, middle, and last inclusive bases using physical pixel edges', () => {
    const layout = simpleLayout(2);
    expect(xForRegionPosition(layout, 1)).toBe(0);
    expect(xForRegionPosition(layout, 51)).toBeCloseTo(49.75);
    expect(xForRegionPosition(layout, 101)).toBe(99.5);
    expect(xForRegionPosition(layout, 0)).toBeNull();
    expect(xForRegionPosition(layout, 102)).toBeNull();
  });

  it('uses the canonical Y scale and caps values above 350', () => {
    const layout = simpleLayout(4);
    expect(yForRegionNegLog10P(layout, 0)).toBe(50);
    expect(yForRegionNegLog10P(layout, 10)).toBeCloseTo(20);
    expect(yForRegionNegLog10P(layout, 350)).toBe(0);
    expect(yForRegionNegLog10P(layout, 900)).toBe(0);
  });

  it.each([1, 2, 4])('requires exact physical dimensions at DPR %i while keeping logical geometry', (dpr) => {
    const layout = simpleLayout(dpr);
    expect(() => validateRegionRasterDimensions(layout, 100 * dpr, 50 * dpr)).not.toThrow();
    expect(() => validateRegionRasterDimensions(layout, 100 * dpr + 1, 50 * dpr)).toThrow(/DPR/);
    expect(layout.width).toBe(100);
    expect(xForRegionPosition(layout, layout.effective_end)).toBe(100 - 1 / dpr);
  });

  it('positions semantic overlay points in logical SVG coordinates', () => {
    const { manifest, overlay } = pair();
    const first = positionRegionOverlayPoint(overlay.points[0], manifest.layout);
    const last = positionRegionOverlayPoint(overlay.points[2], manifest.layout);
    expect(first).toMatchObject({ x: 0, y: 0, significant: true, coding: true });
    expect(last?.x).toBeCloseTo(799.4992, 3);
    expect(last?.key).toBe('4:68999999:A:G:9');
  });
});
