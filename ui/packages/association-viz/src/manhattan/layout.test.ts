import { describe, expect, it } from 'vitest';
import { fullGrch38Manifest, goldenManifest } from '../test/fixture.js';
import {
  canonicalContig,
  computeDisplayHits,
  computeLayoutHash,
  contigAtNormalizedX,
  deriveCanonicalContigs,
  sha256,
  verifyLayoutHash,
  xForPosition,
  yForNegLog10P,
} from './layout.js';

describe('manifest-driven geometry', () => {
  const manifest = goldenManifest();

  it('matches the cross-language Rust layout hash encoding', () => {
    expect(computeLayoutHash(manifest.layout, manifest.assembly)).toBe(manifest.layout_hash);
    expect(sha256(new TextEncoder().encode('abc'))).toBe('ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad');
    expect(() => verifyLayoutHash(manifest.layout, manifest.assembly, 'sha256:wrong')).toThrow(/layout hash mismatch/);
  });

  it.each([
    ['chr1', '1'], ['01', '1'], ['ChrX', 'X'], ['chrM', 'MT'], ['MT', 'MT'],
  ])('canonicalizes alias %s to %s', (alias, expected) => {
    expect(canonicalContig(alias)).toBe(expected);
  });

  it('maps 1-based raw coordinates exactly and excludes gaps/out-of-range positions', () => {
    expect(xForPosition(manifest.layout, 'chr1', 3)).toBeCloseTo(0.064, 12);
    expect(xForPosition(manifest.layout, '2', 9)).toBeCloseTo(0.616, 12);
    expect(xForPosition(manifest.layout, '1', 0)).toBeNull();
    expect(xForPosition(manifest.layout, '2', 21)).toBeNull();
    expect(contigAtNormalizedX(manifest.layout, 0.34)).toBeNull();
    expect(contigAtNormalizedX(manifest.layout, 0.5)).toBe('2');
  });

  it('preserves underflow significance and scales at multiple viewports', () => {
    const hits = computeDisplayHits(manifest.top_hits, manifest.layout);
    expect(hits.map((hit) => [hit.x_normalized, hit.y_normalized])).toEqual([
      [0.064, 0],
      [0.616, 0.52],
    ]);
    for (const [width, height] of [[100, 50], [375, 187.5], [1200, 600]]) {
      expect(hits[0].x_normalized * width).toBeCloseTo(width * 0.064, 10);
      expect(hits[1].y_normalized * height).toBeCloseTo(height * 0.52, 10);
    }
    expect(yForNegLog10P(manifest.layout.y_scale, 400)).toBe(0);
    expect(yForNegLog10P(manifest.layout.y_scale, 10)).toBeCloseTo(0.4, 12);
  });

  it('accepts one-ULP Rust JSON round trips for a generated full GRCh38 Phase 1 layout', () => {
    const generated = fullGrch38Manifest();
    expect(generated.layout).toMatchObject({ width: 3000, height: 800 });
    expect(generated.layout.contigs).toHaveLength(25);
    expect(generated.top_hits).toHaveLength(10);
    const expected = deriveCanonicalContigs(generated.layout);
    const oneUlpMismatches: string[] = [];
    const buffer = new ArrayBuffer(8);
    const view = new DataView(buffer);
    const nextUp = (value: number): number => {
      view.setFloat64(0, value);
      view.setBigUint64(0, view.getBigUint64(0) + 1n);
      return view.getFloat64(0);
    };

    generated.layout.contigs.forEach((actual, index) => {
      for (const coordinate of ['x_start', 'x_end'] as const) {
        const canonical = expected[index][coordinate];
        if (actual[coordinate] !== canonical) {
          expect(nextUp(actual[coordinate])).toBe(canonical);
          oneUlpMismatches.push(`${actual.name}.${coordinate}`);
        }
      }
    });
    expect(oneUlpMismatches).toEqual(['6.x_end', '7.x_start', '22.x_end', 'X.x_start']);
    expect(() => verifyLayoutHash(generated.layout, generated.assembly, generated.layout_hash)).not.toThrow();
  });

  it('rejects meaningful geometry drift even when the layout hash inputs are unchanged', () => {
    const generated = fullGrch38Manifest();
    generated.layout.contigs[21].x_end += 0.01;
    expect(computeLayoutHash(generated.layout, generated.assembly)).toBe(generated.layout_hash);
    expect(() => verifyLayoutHash(generated.layout, generated.assembly, generated.layout_hash)).toThrow(/non-canonical/i);

    const nonFinite = fullGrch38Manifest();
    nonFinite.layout.contigs[0].x_end = Number.NaN;
    expect(() => verifyLayoutHash(nonFinite.layout, nonFinite.assembly, nonFinite.layout_hash)).toThrow(/non-canonical/i);

    const changed = structuredClone(manifest.layout);
    changed.contigs[0].x_end += 1;
    expect(() => verifyLayoutHash(changed, manifest.assembly, manifest.layout_hash)).toThrow(/non-canonical/i);
    const changedScale = structuredClone(manifest.layout);
    changedScale.y_scale.linear_fraction = 0.5;
    expect(() => verifyLayoutHash(changedScale, manifest.assembly, manifest.layout_hash)).toThrow(/canonical V1 y scale/i);
  });
});
