import { describe, expect, it } from 'vitest';
import { rawGoldenRegionManifest, rawGoldenRegionOverlay } from '../test/fixture.js';
import {
  parseAssociationRegionOverlay,
  parseAssociationRegionRenderManifest,
  validateAssociationRegionPair,
} from './parse.js';

type Json = Record<string, any>;
const manifest = () => structuredClone(rawGoldenRegionManifest()) as Json;
const overlay = () => structuredClone(rawGoldenRegionOverlay()) as Json;

describe('association region V1 contracts', () => {
  it('parses Rust goldens and validates the pair', () => {
    const pair = validateAssociationRegionPair(manifest(), overlay());
    expect(pair.manifest.raster_hash).toBe(pair.overlay.raster_hash);
    expect(pair.manifest.overlay_hash).toBe(pair.overlay.overlay_hash);
  });

  it.each([0, -1, 1.000001, Number.NaN, Number.POSITIVE_INFINITY])(
    'rejects invalid threshold %s in both contracts',
    (value) => {
      const m = manifest(); m.significance_threshold = value;
      const o = overlay(); o.significance_threshold = value;
      expect(() => parseAssociationRegionRenderManifest(m)).toThrow(/threshold/i);
      expect(() => parseAssociationRegionOverlay(o)).toThrow(/threshold/i);
    },
  );

  it('rejects every independently perturbed disclosure field', () => {
    const fields = [
      'input_point_count', 'protected_selected_count', 'protected_significant_count',
      'protected_coding_count', 'protected_union_count', 'ordinary_input_count',
      'ordinary_budget', 'rendered_ordinary_count', 'omitted_ordinary_count',
      'rasterized_point_count', 'overlay_point_count',
    ];
    for (const field of fields) {
      const input = overlay();
      input.aggregation[field] += 1;
      expect(() => parseAssociationRegionOverlay(input), field).toThrow();
    }
  });

  it('rejects adversarial protected flags, selection state, and arbitrary hashes', () => {
    for (const mutate of [
      (input: Json) => { input.points[0].selected = !input.points[0].selected; },
      (input: Json) => { input.points[0].significant = !input.points[0].significant; },
      (input: Json) => { input.points[0].coding = !input.points[0].coding; },
      (input: Json) => { input.points[0].ordinary_representative = !input.points[0].ordinary_representative; },
      (input: Json) => { input.selected_ordinals = []; },
      (input: Json) => { input.raster_hash = `sha256:${'0'.repeat(64)}`; },
      (input: Json) => { input.render_hash = `sha256:${'1'.repeat(64)}`; },
      (input: Json) => { input.overlay_hash = `sha256:${'2'.repeat(64)}`; },
    ]) {
      const input = overlay(); mutate(input);
      expect(() => parseAssociationRegionOverlay(input)).toThrow();
    }
  });

  it('enforces safe integers, canonical geometry/Y scale, text, media, and resources', () => {
    for (const mutate of [
      (input: Json) => { input.layout.requested_width = 4_051; },
      (input: Json) => { input.layout.physical_width += 1; },
      (input: Json) => { input.layout.y_scale.height += 1; },
      (input: Json) => { input.layout.y_scale.max_neg_log_p = 349; },
      (input: Json) => { input.counts.source_point_count = Number.MAX_SAFE_INTEGER + 1; },
      (input: Json) => { input.assembly = 'é'.repeat(33); },
      (input: Json) => { input.resources.raster.media_type = 'image/jpeg'; },
      (input: Json) => { input.resources.overlay.url = '../secret'; },
    ]) {
      const input = manifest(); mutate(input);
      expect(() => parseAssociationRegionRenderManifest(input)).toThrow();
    }
  });

  it('rejects pair substitution even when both documents are independently valid', () => {
    const otherManifest = manifest();
    const otherOverlay = overlay();
    // Exact-request layout identity differs, while effective raster work is unchanged.
    otherManifest.layout.requested_start += 1;
    otherOverlay.layout.requested_start += 1;
    // Their checked hashes are now stale, so independent validation itself blocks substitution.
    expect(() => validateAssociationRegionPair(otherManifest, otherOverlay)).toThrow(/hash/i);

    const mismatched = overlay();
    mismatched.aggregation = structuredClone(manifest().aggregation);
    mismatched.aggregation.rule_id = 'future_rule';
    expect(() => validateAssociationRegionPair(manifest(), mismatched)).toThrow();
  });

  it('rejects duplicate and non-total gene orderings', () => {
    const duplicated = overlay();
    duplicated.gene_annotations.features.push(structuredClone(duplicated.gene_annotations.features[0]));
    expect(() => parseAssociationRegionOverlay(duplicated)).toThrow(/gene/i);

    const strandTie = overlay();
    const tied = structuredClone(strandTie.gene_annotations.features[0]);
    tied.strand = 'minus';
    strandTie.gene_annotations.features.unshift(tied);
    expect(() => parseAssociationRegionOverlay(strandTie)).toThrow(/gene/i);
  });
});
