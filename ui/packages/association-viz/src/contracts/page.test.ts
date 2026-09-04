import { describe, expect, it } from 'vitest';
import { rawGoldenManifest } from '../test/fixture.js';
import { ManifestValidationError, parseAssociationPage } from './parse.js';

type Json = Record<string, any>;

function page(): Json {
  const manifest = rawGoldenManifest() as Json;
  return {
    schema_version: '1.0',
    dataset_id: '2f1f76b8-d244-4d95-8219-ec8dbeb24d8b',
    points: structuredClone(manifest.top_hits),
    offset: 100,
    limit: 100,
    total: 4321,
    sort: 'pvalue',
    direction: 'ascending',
    search: 'gene',
  };
}

describe('association page V1 parser', () => {
  it('parses table pages and preserves compatible minor fields', () => {
    const input = page();
    input.schema_version = '1.29';
    input.future_page_metadata = { cursor: 'opaque' };
    input.points[0].future_point_metadata = true;

    const parsed = parseAssociationPage(input);
    expect(parsed.schema_version).toBe('1.29');
    expect(parsed.future_page_metadata).toEqual({ cursor: 'opaque' });
    expect(parsed.points[0].future_point_metadata).toBe(true);
  });

  it('accepts region envelopes with nullable or omitted table fields', () => {
    const nullable = page();
    nullable.sort = null;
    nullable.direction = null;
    nullable.search = null;
    expect(parseAssociationPage(nullable)).toMatchObject({ sort: null, direction: null, search: null });

    const omitted = page();
    delete omitted.sort;
    delete omitted.direction;
    delete omitted.search;
    expect(parseAssociationPage(omitted)).toMatchObject({
      dataset_id: omitted.dataset_id,
      offset: 100,
      limit: 100,
    });
  });

  it.each(['2.0', '0.9'])('rejects unsupported major %s', (version) => {
    const input = page();
    input.schema_version = version;
    expect(() => parseAssociationPage(input)).toThrow(/unsupported association page schema major/i);
  });

  it.each(['1', 'v1', '1.x', '1.2.3', '01.0'])('rejects malformed version %s', (version) => {
    const input = page();
    input.schema_version = version;
    expect(() => parseAssociationPage(input)).toThrow(/malformed schema version/i);
  });

  it.each([
    '2F1F76B8-D244-4D95-8219-EC8DBEB24D8B',
    '2f1f76b8-d244-1d95-8219-ec8dbeb24d8b',
    '2f1f76b8-d244-4d95-7219-ec8dbeb24d8b',
    'not-a-dataset-id',
  ])('rejects invalid opaque dataset id %s', (datasetId) => {
    const input = page();
    input.dataset_id = datasetId;
    expect(() => parseAssociationPage(input)).toThrow(/UUID-v4/i);
  });

  it('strictly validates sort, direction, bounds, and search', () => {
    for (const mutate of [
      (input: Json) => { input.sort = 'beta'; },
      (input: Json) => { input.direction = 'up'; },
      (input: Json) => { input.offset = -1; },
      (input: Json) => { input.offset = Number.MAX_SAFE_INTEGER + 1; },
      (input: Json) => { input.limit = 0; },
      (input: Json) => { input.limit = 1001; },
      (input: Json) => { input.total = 1.5; },
      (input: Json) => { input.search = 'x'.repeat(257); },
      (input: Json) => { input.search = 'é'.repeat(129); },
    ]) {
      const input = page();
      mutate(input);
      expect(() => parseAssociationPage(input)).toThrow(ManifestValidationError);
    }
  });

  it('validates every normalized point and its browser-safe bounds', () => {
    for (const mutate of [
      (input: Json) => { input.points[0].contig = 'chr1'; },
      (input: Json) => { input.points[0].position = Number.MAX_SAFE_INTEGER + 1; },
      (input: Json) => { input.points[0].pvalue = Number.NaN; },
      (input: Json) => { input.points[0].neg_log10_p = -1; },
      (input: Json) => { input.points[0].allele_frequency = 2; },
      (input: Json) => { input.points[0].gene_symbol = 'x'.repeat(4097); },
    ]) {
      const input = page();
      mutate(input);
      expect(() => parseAssociationPage(input)).toThrow(ManifestValidationError);
    }
  });

  it('rejects oversized pages before traversing malformed tail entries', () => {
    const input = page();
    let traversedTail = false;
    const points = Array.from({ length: 1_001 }, () => ({ ...input.points[0] }));
    Object.defineProperty(points, 1_000, {
      configurable: true,
      get() {
        traversedTail = true;
        throw new Error('malformed tail was traversed');
      },
    });
    input.points = points;

    expect(() => parseAssociationPage(input)).toThrow(/more than 1000/);
    expect(traversedTail).toBe(false);
  });

  it('accepts the JSON-safe integer boundary', () => {
    const input = page();
    input.offset = Number.MAX_SAFE_INTEGER;
    input.total = Number.MAX_SAFE_INTEGER;
    input.points[0].source_ordinal = Number.MAX_SAFE_INTEGER;
    expect(parseAssociationPage(input)).toMatchObject({
      offset: Number.MAX_SAFE_INTEGER,
      total: Number.MAX_SAFE_INTEGER,
    });
  });
});
