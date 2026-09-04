import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { act, renderHook } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  computeLayoutHash,
  type AssociationVisualizationManifestV1,
} from '@genohype/association-viz';
import type { ViewerRoute } from './route.js';
import {
  DETAIL_RESIZE_DEBOUNCE_MS,
  chromosomeManifestUrl,
  detailRequestMetrics,
  loadChromosomeDetail,
  loadLocusDetail,
  regionManifestUrl,
  useAssociationDetail,
} from './useAssociationDetail.js';

type Json = Record<string, any>;
const DATASET_ID = '123e4567-e89b-42d3-a456-426614174000';

function jsonFixture(name: string): Json {
  return JSON.parse(readFileSync(resolve(process.cwd(), `../../../association/tests/fixtures/${name}`), 'utf8'));
}

function rootManifest(): AssociationVisualizationManifestV1 {
  const root = jsonFixture('association-visualization-v1.json');
  root.plot_id = DATASET_ID;
  return root as AssociationVisualizationManifestV1;
}

function chromosomeManifest(): Json {
  const chromosome = jsonFixture('association-visualization-v1.json');
  chromosome.plot_id = DATASET_ID;
  chromosome.layout.contigs = [chromosome.layout.contigs[0]];
  chromosome.layout.contigs[0].x_start = 0;
  chromosome.layout.contigs[0].x_end = chromosome.layout.width;
  chromosome.top_hits = [chromosome.top_hits[0]];
  chromosome.row_count = 1;
  chromosome.valid_row_count = 1;
  chromosome.skipped_row_count = 0;
  chromosome.rendered_point_count = 1;
  chromosome.significant_hit_count = 1;
  chromosome.layout_hash = computeLayoutHash(chromosome.layout, chromosome.assembly);
  return chromosome;
}

function response(body: unknown, status = 200, url?: string): Response {
  const result = new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json' } });
  if (url) Object.defineProperty(result, 'url', { configurable: true, value: url });
  return result;
}

const locusRoute: Extract<ViewerRoute, { kind: 'locus' }> = {
  kind: 'locus', contig: '4', start: 68_000_111, end: 68_999_001, selectedSourceOrdinal: 9,
};
const fixtureMetrics = { width: 777, height: 320, dpr: 2 };

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe('association detail transport', () => {
  it('bounds viewport width and integer DPR deterministically', () => {
    expect(detailRequestMetrics(20, 0.4)).toEqual({ width: 50, height: 320, dpr: 1 });
    expect(detailRequestMetrics(857, 1.6)).toEqual(fixtureMetrics);
    expect(detailRequestMetrics(10_000, 9)).toEqual({ width: 4050, height: 320, dpr: 4 });
  });

  it('constructs only bounded dataset detail endpoints', () => {
    const root = new URL('https://host.test/token/');
    expect(chromosomeManifestUrl(root, DATASET_ID, 'X').href)
      .toBe(`https://host.test/token/api/datasets/${DATASET_ID}/chromosomes/X/manifest`);
    const url = regionManifestUrl(root, DATASET_ID, locusRoute, fixtureMetrics);
    expect(url.pathname).toBe(`/token/api/datasets/${DATASET_ID}/region/manifest`);
    expect(Object.fromEntries(url.searchParams)).toEqual({
      contig: '4', start: '68000111', end: '68999001', width: '777', height: '320', dpr: '2', selected: '9',
    });
    expect(url.href).not.toContain('points');
    expect(url.href).not.toContain('limit');
  });

  it('loads and validates a one-contig manifest and rebases its raster against response.url', async () => {
    const finalUrl = `https://host.test/final/api/datasets/${DATASET_ID}/chromosomes/1/manifest`;
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(response(chromosomeManifest(), 200, finalUrl)));
    await expect(loadChromosomeDetail(new URL('https://host.test/token/request'), rootManifest(), '1'))
      .resolves.toMatchObject({ kind: 'chromosome-ready', rasterUrl: `https://host.test/final/api/datasets/${DATASET_ID}/chromosomes/1/height-genome.png` });
  });

  it('loads a validated manifest/overlay pair and rebases query-free descriptors to the final response URL', async () => {
    const manifest = jsonFixture('association-region-render-v1.json');
    const overlay = jsonFixture('association-region-overlay-v1.json');
    const finalUrl = `https://host.test/final/api/datasets/${DATASET_ID}/region/manifest?ignored=1`;
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(response(manifest, 200, finalUrl))
      .mockResolvedValueOnce(response(overlay));
    vi.stubGlobal('fetch', fetchMock);
    const state = await loadLocusDetail(new URL('https://host.test/token/request'), rootManifest(), locusRoute, fixtureMetrics);
    expect(state).toMatchObject({
      kind: 'locus-ready',
      rasterUrl: `https://host.test/final/api/datasets/${DATASET_ID}/region/region/raster/fixture.png`,
    });
    expect(String(fetchMock.mock.calls[1]?.[0])).toBe(
      `https://host.test/final/api/datasets/${DATASET_ID}/region/region/overlay/fixture.json`,
    );
  });

  it.each([
    [400, 'resource-error'],
    [404, 'resource-error'],
    [422, 'bounded-too-dense'],
    [429, 'resource-error'],
    [503, 'resource-error'],
  ])('maps HTTP %i to a visible typed state', async (status, kind) => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(response({}, status)));
    await expect(loadLocusDetail(new URL('https://host.test/token/request'), rootManifest(), locusRoute, fixtureMetrics))
      .resolves.toMatchObject({ kind });
  });

  it.each([429, 503])('marks HTTP %i server-busy responses as bounded and retriable', async (status) => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(response({ source_path: '/private/input.tsv' }, status)));
    const state = await loadLocusDetail(new URL('https://host.test/token/request'), rootManifest(), locusRoute, fixtureMetrics);
    expect(state).toEqual({
      kind: 'resource-error',
      message: 'The association server is busy. Try this detail again shortly.',
      retriable: true,
    });
    expect(JSON.stringify(state)).not.toContain('/private');
  });

  it('rejects bad manifest hashes and mismatched overlays before display', async () => {
    const badManifest = jsonFixture('association-region-render-v1.json');
    badManifest.render_hash = `sha256:${'0'.repeat(64)}`;
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(response(badManifest)));
    await expect(loadLocusDetail(new URL('https://host.test/token/request'), rootManifest(), locusRoute, fixtureMetrics))
      .resolves.toMatchObject({ kind: 'schema-error' });

    const overlay = jsonFixture('association-region-overlay-v1.json');
    overlay.render_hash = `sha256:${'0'.repeat(64)}`;
    vi.stubGlobal('fetch', vi.fn()
      .mockResolvedValueOnce(response(jsonFixture('association-region-render-v1.json')))
      .mockResolvedValueOnce(response(overlay)));
    await expect(loadLocusDetail(new URL('https://host.test/token/request'), rootManifest(), locusRoute, fixtureMetrics))
      .resolves.toMatchObject({ kind: 'schema-error' });
  });

  it('debounces viewport changes and aborts stale detail requests', async () => {
    vi.useFakeTimers();
    Object.defineProperty(window, 'innerWidth', { configurable: true, value: 900 });
    Object.defineProperty(window, 'devicePixelRatio', { configurable: true, value: 1 });
    const signals: AbortSignal[] = [];
    const fetchMock = vi.fn((_input: URL | RequestInfo, init?: RequestInit) => {
      signals.push(init?.signal as AbortSignal);
      return new Promise<Response>(() => {});
    });
    vi.stubGlobal('fetch', fetchMock);
    const route: ViewerRoute = { ...locusRoute };
    const root = new URL('https://host.test/token/');
    const manifest = rootManifest();
    renderHook(() => useAssociationDetail(route, root, manifest));
    expect(fetchMock).toHaveBeenCalledTimes(1);

    Object.defineProperty(window, 'innerWidth', { configurable: true, value: 1200 });
    act(() => window.dispatchEvent(new Event('resize')));
    await act(async () => { await vi.advanceTimersByTimeAsync(DETAIL_RESIZE_DEBOUNCE_MS - 1); });
    expect(fetchMock).toHaveBeenCalledTimes(1);
    await act(async () => { await vi.advanceTimersByTimeAsync(1); });
    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(signals[0].aborted).toBe(true);
    expect(new URL(String(fetchMock.mock.calls[1]?.[0])).searchParams.get('width')).toBe('1120');
  });
});
