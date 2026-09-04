import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';
import { computeLayoutHash } from '@genohype/association-viz';
import { App, associationManifestUrl, loadAssociationViewer } from './App.js';

type Json = Record<string, any>;
function fixture(): Json {
  return JSON.parse(readFileSync(
    resolve(process.cwd(), '../../../association/tests/fixtures/association-visualization-v1.json'),
    'utf8',
  ));
}

function response(body: unknown, status = 200, url?: string): Response {
  const result = new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json' } });
  if (url) Object.defineProperty(result, 'url', { configurable: true, value: url });
  return result;
}

function locusRootFixture(): Json {
  const root = fixture();
  root.plot_id = '123e4567-e89b-42d3-a456-426614174000';
  root.layout.contigs = [{ name: '4', length: 190214555, x_start: 0, x_end: 100, color: '#404040' }];
  root.top_hits = root.top_hits.map((hit: Json, index: number) => ({
    ...hit, contig: '4', position: 68_500_000 + index, source_ordinal: index === 0 ? 9 : 8,
  }));
  root.layout_hash = computeLayoutHash(root.layout, root.assembly);
  delete root.resources.table_api;
  root.resources.region_api.url = 'datasets/123e4567-e89b-42d3-a456-426614174000/region';
  return root;
}

function regionFixture(name: string): Json {
  return JSON.parse(readFileSync(resolve(process.cwd(), `../../../association/tests/fixtures/${name}`), 'utf8'));
}

describe('association viewer app', () => {
  it('constructs token-base-safe relative manifest and raster URLs', async () => {
    expect(associationManifestUrl().href).toBe('https://example.test/token/api/manifest');
    const manifest = fixture();
    manifest.resources.raster.url = 'raster';
    const fetchMock = vi.fn().mockResolvedValue(response(manifest));
    vi.stubGlobal('fetch', fetchMock);
    const state = await loadAssociationViewer(associationManifestUrl());
    expect(fetchMock).toHaveBeenCalledWith(
      new URL('https://example.test/token/api/manifest'),
      expect.objectContaining({ headers: { Accept: 'application/json' } }),
    );
    expect(state).toMatchObject({
      kind: 'ready',
      rasterUrl: 'https://example.test/token/api/raster',
      tableApiUrl: new URL('https://example.test/token/api/api/height-genome/hits'),
    });
  });

  it('preserves ordinary manifest-relative resource resolution', async () => {
    const manifest = fixture();
    const fetchMock = vi.fn().mockResolvedValue(response(manifest));
    vi.stubGlobal('fetch', fetchMock);
    const state = await loadAssociationViewer(associationManifestUrl());
    expect(state).toMatchObject({
      kind: 'ready',
      rasterUrl: 'https://example.test/token/api/height-genome.png',
    });
  });

  it('resolves a table descriptor against the authoritative manifest response URL', async () => {
    const manifest = fixture();
    manifest.resources.table_api.url = 'datasets/opaque/table';
    const manifestResponse = response(manifest);
    Object.defineProperty(manifestResponse, 'url', {
      configurable: true,
      value: 'https://example.test/redirected-token/api/manifest',
    });
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(manifestResponse));
    const state = await loadAssociationViewer(new URL('https://example.test/original-token/api/manifest'));
    expect(state).toMatchObject({
      kind: 'ready',
      tableApiUrl: new URL('https://example.test/redirected-token/api/datasets/opaque/table'),
    });
  });

  it('keeps the exact inline table path and makes no page request when table_api is absent', async () => {
    const manifest = fixture();
    delete manifest.resources.table_api;
    const fetchMock = vi.fn().mockResolvedValueOnce(response(manifest));
    vi.stubGlobal('fetch', fetchMock);
    render(<App />);
    await waitFor(() => expect(document.querySelector('[data-state="ready"]')).toBeInTheDocument());
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(screen.getByText('2 of 2 hits')).toBeInTheDocument();
  });

  it('keeps the raster available and shows a path-safe inline fallback when the table API fails', async () => {
    const manifest = fixture();
    manifest.resources.table_api.url = 'datasets/opaque/table';
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(response(manifest))
      .mockResolvedValueOnce(response({ source_path: '/private/study.tsv' }));
    vi.stubGlobal('fetch', fetchMock);
    render(<App />);
    const alert = await screen.findByRole('alert');
    expect(alert).toHaveTextContent('Showing inline top hits instead');
    expect(alert).not.toHaveTextContent('/private');
    expect(document.querySelector('[data-state="ready"]')).toBeInTheDocument();
    expect(screen.getByRole('img')).toBeInTheDocument();
    expect(screen.getByText('2 of 2 hits')).toBeInTheDocument();
  });

  it('shows distinct loading and empty states', async () => {
    vi.stubGlobal('fetch', vi.fn(() => new Promise(() => {})));
    const first = render(<App />);
    expect(screen.getByRole('status')).toHaveTextContent('Loading association manifest');
    first.unmount();

    const empty = fixture();
    Object.assign(empty, {
      row_count: 0,
      valid_row_count: 0,
      skipped_row_count: 0,
      rendered_point_count: 0,
      significant_hit_count: 0,
      top_hits: [],
    });
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(response(empty)));
    render(<App />);
    await waitFor(() => expect(document.querySelector('[data-state="empty"]')).toBeInTheDocument());
    expect(screen.getByText(/No valid association results/)).toBeInTheDocument();
  });

  it('distinguishes schema errors from resource errors', async () => {
    const major = fixture();
    major.schema_version = '2.0';
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(response(major)));
    const first = render(<App />);
    await waitFor(() => expect(document.querySelector('[data-state="schema-error"]')).toBeInTheDocument());
    expect(screen.getByRole('alert')).toHaveTextContent('Unsupported association data');
    first.unmount();

    const malformedResource = fixture();
    malformedResource.resources.raster.url = '../secret.png';
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(response(malformedResource)));
    render(<App />);
    await waitFor(() => expect(document.querySelector('[data-state="resource-error"]')).toBeInTheDocument());
    expect(screen.getByRole('alert')).toHaveTextContent('Association resource unavailable');
  });

  it('turns fetch, HTTP, JSON, and broken-image failures into resource/schema states', async () => {
    vi.stubGlobal('fetch', vi.fn().mockRejectedValue(new Error('offline')));
    expect(await loadAssociationViewer(associationManifestUrl())).toMatchObject({ kind: 'resource-error', message: expect.stringContaining('offline') });
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(response({}, 503)));
    expect(await loadAssociationViewer(associationManifestUrl())).toMatchObject({ kind: 'resource-error', message: expect.stringContaining('503') });
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response('{', { status: 200 })));
    expect(await loadAssociationViewer(associationManifestUrl())).toMatchObject({ kind: 'schema-error' });

    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(response(fixture())));
    render(<App />);
    await waitFor(() => expect(document.querySelector('[data-state="ready"]')).toBeInTheDocument());
    fireEvent.error(screen.getByRole('img'));
    await waitFor(() => expect(document.querySelector('[data-state="resource-error"]')).toBeInTheDocument());
  });

  it('pages server rows while preserving a selected top hit outside the current page', async () => {
    vi.stubGlobal('ResizeObserver', class { observe() {} disconnect() {} unobserve() {} });
    const manifest = fixture();
    manifest.resources.table_api.url = 'datasets/opaque/table';
    const [firstHit, secondHit] = manifest.top_hits;
    const page = (point: Json, offset: number) => ({
      schema_version: '1.0',
      dataset_id: '2f1f76b8-d244-4d95-8219-ec8dbeb24d8b',
      points: [point],
      offset,
      limit: 100,
      total: 200,
      sort: 'pvalue',
      direction: 'ascending',
      search: '',
    });
    const fetchMock = vi.fn(async (input: URL | RequestInfo) => {
      if (fetchMock.mock.calls.length === 1) return response(manifest);
      const url = new URL(String(input));
      const offset = Number(url.searchParams.get('offset'));
      return response(page(offset === 0 ? firstHit : secondHit, offset));
    });
    vi.stubGlobal('fetch', fetchMock);
    render(<App />);
    await waitFor(() => expect(screen.getAllByText('1–1 of 200 hits')).toHaveLength(2));
    const image = screen.getByRole('img');
    vi.spyOn(image, 'getBoundingClientRect').mockReturnValue({
      x: 0, y: 0, top: 0, left: 0, right: 100, bottom: 50, width: 100, height: 50, toJSON: () => ({}),
    });
    fireEvent.load(image);
    const firstRow = within(screen.getByRole('table')).getAllByRole('row')[1];
    await userEvent.click(firstRow);
    expect(firstRow).toHaveAttribute('aria-selected', 'true');
    expect(document.querySelector('.association-hit-selection')).toBeInTheDocument();

    await userEvent.click(screen.getByRole('button', { name: 'Next' }));
    await waitFor(() => expect(screen.getAllByText('101–101 of 200 hits')).toHaveLength(2));
    expect(within(screen.getByRole('table')).getAllByRole('row')[1]).toHaveAttribute('aria-selected', 'false');
    expect(document.querySelector('.association-hit-selection')).toBeInTheDocument();
  });

  it('synchronizes controlled table and plot selection', async () => {
    vi.stubGlobal('ResizeObserver', class { observe() {} disconnect() {} unobserve() {} });
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(response(fixture())));
    render(<App />);
    await waitFor(() => expect(document.querySelector('[data-state="ready"]')).toBeInTheDocument());
    const image = screen.getByRole('img');
    vi.spyOn(image, 'getBoundingClientRect').mockReturnValue({
      x: 0, y: 0, top: 0, left: 0, right: 100, bottom: 50, width: 100, height: 50, toJSON: () => ({}),
    });
    fireEvent.load(image);
    const table = screen.getByRole('table');
    const firstRow = within(table).getAllByRole('row')[1];
    await userEvent.click(firstRow);
    expect(firstRow).toHaveAttribute('aria-selected', 'true');
    expect(document.querySelector('.association-hit-selection')).toBeInTheDocument();
    expect(new URL(window.location.href).searchParams.get('selected')).toBe(String(fixture().top_hits[0].source_ordinal));
  });

  it('navigates chromosome clicks with history while retaining token-root API requests', async () => {
    vi.stubGlobal('ResizeObserver', class { observe() {} disconnect() {} unobserve() {} });
    const root = fixture();
    delete root.resources.table_api;
    const pending = new Promise<Response>(() => {});
    const fetchMock = vi.fn((input: URL | RequestInfo) => {
      const url = new URL(String(input));
      return url.pathname === '/token/api/manifest' ? Promise.resolve(response(root)) : pending;
    });
    vi.stubGlobal('fetch', fetchMock);
    render(<App />);
    await waitFor(() => expect(document.querySelector('[data-route="root"]')).toBeInTheDocument());
    const image = screen.getByRole('img');
    vi.spyOn(image, 'getBoundingClientRect').mockReturnValue({
      x: 0, y: 0, top: 0, left: 0, right: 100, bottom: 50, width: 100, height: 50, toJSON: () => ({}),
    });
    fireEvent.load(image);
    await userEvent.click(await screen.findByRole('button', { name: 'Chromosome 1' }));
    expect(window.location.pathname).toBe('/token/chromosome/1');
    await waitFor(() => expect(fetchMock.mock.calls.some(([input]) =>
      new URL(String(input)).pathname === '/token/api/datasets/height-genome/chromosomes/1/manifest')).toBe(true));
    expect(screen.getByRole('status')).toHaveTextContent('Loading association detail');
  });

  it('loads a direct locus route, rebases resources, and synchronizes selected overlay/table state', async () => {
    Object.defineProperty(window, 'innerWidth', { configurable: true, value: 857 });
    Object.defineProperty(window, 'devicePixelRatio', { configurable: true, value: 2 });
    window.history.replaceState(null, '', '/token/locus/4/68000111-68999001?selected=9');
    const root = locusRootFixture();
    const regionManifest = regionFixture('association-region-render-v1.json');
    const overlay = regionFixture('association-region-overlay-v1.json');
    const selected = overlay.points.find((entry: Json) => entry.point.source_ordinal === 9).point;
    const page = {
      schema_version: '1.0', dataset_id: root.plot_id, points: [selected], offset: 0, limit: 100, total: 3,
      sort: null, direction: null, search: null,
    };
    const fetchMock = vi.fn((input: URL | RequestInfo) => {
      const url = new URL(String(input));
      if (url.pathname === '/token/api/manifest') return Promise.resolve(response(root));
      if (url.pathname.endsWith('/region/manifest')) {
        return Promise.resolve(response(regionManifest, 200, `https://example.test/token/api/datasets/${root.plot_id}/region/manifest`));
      }
      if (url.pathname.endsWith('/region/overlay/fixture.json')) return Promise.resolve(response(overlay));
      if (url.pathname.endsWith(`/datasets/${root.plot_id}/region`)) return Promise.resolve(response(page));
      return Promise.resolve(response({}, 404));
    });
    vi.stubGlobal('fetch', fetchMock);
    render(<App />);
    await waitFor(() => expect(document.querySelector('[data-route="locus"]')).toBeInTheDocument());
    expect(screen.getAllByText('GENE1').length).toBeGreaterThan(0);
    await waitFor(() => expect(within(screen.getByRole('table')).getAllByRole('row')[1]).toHaveAttribute('aria-selected', 'true'));
    const manifestRequest = new URL(String(fetchMock.mock.calls.find(([input]) => String(input).includes('/region/manifest'))?.[0]));
    expect(Object.fromEntries(manifestRequest.searchParams)).toMatchObject({
      contig: '4', start: '68000111', end: '68999001', width: '777', height: '320', dpr: '2', selected: '9',
    });
    expect(fetchMock.mock.calls.some(([input]) => String(input).includes('limit=100'))).toBe(true);
    expect(fetchMock.mock.calls.every(([input]) => !String(input).includes('limit=1001'))).toBe(true);
  });

  it('reacts to popstate and canonicalizes aliases after the root manifest loads', async () => {
    const root = fixture();
    delete root.resources.table_api;
    const fetchMock = vi.fn((input: URL | RequestInfo) => {
      const url = new URL(String(input));
      if (url.pathname === '/token/api/manifest') return Promise.resolve(response(root));
      return new Promise<Response>(() => {});
    });
    vi.stubGlobal('fetch', fetchMock);
    render(<App />);
    await waitFor(() => expect(document.querySelector('[data-route="root"]')).toBeInTheDocument());
    window.history.pushState(null, '', '/token/chromosome/chr02?selected=2');
    fireEvent.popState(window);
    await waitFor(() => expect(window.location.href).toBe('https://example.test/token/chromosome/2?selected=2'));
    expect(screen.getByRole('status')).toHaveTextContent('Loading association detail');
  });
});
