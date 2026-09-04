import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';
import { App, associationManifestUrl, loadAssociationViewer } from './App.js';

type Json = Record<string, any>;
function fixture(): Json {
  return JSON.parse(readFileSync(
    resolve(process.cwd(), '../../../association/tests/fixtures/association-visualization-v1.json'),
    'utf8',
  ));
}

function response(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json' } });
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
  });
});
