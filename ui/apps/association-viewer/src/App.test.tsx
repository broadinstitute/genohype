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
    const fetchMock = vi.fn().mockResolvedValue(response(fixture()));
    vi.stubGlobal('fetch', fetchMock);
    const state = await loadAssociationViewer(associationManifestUrl());
    expect(fetchMock).toHaveBeenCalledWith(
      new URL('https://example.test/token/api/manifest'),
      expect.objectContaining({ headers: { Accept: 'application/json' } }),
    );
    expect(state).toMatchObject({
      kind: 'ready',
      rasterUrl: 'https://example.test/token/api/height-genome.png',
    });
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
