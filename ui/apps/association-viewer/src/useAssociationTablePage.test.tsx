import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { act, renderHook, waitFor } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import type { AssociationPointV1 } from '@genohype/association-viz';
import {
  associationTablePageUrl,
  useAssociationTablePage,
} from './useAssociationTablePage.js';

type Json = Record<string, any>;
const DATASET_ID = '2f1f76b8-d244-4d95-8219-ec8dbeb24d8b';

function fixture(): Json {
  return JSON.parse(readFileSync(
    resolve(process.cwd(), '../../../association/tests/fixtures/association-visualization-v1.json'),
    'utf8',
  ));
}

function page(
  points: AssociationPointV1[],
  options: Partial<Json> = {},
): Json {
  return {
    schema_version: '1.0',
    dataset_id: DATASET_ID,
    points,
    offset: 0,
    limit: 100,
    total: points.length,
    sort: 'pvalue',
    direction: 'ascending',
    search: '',
    ...options,
  };
}

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json' },
  });
}

function deferred<T>() {
  let resolvePromise!: (value: T) => void;
  const promise = new Promise<T>((resolveValue) => { resolvePromise = resolveValue; });
  return { promise, resolve: resolvePromise };
}

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe('association table page transport', () => {
  it('constructs bounded token-scoped requests exclusively through URL search parameters', () => {
    const url = associationTablePageUrl(
      new URL('https://example.test/token/api/datasets/opaque/table'),
      100,
      100,
      'locus',
      'descending',
      'BRCA1 & 10%_hits',
    );
    expect(url.origin + url.pathname).toBe('https://example.test/token/api/datasets/opaque/table');
    expect(Object.fromEntries(url.searchParams)).toEqual({
      offset: '100',
      limit: '100',
      sort: 'locus',
      direction: 'descending',
      search: 'BRCA1 & 10%_hits',
    });
    expect(url.href).toContain('search=BRCA1+%26+10%25_hits');
  });

  it('manages paging, sort, and debounced search state', async () => {
    const hits = fixture().top_hits as AssociationPointV1[];
    const fetchMock = vi.fn(async (input: URL | RequestInfo) => {
      const url = new URL(String(input));
      return jsonResponse(page(hits.slice(0, 1), {
        offset: Number(url.searchParams.get('offset')),
        sort: url.searchParams.get('sort'),
        direction: url.searchParams.get('direction'),
        search: url.searchParams.get('search'),
        total: 250,
      }));
    });
    vi.stubGlobal('fetch', fetchMock);
    const { result } = renderHook(() => useAssociationTablePage({
      tableApiUrl: new URL('https://example.test/token/api/datasets/id/table'),
      fallbackRows: hits,
      total: 250,
      debounceMs: 10,
    }));

    await waitFor(() => expect(result.current.loading).toBe(false));
    act(() => result.current.setOffset(100));
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2));
    expect(result.current.offset).toBe(100);

    act(() => result.current.setSort('locus', 'descending'));
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(3));
    expect(result.current).toMatchObject({ offset: 0, sort: 'locus', direction: 'descending' });

    act(() => result.current.setQuery('BRCA1 & 10%'));
    expect(result.current.offset).toBe(0);
    expect(fetchMock).toHaveBeenCalledTimes(3);
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(4));
    const lastUrl = new URL(String(fetchMock.mock.calls[fetchMock.mock.calls.length - 1]?.[0]));
    expect(lastUrl.searchParams.get('search')).toBe('BRCA1 & 10%');
  });

  it('debounces search, aborts superseded work, and ignores stale completion', async () => {
    vi.useFakeTimers();
    const hits = fixture().top_hits as AssociationPointV1[];
    const first = deferred<Response>();
    const second = deferred<Response>();
    const signals: AbortSignal[] = [];
    const fetchMock = vi.fn((_input: URL | RequestInfo, init?: RequestInit) => {
      signals.push(init?.signal as AbortSignal);
      return fetchMock.mock.calls.length === 1 ? first.promise : second.promise;
    });
    vi.stubGlobal('fetch', fetchMock);

    const { result } = renderHook(() => useAssociationTablePage({
      tableApiUrl: new URL('https://example.test/token/api/datasets/id/table'),
      fallbackRows: hits,
      total: 2,
    }));
    expect(fetchMock).toHaveBeenCalledTimes(1);

    act(() => result.current.setQuery('new search'));
    expect(signals[0].aborted).toBe(true);
    await act(async () => { await vi.advanceTimersByTimeAsync(149); });
    expect(fetchMock).toHaveBeenCalledTimes(1);
    await act(async () => { await vi.advanceTimersByTimeAsync(1); });
    expect(fetchMock).toHaveBeenCalledTimes(2);

    await act(async () => {
      second.resolve(jsonResponse(page([hits[1]], { search: 'new search', total: 1 })));
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(result.current.rows).toEqual([hits[1]]);

    await act(async () => {
      first.resolve(jsonResponse(page([hits[0]])));
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(result.current.rows).toEqual([hits[1]]);
  });

  it.each([
    ['malformed', () => Promise.resolve(jsonResponse({ source_path: '/private/input.tsv' }))],
    ['network', () => Promise.reject(new Error('/private/input.tsv was unavailable'))],
    ['HTTP', () => Promise.resolve(jsonResponse({}, 500))],
  ])('falls back to bounded inline rows for %s failures without leaking details', async (_name, request) => {
    const hits = fixture().top_hits as AssociationPointV1[];
    vi.stubGlobal('fetch', vi.fn(request));
    const { result } = renderHook(() => useAssociationTablePage({
      tableApiUrl: new URL('https://example.test/token/api/datasets/id/table'),
      fallbackRows: hits,
      total: 10_000,
    }));
    await waitFor(() => expect(result.current.error).not.toBeNull());
    expect(result.current.rows).toEqual(hits);
    expect(result.current.total).toBe(hits.length);
    expect(result.current.error).not.toContain('/private');
  });
});
