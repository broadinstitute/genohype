import { useCallback, useEffect, useRef, useState } from 'react';
import {
  parseAssociationPage,
  type AssociationPointV1,
  type AssociationSortDirectionV1,
  type AssociationTableSortV1,
  type DatasetIdV1,
} from '@genohype/association-viz';

export const ASSOCIATION_TABLE_PAGE_LIMIT = 100;
export const ASSOCIATION_TABLE_SEARCH_DEBOUNCE_MS = 150;
const MAX_SEARCH_BYTES = 256;

export interface AssociationTablePageState {
  rows: readonly AssociationPointV1[];
  total: number;
  offset: number;
  limit: number;
  query: string;
  sort: AssociationTableSortV1;
  direction: AssociationSortDirectionV1;
  loading: boolean;
  error: string | null;
  setQuery: (query: string) => void;
  setSort: (sort: AssociationTableSortV1, direction: AssociationSortDirectionV1) => void;
  setOffset: (offset: number) => void;
  retry: () => void;
}

interface UseAssociationTablePageOptions {
  tableApiUrl: URL;
  expectedDatasetId: DatasetIdV1;
  fallbackRows: readonly AssociationPointV1[];
  total: number;
  limit?: number;
  debounceMs?: number;
  requestKind?: 'table' | 'region';
}

function boundedSearch(input: string): string {
  const encoder = new TextEncoder();
  let result = '';
  let bytes = 0;
  for (const character of input) {
    const characterBytes = encoder.encode(character).length;
    if (bytes + characterBytes > MAX_SEARCH_BYTES) break;
    result += character;
    bytes += characterBytes;
  }
  return result;
}

export function associationTablePageUrl(
  tableApiUrl: URL,
  offset: number,
  limit: number,
  sort: AssociationTableSortV1,
  direction: AssociationSortDirectionV1,
  search: string,
  requestKind: 'table' | 'region' = 'table',
): URL {
  const url = new URL(tableApiUrl.href);
  url.searchParams.set('offset', String(offset));
  url.searchParams.set('limit', String(limit));
  if (requestKind === 'table') {
    url.searchParams.set('sort', sort);
    url.searchParams.set('direction', direction);
    url.searchParams.set('search', search);
  } else {
    url.searchParams.delete('sort');
    url.searchParams.delete('direction');
    url.searchParams.delete('search');
  }
  return url;
}

export function useAssociationTablePage({
  tableApiUrl,
  expectedDatasetId,
  fallbackRows,
  total: manifestTotal,
  limit = ASSOCIATION_TABLE_PAGE_LIMIT,
  debounceMs = ASSOCIATION_TABLE_SEARCH_DEBOUNCE_MS,
  requestKind = 'table',
}: UseAssociationTablePageOptions): AssociationTablePageState {
  const [rows, setRows] = useState<readonly AssociationPointV1[]>(fallbackRows);
  const [total, setTotal] = useState(manifestTotal);
  const [offset, setOffsetState] = useState(0);
  const [query, setQueryState] = useState('');
  const [debouncedQuery, setDebouncedQuery] = useState('');
  const [sort, setSortState] = useState<AssociationTableSortV1>(requestKind === 'region' ? 'locus' : 'pvalue');
  const [direction, setDirection] = useState<AssociationSortDirectionV1>('ascending');
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [retryCount, setRetryCount] = useState(0);
  const requestSequence = useRef(0);
  const tableApiHref = tableApiUrl.href;
  const requestIdentity = `${requestKind}\u0000${expectedDatasetId}\u0000${tableApiHref}`;
  const requestIdentityRef = useRef(requestIdentity);
  const fallbackRowsRef = useRef(fallbackRows);
  fallbackRowsRef.current = fallbackRows;

  // Reset synchronously during prop reconciliation so a new endpoint can never issue
  // its first request with the previous endpoint's paging or control state.
  if (requestIdentityRef.current !== requestIdentity) {
    requestIdentityRef.current = requestIdentity;
    requestSequence.current += 1;
    setRows(fallbackRows);
    setTotal(manifestTotal);
    setOffsetState(0);
    setQueryState('');
    setDebouncedQuery('');
    setSortState(requestKind === 'region' ? 'locus' : 'pvalue');
    setDirection('ascending');
    setLoading(true);
    setError(null);
    setRetryCount(0);
  }

  useEffect(() => {
    if (query === debouncedQuery) return;
    const timeout = window.setTimeout(() => setDebouncedQuery(query), debounceMs);
    return () => window.clearTimeout(timeout);
  }, [debounceMs, debouncedQuery, query]);

  useEffect(() => {
    const sequence = ++requestSequence.current;
    if (query !== debouncedQuery) {
      setLoading(true);
      setError(null);
      return;
    }

    const controller = new AbortController();
    const requestUrl = associationTablePageUrl(
      new URL(tableApiHref),
      offset,
      limit,
      sort,
      direction,
      debouncedQuery,
      requestKind,
    );
    setLoading(true);
    setError(null);

    void fetch(requestUrl, {
      signal: controller.signal,
      headers: { Accept: 'application/json' },
    }).then(async (response) => {
      if (!response.ok) throw new Error(response.status === 429 || response.status === 503 ? 'busy' : 'http');
      let input: unknown;
      try {
        input = await response.json();
      } catch {
        throw new Error('invalid');
      }
      let page;
      try {
        page = parseAssociationPage(input);
      } catch {
        throw new Error('invalid');
      }
      if (
        page.dataset_id !== expectedDatasetId
        || page.offset !== offset
        || page.limit !== limit
        || (requestKind === 'region' && (page.sort != null || page.direction != null || page.search != null))
        || (requestKind === 'table' && page.sort !== sort)
        || (requestKind === 'table' && page.direction !== direction)
        || (requestKind === 'table' && (page.search ?? '') !== debouncedQuery)
        || page.points.length > limit
        || page.total < page.offset + page.points.length
      ) {
        throw new Error('invalid');
      }
      if (controller.signal.aborted || sequence !== requestSequence.current) return;
      setRows(page.points);
      setTotal(page.total);
      setLoading(false);
    }).catch((requestError: unknown) => {
      if (controller.signal.aborted || sequence !== requestSequence.current) return;
      setRows(fallbackRowsRef.current);
      setTotal(fallbackRowsRef.current.length);
      setLoading(false);
      setError(
        requestError instanceof Error && requestError.message === 'busy'
          ? 'Association table service is busy. Showing inline top hits instead; retry shortly.'
          : requestError instanceof Error && requestError.message === 'http'
            ? 'Association table request failed. Showing inline top hits instead.'
            : 'Association table returned invalid or unavailable data. Showing inline top hits instead.',
      );
    });

    return () => controller.abort();
  }, [debouncedQuery, direction, expectedDatasetId, limit, offset, query, requestKind, retryCount, sort, tableApiHref]);

  const setQuery = useCallback((nextQuery: string) => {
    if (requestKind === 'region') return;
    setQueryState(boundedSearch(nextQuery));
    setOffsetState(0);
  }, [requestKind]);

  const setSort = useCallback((nextSort: AssociationTableSortV1, nextDirection: AssociationSortDirectionV1) => {
    if (requestKind === 'region') return;
    setSortState(nextSort);
    setDirection(nextDirection);
    setOffsetState(0);
  }, [requestKind]);

  const setOffset = useCallback((nextOffset: number) => {
    const safeOffset = Number.isFinite(nextOffset)
      ? Math.max(0, Math.min(Number.MAX_SAFE_INTEGER, Math.trunc(nextOffset)))
      : 0;
    setOffsetState(safeOffset);
  }, []);

  const retry = useCallback(() => setRetryCount((value) => value + 1), []);

  return {
    rows,
    total,
    offset,
    limit,
    query,
    sort,
    direction,
    loading,
    error,
    setQuery,
    setSort,
    setOffset,
    retry,
  };
}
