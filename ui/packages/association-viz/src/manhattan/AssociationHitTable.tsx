import { useEffect, useMemo, useRef, useState } from 'react';
import type {
  AssociationPointV1,
  AssociationSortDirectionV1,
  AssociationTableSortV1,
} from '../contracts/types.js';
import { compareCanonicalContigs, stableHitKey } from './layout.js';

interface AssociationHitTableSharedProps {
  /** Fixed locus mode removes unsupported search/sort controls and orders local fallback rows by locus. */
  controls?: 'full' | 'fixed-locus';
  selectedHitKey?: string | null;
  onSelectedHitChange?: (hit: AssociationPointV1) => void;
  onHitHover?: (hit: AssociationPointV1 | null) => void;
  onOpenLocus?: (hit: AssociationPointV1) => void;
  height?: number;
  rowHeight?: number;
  className?: string;
}

/** Existing bounded, client-side table behavior. */
export interface AssociationHitTableLocalProps extends AssociationHitTableSharedProps {
  mode?: 'local';
  hits: readonly AssociationPointV1[];
}

/** Fetch-independent controlled mode for one server-ordered page. */
export interface AssociationHitTableControlledProps extends AssociationHitTableSharedProps {
  mode: 'server';
  rows: readonly AssociationPointV1[];
  total: number;
  offset: number;
  limit: number;
  query: string;
  sort: AssociationTableSortV1;
  direction: AssociationSortDirectionV1;
  loading: boolean;
  error?: string | null;
  onQueryChange: (query: string) => void;
  onSortChange: (
    sort: AssociationTableSortV1,
    direction: AssociationSortDirectionV1,
  ) => void;
  onOffsetChange: (offset: number) => void;
}

export type AssociationHitTableProps =
  | AssociationHitTableLocalProps
  | AssociationHitTableControlledProps;

function compareLocus(left: AssociationPointV1, right: AssociationPointV1): number {
  return compareCanonicalContigs(left.contig, right.contig)
    || left.position - right.position
    || left.source_ordinal - right.source_ordinal;
}

function searchableText(hit: AssociationPointV1): string {
  return [hit.contig, hit.position, hit.ref_allele, hit.alt_allele, hit.gene_symbol, hit.consequence, hit.series]
    .filter((value) => value != null)
    .join(' ')
    .toLowerCase();
}

function formatVariant(hit: AssociationPointV1): string {
  const alleles = hit.ref_allele && hit.alt_allele ? ` ${hit.ref_allele}>${hit.alt_allele}` : '';
  return `${hit.contig}:${hit.position.toLocaleString()}${alleles}`;
}

export function AssociationHitTable(props: AssociationHitTableProps) {
  const {
    selectedHitKey,
    onSelectedHitChange,
    onHitHover,
    onOpenLocus,
    height = 320,
    rowHeight = 42,
    className,
    controls = 'full',
  } = props;
  const serverMode = props.mode === 'server';
  const fixedLocusOrder = controls === 'fixed-locus';
  const viewportRef = useRef<HTMLDivElement>(null);
  const pendingFocusKey = useRef<string | null>(null);
  const [localQuery, setLocalQuery] = useState('');
  const [localSort, setLocalSort] = useState<AssociationTableSortV1>('pvalue');
  const [localDirection, setLocalDirection] = useState<AssociationSortDirectionV1>('ascending');
  const [scrollTop, setScrollTop] = useState(0);
  const [activeKey, setActiveKey] = useState<string | null>(selectedHitKey ?? null);

  const query = fixedLocusOrder ? '' : serverMode ? props.query : localQuery;
  const sort = fixedLocusOrder ? 'locus' : serverMode ? props.sort : localSort;
  const direction = fixedLocusOrder ? 'ascending' : serverMode ? props.direction : localDirection;
  const sourceRows = serverMode ? props.rows : props.hits;
  const rows = useMemo(() => {
    if (serverMode) return [...sourceRows];
    const normalizedQuery = query.trim().toLowerCase();
    const filtered = normalizedQuery
      ? sourceRows.filter((hit) => searchableText(hit).includes(normalizedQuery))
      : [...sourceRows];
    filtered.sort((left, right) => {
      const order = sort === 'pvalue'
        ? left.pvalue - right.pvalue || right.neg_log10_p - left.neg_log10_p || compareLocus(left, right)
        : compareLocus(left, right);
      return direction === 'ascending' ? order : -order;
    });
    return filtered;
  }, [direction, query, serverMode, sort, sourceRows]);

  useEffect(() => {
    if (selectedHitKey && rows.some((hit) => stableHitKey(hit) === selectedHitKey)) {
      setActiveKey(selectedHitKey);
    }
  }, [rows, selectedHitKey]);

  useEffect(() => {
    if (!activeKey || rows.some((hit) => stableHitKey(hit) === activeKey)) return;
    setActiveKey(rows[0] ? stableHitKey(rows[0]) : null);
  }, [activeKey, rows]);

  useEffect(() => {
    if (!selectedHitKey || !viewportRef.current) return;
    const index = rows.findIndex((hit) => stableHitKey(hit) === selectedHitKey);
    if (index < 0) return;
    const top = index * rowHeight;
    const bottom = top + rowHeight;
    const viewport = viewportRef.current;
    let nextScrollTop = viewport.scrollTop;
    if (top < viewport.scrollTop) nextScrollTop = top;
    else if (bottom > viewport.scrollTop + height) nextScrollTop = bottom - height;
    if (nextScrollTop !== viewport.scrollTop) {
      viewport.scrollTop = nextScrollTop;
      setScrollTop(nextScrollTop);
    }
  }, [height, rowHeight, rows, selectedHitKey]);

  const serverOffset = serverMode ? props.offset : null;
  useEffect(() => {
    if (serverOffset === null) return;
    if (viewportRef.current) viewportRef.current.scrollTop = 0;
    setScrollTop(0);
  }, [serverOffset]);

  const overscan = 3;
  const start = Math.max(0, Math.floor(scrollTop / rowHeight) - overscan);
  const visibleCount = Math.ceil(height / rowHeight) + overscan * 2;
  const end = Math.min(rows.length, start + visibleCount);
  const visibleRows = rows.slice(start, end);

  useEffect(() => {
    const key = pendingFocusKey.current;
    if (!key) return;
    const row = viewportRef.current?.querySelector<HTMLElement>(`[data-hit-key="${CSS.escape(key)}"]`);
    if (row) {
      pendingFocusKey.current = null;
      row.focus();
    }
  }, [activeKey, start, end]);

  const resetScroll = () => {
    if (viewportRef.current) viewportRef.current.scrollTop = 0;
    setScrollTop(0);
  };

  const updateQuery = (nextQuery: string) => {
    resetScroll();
    if (serverMode) {
      props.onQueryChange(nextQuery);
      props.onOffsetChange(0);
    } else {
      setLocalQuery(nextQuery);
    }
  };

  const updateSort = (field: AssociationTableSortV1) => {
    const nextDirection = sort === field
      ? direction === 'ascending' ? 'descending' : 'ascending'
      : 'ascending';
    resetScroll();
    if (serverMode) {
      props.onSortChange(field, nextDirection);
      props.onOffsetChange(0);
    } else {
      setLocalSort(field);
      setLocalDirection(nextDirection);
    }
  };

  const moveActive = (current: AssociationPointV1, movement: number) => {
    const index = rows.findIndex((hit) => stableHitKey(hit) === stableHitKey(current));
    const nextIndex = Math.max(0, Math.min(rows.length - 1, index + movement));
    const next = rows[nextIndex];
    if (!next) return;
    const key = stableHitKey(next);
    pendingFocusKey.current = key;
    setActiveKey(key);
    const viewport = viewportRef.current;
    if (viewport) {
      const top = nextIndex * rowHeight;
      const bottom = top + rowHeight;
      let nextScrollTop = viewport.scrollTop;
      if (top < viewport.scrollTop) nextScrollTop = top;
      else if (bottom > viewport.scrollTop + height) nextScrollTop = bottom - height;
      if (nextScrollTop !== viewport.scrollTop) {
        viewport.scrollTop = nextScrollTop;
        setScrollTop(nextScrollTop);
      }
    }
    onSelectedHitChange?.(next);
  };

  const classes = ['association-table-section', className].filter(Boolean).join(' ');
  const rangeStart = serverMode && rows.length > 0 ? props.offset + 1 : 0;
  const rangeEnd = serverMode && rows.length > 0
    ? Math.min(props.total, props.offset + rows.length)
    : 0;
  const pageStatus = serverMode
    ? `${rangeStart.toLocaleString()}–${rangeEnd.toLocaleString()} of ${props.total.toLocaleString()} hits`
    : `${rows.length.toLocaleString()} of ${props.hits.length.toLocaleString()} hits`;

  return (
    <section className={classes} aria-label="Top association hits" aria-busy={serverMode && props.loading}>
      <div className="association-table-toolbar">
        {fixedLocusOrder ? (
          <span className="association-table-fixed-order">Ordered by locus</span>
        ) : (
          <label>
            <span>Search hits</span>
            <input
              type="search"
              value={query}
              onChange={(event) => updateQuery(event.target.value)}
              placeholder="Variant, gene, consequence…"
            />
          </label>
        )}
        <span role="status" aria-live="polite" aria-atomic="true">{pageStatus}</span>
      </div>
      {serverMode && (
        <>
          <span className="association-visually-hidden" aria-live="polite" aria-atomic="true">
            {props.loading ? 'Loading association hits.' : `Association hits loaded. ${pageStatus}.`}
          </span>
          {props.error && <div className="association-table-error" role="alert">{props.error}</div>}
        </>
      )}
      <div
        ref={viewportRef}
        className="association-table-viewport"
        style={{ height }}
        onScroll={(event) => setScrollTop(event.currentTarget.scrollTop)}
      >
        <table className="association-hit-table">
          <thead>
            <tr>
              {fixedLocusOrder ? (
                <th>Variant <span className="association-visually-hidden">(fixed locus order)</span></th>
              ) : (
                <th aria-sort={sort === 'locus' ? direction : 'none'}>
                  <button type="button" onClick={() => updateSort('locus')}>Variant</button>
                </th>
              )}
              <th>Gene</th>
              <th>Consequence</th>
              {fixedLocusOrder ? (
                <th>P-value</th>
              ) : (
                <th aria-sort={sort === 'pvalue' ? direction : 'none'}>
                  <button type="button" onClick={() => updateSort('pvalue')}>P-value</button>
                </th>
              )}
              <th>Beta</th>
              <th>AF</th>
              <th><span className="association-visually-hidden">Actions</span></th>
            </tr>
          </thead>
          <tbody>
            {start > 0 && <tr aria-hidden="true" className="association-spacer"><td colSpan={7} style={{ height: start * rowHeight }} /></tr>}
            {visibleRows.map((hit) => {
              const key = stableHitKey(hit);
              const selected = key === selectedHitKey;
              const active = key === (activeKey ?? selectedHitKey ?? (rows[0] ? stableHitKey(rows[0]) : null));
              return (
                <tr
                  key={key}
                  data-hit-key={key}
                  className={selected ? 'is-selected' : ''}
                  aria-selected={selected}
                  tabIndex={active ? 0 : -1}
                  style={{ height: rowHeight }}
                  onFocus={() => setActiveKey(key)}
                  onMouseEnter={() => onHitHover?.(hit)}
                  onMouseLeave={() => onHitHover?.(null)}
                  onClick={() => onSelectedHitChange?.(hit)}
                  onKeyDown={(event) => {
                    if (event.target !== event.currentTarget) return;
                    if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
                      event.preventDefault();
                      moveActive(hit, event.key === 'ArrowDown' ? 1 : -1);
                    } else if (event.key === 'Enter' || event.key === ' ') {
                      event.preventDefault();
                      onSelectedHitChange?.(hit);
                    } else if (event.key.toLowerCase() === 'o') onOpenLocus?.(hit);
                  }}
                >
                  <td>{formatVariant(hit)}</td>
                  <td>{hit.gene_symbol || '—'}</td>
                  <td>{hit.consequence?.replace(/_/g, ' ') || '—'}</td>
                  <td>{hit.pvalue === 0 ? `−log₁₀ ${hit.neg_log10_p}` : hit.pvalue.toExponential(2)}</td>
                  <td>{hit.beta?.toPrecision(3) ?? '—'}</td>
                  <td>{hit.allele_frequency?.toPrecision(3) ?? '—'}</td>
                  <td>
                    <button
                      type="button"
                      onClick={(event) => { event.stopPropagation(); onOpenLocus?.(hit); }}
                      aria-label={`Open locus ${hit.contig}:${hit.position}`}
                    >Open</button>
                  </td>
                </tr>
              );
            })}
            {end < rows.length && <tr aria-hidden="true" className="association-spacer"><td colSpan={7} style={{ height: (rows.length - end) * rowHeight }} /></tr>}
          </tbody>
        </table>
        {rows.length === 0 && !serverMode && <div className="association-table-empty">No matching hits.</div>}
        {rows.length === 0 && serverMode && !props.loading && !props.error && (
          <div className="association-table-empty">No matching hits.</div>
        )}
      </div>
      {serverMode && (
        <nav className="association-table-pagination" aria-label="Association hit pages">
          <button
            type="button"
            disabled={props.loading || props.offset === 0}
            onClick={() => props.onOffsetChange(Math.max(0, props.offset - props.limit))}
          >Previous</button>
          <span>{pageStatus}</span>
          <button
            type="button"
            disabled={props.loading || rows.length === 0 || props.offset + rows.length >= props.total}
            onClick={() => props.onOffsetChange(props.offset + props.limit)}
          >Next</button>
        </nav>
      )}
    </section>
  );
}
