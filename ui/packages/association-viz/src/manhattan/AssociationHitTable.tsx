import { useEffect, useMemo, useRef, useState } from 'react';
import type { AssociationPointV1 } from '../contracts/types.js';
import { compareCanonicalContigs, stableHitKey } from './layout.js';

export interface AssociationHitTableProps {
  hits: readonly AssociationPointV1[];
  selectedHitKey?: string | null;
  onSelectedHitChange?: (hit: AssociationPointV1) => void;
  onHitHover?: (hit: AssociationPointV1 | null) => void;
  onOpenLocus?: (hit: AssociationPointV1) => void;
  height?: number;
  rowHeight?: number;
  className?: string;
}

type SortField = 'pvalue' | 'locus';
type SortDirection = 'ascending' | 'descending';

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

export function AssociationHitTable({
  hits,
  selectedHitKey,
  onSelectedHitChange,
  onHitHover,
  onOpenLocus,
  height = 320,
  rowHeight = 42,
  className,
}: AssociationHitTableProps) {
  const viewportRef = useRef<HTMLDivElement>(null);
  const pendingFocusKey = useRef<string | null>(null);
  const [query, setQuery] = useState('');
  const [sortField, setSortField] = useState<SortField>('pvalue');
  const [sortDirection, setSortDirection] = useState<SortDirection>('ascending');
  const [scrollTop, setScrollTop] = useState(0);
  const [activeKey, setActiveKey] = useState<string | null>(selectedHitKey ?? null);
  const rows = useMemo(() => {
    const normalizedQuery = query.trim().toLowerCase();
    const filtered = normalizedQuery ? hits.filter((hit) => searchableText(hit).includes(normalizedQuery)) : [...hits];
    filtered.sort((left, right) => {
      const order = sortField === 'pvalue'
        ? left.pvalue - right.pvalue || right.neg_log10_p - left.neg_log10_p || compareLocus(left, right)
        : compareLocus(left, right);
      return sortDirection === 'ascending' ? order : -order;
    });
    return filtered;
  }, [hits, query, sortDirection, sortField]);

  useEffect(() => {
    if (selectedHitKey) setActiveKey(selectedHitKey);
  }, [selectedHitKey]);

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

  const updateSort = (field: SortField) => {
    if (sortField === field) setSortDirection((value) => value === 'ascending' ? 'descending' : 'ascending');
    else {
      setSortField(field);
      setSortDirection('ascending');
    }
  };

  const moveActive = (current: AssociationPointV1, direction: number) => {
    const index = rows.findIndex((hit) => stableHitKey(hit) === stableHitKey(current));
    const nextIndex = Math.max(0, Math.min(rows.length - 1, index + direction));
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
  return (
    <section className={classes} aria-label="Top association hits">
      <div className="association-table-toolbar">
        <label>
          <span>Search hits</span>
          <input
            type="search"
            value={query}
            onChange={(event) => { setQuery(event.target.value); setScrollTop(0); }}
            placeholder="Variant, gene, consequence…"
          />
        </label>
        <span role="status">{rows.length.toLocaleString()} of {hits.length.toLocaleString()} hits</span>
      </div>
      <div
        ref={viewportRef}
        className="association-table-viewport"
        style={{ height }}
        onScroll={(event) => setScrollTop(event.currentTarget.scrollTop)}
      >
        <table className="association-hit-table">
          <thead>
            <tr>
              <th aria-sort={sortField === 'locus' ? sortDirection : 'none'}>
                <button type="button" onClick={() => updateSort('locus')}>Variant</button>
              </th>
              <th>Gene</th>
              <th>Consequence</th>
              <th aria-sort={sortField === 'pvalue' ? sortDirection : 'none'}>
                <button type="button" onClick={() => updateSort('pvalue')}>P-value</button>
              </th>
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
        {rows.length === 0 && <div className="association-table-empty">No matching hits.</div>}
      </div>
    </section>
  );
}
