import { useState, useMemo, useRef } from 'react';
import { useVirtualizer } from '@tanstack/react-virtual';
import { useAtom, useAtomValue, useSetAtom } from 'jotai';
import {
  catalogAtom,
  summaryAtom,
  batchAtom,
  libraryFilterAtom,
  libraryAncestryFilterAtom,
  libraryTraitTypeFilterAtom,
  libraryAssetFilterAtom,
  libraryStatusFilterAtom,
  librarySelectedCategoriesAtom,
  librarySelectedIdsAtom,
  librarySortKeyAtom,
  librarySortDirAtom,
  libraryRandomSampleAtom,
  libraryLimitCountAtom,
  fetchDashboardDataAtom
} from '../../atoms/dashboardAtoms';
import '../panels.css';

const selectStyle: React.CSSProperties = {
  padding: '4px 8px',
  background: 'var(--bg)',
  color: 'var(--text)',
  border: '1px solid var(--border)',
  borderRadius: '4px',
  fontSize: '11px',
};

type SortKey = 'status' | 'id' | 'ancestry' | 'description' | 'trait_type' | 'cases' | 'assets';

export const PhenotypeLibraryPanel: React.FC = () => {
  const catalog = useAtomValue(catalogAtom);
  const summary = useAtomValue(summaryAtom);

  const [filter, setFilter] = useAtom(libraryFilterAtom);
  const [ancestryFilter, setAncestryFilter] = useAtom(libraryAncestryFilterAtom);
  const [traitTypeFilter, setTraitTypeFilter] = useAtom(libraryTraitTypeFilterAtom);
  const [assetFilter, setAssetFilter] = useAtom(libraryAssetFilterAtom);
  const [statusFilter, setStatusFilter] = useAtom(libraryStatusFilterAtom);
  const [selectedCategories, setSelectedCategories] = useAtom(librarySelectedCategoriesAtom);
  const [selectedIds, setSelectedIds] = useAtom(librarySelectedIdsAtom);
  const [sortKey, setSortKey] = useAtom(librarySortKeyAtom);
  const [sortDir, setSortDir] = useAtom(librarySortDirAtom);
  const [randomSamplePct, setRandomSamplePct] = useAtom(libraryRandomSampleAtom);
  const [limitCount, setLimitCount] = useAtom(libraryLimitCountAtom);

  const fetchDashboardData = useSetAtom(fetchDashboardDataAtom);

  const [assetsPath, setAssetsPath] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleSort = (key: SortKey) => {
    if (sortKey === key) {
      setSortDir(d => d === 'asc' ? 'desc' : 'asc');
    } else {
      setSortKey(key);
      setSortDir('asc');
    }
  };

  const sortIndicator = (key: SortKey) =>
    sortKey === key ? (sortDir === 'asc' ? ' ▲' : ' ▼') : '';

  // Derive unique filter options from catalog
  const ancestries = useMemo(() => [...new Set(catalog.map(c => c.ancestry))].sort(), [catalog]);
  const traitTypes = useMemo(() => [...new Set(catalog.map(c => c.trait_type).filter(Boolean))].sort() as string[], [catalog]);

  const categories = useMemo(() => {
    const counts: Record<string, number> = {};
    catalog.forEach(c => {
      if (c.category) counts[c.category] = (counts[c.category] || 0) + 1;
    });
    return Object.entries(counts).sort((a, b) => a[0].localeCompare(b[0]));
  }, [catalog]);

  const toggleCategory = (cat: string) => {
    const next = new Set(selectedCategories);
    if (next.has(cat)) next.delete(cat);
    else next.add(cat);
    setSelectedCategories(next);
  };

  const handleLoadAssets = async () => {
    if (!assetsPath.trim()) return;
    setLoading(true);
    setError(null);
    try {
      const res = await fetch('/api/catalog/load', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ assets_json: assetsPath }),
      });
      const data = await res.json();
      if (!data.success) setError('Failed: ' + data.error);
    } catch (e) {
      console.error(e);
      setError('Network error connecting to server');
    } finally {
      setLoading(false);
    }
  };

  const handleAction = async (endpoint: string) => {
    if (selectedIds.size === 0) return;
    setError(null);

    const phenotypes = Array.from(selectedIds).map(key => {
      const [id, ancestry] = key.split('::');
      return [id, ancestry];
    });

    try {
      const res = await fetch(endpoint, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ phenotypes }),
      });
      const data = await res.json();
      if (data.success) {
        setSelectedIds(new Set());
        fetchDashboardData();
      } else {
        setError('Action failed: ' + data.error);
      }
    } catch (e) {
      console.error(e);
      setError('Error communicating with server');
    }
  };

  const batch = useAtomValue(batchAtom);

  const filteredCatalog = useMemo(() => {
    // Map live batch status onto static catalog entries
    let result = catalog.map(c => {
      const activeBatchItem = batch?.phenotypes?.find(p => p.id.endsWith(`/${c.ancestry}/${c.id}`));
      return {
        ...c,
        status: activeBatchItem ? activeBatchItem.stage : c.status
      };
    });

    if (ancestryFilter) {
      result = result.filter(c => c.ancestry === ancestryFilter);
    }
    if (traitTypeFilter) {
      result = result.filter(c => c.trait_type === traitTypeFilter);
    }
    if (statusFilter) {
      result = result.filter(c => c.status === statusFilter);
    }
    if (selectedCategories.size > 0) {
      result = result.filter(c => c.category && selectedCategories.has(c.category));
    }
    if (assetFilter) {
      result = result.filter(c => {
        if (assetFilter === 'exome') return c.has_exome;
        if (assetFilter === 'genome') return c.has_genome;
        if (assetFilter === 'burden') return c.has_gene_burden;
        if (assetFilter === 'both') return c.has_exome && c.has_genome;
        return true;
      });
    }
    if (filter) {
      const lower = filter.toLowerCase();
      result = result.filter(c =>
        c.id.toLowerCase().includes(lower) ||
        (c.description && c.description.toLowerCase().includes(lower))
      );
    }

    // Sort — active statuses always float to top
    const statusPriority = (s: string) => {
      switch (s) {
        case 'scanning': return 0;
        case 'aggregating': return 1;
        case 'ingesting': return 2;
        case 'queued': return 3;
        case 'failed': return 4;
        case 'completed': return 5;
        case 'ingested': return 6;
        default: return 7; // idle
      }
    };

    const sorted = [...result].sort((a, b) => {
      // Active statuses first, always
      const pa = statusPriority(a.status);
      const pb = statusPriority(b.status);
      if (pa !== pb) return pa - pb;

      let cmp = 0;
      switch (sortKey) {
        case 'status':
          cmp = 0; // already sorted by priority above
          break;
        case 'id':
          cmp = a.id.localeCompare(b.id);
          break;
        case 'ancestry':
          cmp = a.ancestry.localeCompare(b.ancestry);
          break;
        case 'description':
          cmp = (a.description || '').localeCompare(b.description || '');
          break;
        case 'trait_type':
          cmp = (a.trait_type || '').localeCompare(b.trait_type || '');
          break;
        case 'cases':
          cmp = (a.n_cases || 0) - (b.n_cases || 0);
          break;
        case 'assets': {
          const assetCount = (e: typeof a) => (e.has_exome ? 1 : 0) + (e.has_genome ? 1 : 0) + (e.has_gene_burden ? 1 : 0);
          cmp = assetCount(a) - assetCount(b);
          break;
        }
      }
      return sortDir === 'asc' ? cmp : -cmp;
    });

    // Apply Random % Sample
    let sampled = sorted;
    if (randomSamplePct < 100) {
      sampled = sorted.filter(c => {
        let hash = 0;
        const str = `${c.id}::${c.ancestry}`;
        for (let i = 0; i < str.length; i++) {
          hash = ((hash << 5) - hash) + str.charCodeAt(i);
          hash |= 0; // Convert to 32bit integer
        }
        return (Math.abs(hash) % 100) < randomSamplePct;
      });
    }

    // Apply Limit
    const parsedLimit = parseInt(limitCount, 10);
    if (!isNaN(parsedLimit) && parsedLimit > 0) {
      sampled = sampled.slice(0, parsedLimit);
    }

    return sampled;
  }, [catalog, batch, filter, ancestryFilter, traitTypeFilter, assetFilter, statusFilter, selectedCategories, sortKey, sortDir, randomSamplePct, limitCount]);

  const toggleSelect = (key: string) => {
    const next = new Set(selectedIds);
    if (next.has(key)) next.delete(key);
    else next.add(key);
    setSelectedIds(next);
  };

  const allVisibleSelected = filteredCatalog.length > 0 && filteredCatalog.every(c => selectedIds.has(`${c.id}::${c.ancestry}`));

  const toggleAll = () => {
    if (allVisibleSelected) {
      setSelectedIds(new Set());
    } else {
      setSelectedIds(new Set(filteredCatalog.map(c => `${c.id}::${c.ancestry}`)));
    }
  };

  const isBusy = summary && !summary.idle;

  const thStyle: React.CSSProperties = { cursor: 'pointer', userSelect: 'none', whiteSpace: 'nowrap' };

  const parentRef = useRef<HTMLDivElement>(null);

  const rowVirtualizer = useVirtualizer({
    count: filteredCatalog.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => 35,
    overscan: 10,
  });

  const virtualItems = rowVirtualizer.getVirtualItems();
  const paddingTop = virtualItems.length > 0 ? virtualItems[0].start : 0;
  const paddingBottom = virtualItems.length > 0
    ? rowVirtualizer.getTotalSize() - virtualItems[virtualItems.length - 1].end
    : 0;

  return (
    <div className="panel-container" style={{ display: 'flex', flexDirection: 'column' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '8px', alignItems: 'center' }}>
        <h2 className="panel-title" style={{ margin: 0 }}>Phenotype Library ({filteredCatalog.length}/{catalog.length})</h2>
        <div style={{ display: 'flex', gap: '8px' }}>
          <button
            className="btn btn-primary"
            disabled={selectedIds.size === 0}
            onClick={() => handleAction('/api/catalog/process')}
          >
            Process Selected ({selectedIds.size})
          </button>
          <button
            className="btn btn-primary"
            style={{ background: 'var(--orange)' }}
            disabled={selectedIds.size === 0 || !!isBusy}
            title={isBusy ? "Coordinator is busy" : ""}
            onClick={() => handleAction('/api/catalog/ingest')}
          >
            Ingest Selected ({selectedIds.size})
          </button>
        </div>
      </div>

      {/* Filter controls */}
      <div style={{ display: 'flex', gap: '8px', marginBottom: '8px', flexWrap: 'wrap' }}>
        <input
          value={filter}
          onChange={e => setFilter(e.target.value)}
          placeholder="Search by ID or description..."
          style={{ ...selectStyle, width: '200px' }}
        />
        <select value={ancestryFilter} onChange={e => setAncestryFilter(e.target.value)} style={selectStyle}>
          <option value="">All ancestries</option>
          {ancestries.map(a => <option key={a} value={a}>{a}</option>)}
        </select>
        {traitTypes.length > 0 && (
          <select value={traitTypeFilter} onChange={e => setTraitTypeFilter(e.target.value)} style={selectStyle}>
            <option value="">All trait types</option>
            {traitTypes.map(t => <option key={t} value={t}>{t}</option>)}
          </select>
        )}
        <select value={assetFilter} onChange={e => setAssetFilter(e.target.value)} style={selectStyle}>
          <option value="">All assets</option>
          <option value="exome">Has exome</option>
          <option value="genome">Has genome</option>
          <option value="both">Exome + Genome</option>
          <option value="burden">Has gene burden</option>
        </select>
        <select value={statusFilter} onChange={e => setStatusFilter(e.target.value)} style={selectStyle}>
          <option value="">All statuses</option>
          <option value="idle">Idle</option>
          <option value="queued">Queued</option>
          <option value="scanning">Scanning</option>
          <option value="aggregating">Aggregating</option>
          <option value="completed">Completed</option>
          <option value="ingesting">Ingesting</option>
          <option value="ingested">Ingested</option>
          <option value="failed">Failed</option>
        </select>
      </div>

      {categories.length > 0 && (
        <div style={{ display: 'flex', gap: '6px', marginBottom: '12px', flexWrap: 'wrap', alignItems: 'center' }}>
          <span style={{ fontSize: '10px', color: 'var(--text-dim)', marginRight: '4px' }}>
            Categories ({selectedCategories.size === 0 ? 'All' : `${selectedCategories.size} of ${categories.length}`}):
          </span>
          <button
            style={{ ...selectStyle, padding: '2px 8px', cursor: 'pointer' }}
            onClick={() => setSelectedCategories(new Set(categories.map(c => c[0])))}
          >All</button>
          <button
            style={{ ...selectStyle, padding: '2px 8px', cursor: 'pointer' }}
            onClick={() => setSelectedCategories(new Set())}
          >None</button>

          <div style={{ width: '1px', height: '14px', background: 'var(--border)', margin: '0 4px' }} />

          {categories.map(([cat, count]) => {
            const isSelected = selectedCategories.size === 0 || selectedCategories.has(cat);
            return (
              <button
                key={cat}
                onClick={() => toggleCategory(cat)}
                style={{
                  ...selectStyle,
                  background: isSelected ? 'rgba(57, 197, 207, 0.15)' : 'transparent',
                  borderColor: isSelected ? 'var(--cyan)' : 'var(--border)',
                  color: isSelected ? 'var(--cyan)' : 'var(--text-dim)',
                  padding: '2px 8px',
                  cursor: 'pointer'
                }}
              >
                {cat} <span style={{ opacity: 0.6 }}>({count})</span>
              </button>
            );
          })}
        </div>
      )}

      {/* Subsetting controls for progressive scale-up */}
      <div style={{ display: 'flex', gap: '16px', marginBottom: '12px', alignItems: 'center', background: 'rgba(255, 255, 255, 0.02)', padding: '6px 12px', borderRadius: '4px', border: '1px solid var(--border)' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          <span style={{ fontSize: '11px', color: 'var(--text-dim)' }}>Random Sample:</span>
          <input
            type="range"
            min="1"
            max="100"
            value={randomSamplePct}
            onChange={e => setRandomSamplePct(Number(e.target.value))}
            style={{ width: '100px', cursor: 'pointer' }}
          />
          <span style={{ fontSize: '11px', width: '32px', textAlign: 'right', color: 'var(--cyan)' }}>{randomSamplePct}%</span>
        </div>

        <div style={{ width: '1px', height: '14px', background: 'var(--border)' }} />

        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          <span style={{ fontSize: '11px', color: 'var(--text-dim)' }}>Limit:</span>
          <input
            type="number"
            min="1"
            placeholder="No limit"
            value={limitCount}
            onChange={e => setLimitCount(e.target.value)}
            style={{ ...selectStyle, width: '80px', padding: '2px 6px' }}
          />
        </div>
      </div>

      {error && (
        <div style={{ padding: '8px', marginBottom: '8px', background: 'rgba(248, 81, 73, 0.1)', color: 'var(--red)', border: '1px solid var(--red)', borderRadius: '4px', fontSize: '11px' }}>
          {error}
        </div>
      )}

      <div ref={parentRef} style={{ flex: 1, overflowY: 'auto' }}>
        <table className="data-table">
          <thead style={{ position: 'sticky', top: 0, zIndex: 1, backgroundColor: 'var(--surface)' }}>
            <tr>
              <th style={{ width: '30px', textAlign: 'center' }}>
                <input type="checkbox" checked={allVisibleSelected} onChange={toggleAll} />
              </th>
              <th style={thStyle} onClick={() => handleSort('status')}>Status{sortIndicator('status')}</th>
              <th style={thStyle} onClick={() => handleSort('id')}>Phenotype ID{sortIndicator('id')}</th>
              <th style={thStyle} onClick={() => handleSort('ancestry')}>Ancestry{sortIndicator('ancestry')}</th>
              <th style={thStyle} onClick={() => handleSort('description')}>Description{sortIndicator('description')}</th>
              <th style={thStyle} onClick={() => handleSort('trait_type')}>Trait Type{sortIndicator('trait_type')}</th>
              <th style={thStyle} onClick={() => handleSort('cases')}>Cases / Controls{sortIndicator('cases')}</th>
              <th style={thStyle} onClick={() => handleSort('assets')}>Assets{sortIndicator('assets')}</th>
            </tr>
          </thead>
          <tbody>
            {paddingTop > 0 && (
              <tr>
                <td colSpan={8} style={{ height: paddingTop, padding: 0, border: 0 }}></td>
              </tr>
            )}
            {virtualItems.map(virtualRow => {
              const entry = filteredCatalog[virtualRow.index];
              const key = `${entry.id}::${entry.ancestry}`;
              const isSelected = selectedIds.has(key);

              return (
                <tr
                  key={key}
                  data-index={virtualRow.index}
                  ref={rowVirtualizer.measureElement}
                  style={{ background: isSelected ? 'rgba(88, 166, 255, 0.1)' : undefined }}
                  onClick={() => toggleSelect(key)}
                >
                  <td style={{ textAlign: 'center' }} onClick={e => e.stopPropagation()}>
                    <input type="checkbox" checked={isSelected} onChange={() => toggleSelect(key)} />
                  </td>
                  <td>
                    {entry.status !== 'idle' && (
                      <span className={`stage-badge ${entry.status}`}>{entry.status}</span>
                    )}
                  </td>
                  <td style={{ color: 'var(--cyan)', fontWeight: 600 }}>{entry.id}</td>
                  <td>{entry.ancestry}</td>
                  <td>{entry.description || '--'}</td>
                  <td>{entry.trait_type || '--'}</td>
                  <td>{entry.n_cases ? `${entry.n_cases} / ${entry.n_controls || 0}` : '--'}</td>
                  <td>
                    <div style={{ display: 'flex', gap: '4px' }}>
                      {entry.has_exome && <span style={{ background: 'var(--cyan)', color: '#000', padding: '2px 4px', borderRadius: '3px', fontSize: '9px', fontWeight: 'bold' }}>E</span>}
                      {entry.has_genome && <span style={{ background: 'var(--cyan)', color: '#000', padding: '2px 4px', borderRadius: '3px', fontSize: '9px', fontWeight: 'bold' }}>G</span>}
                      {entry.has_gene_burden && <span style={{ background: 'var(--purple)', color: '#000', padding: '2px 4px', borderRadius: '3px', fontSize: '9px', fontWeight: 'bold' }}>B</span>}
                    </div>
                  </td>
                </tr>
              );
            })}
            {paddingBottom > 0 && (
              <tr>
                <td colSpan={8} style={{ height: paddingBottom, padding: 0, border: 0 }}></td>
              </tr>
            )}
          </tbody>
        </table>
        {filteredCatalog.length === 0 && catalog.length === 0 && (
          <div className="empty-state" style={{ padding: '24px', textAlign: 'center' }}>
            <p>No phenotypes loaded yet.</p>
            <div style={{ display: 'flex', gap: '8px', justifyContent: 'center' }}>
              <input
                value={assetsPath}
                onChange={e => setAssetsPath(e.target.value)}
                placeholder="gs://bucket/path/to/assets.json"
                onKeyDown={e => e.key === 'Enter' && handleLoadAssets()}
                style={{ ...selectStyle, width: '350px' }}
              />
              <button className="btn btn-primary" onClick={handleLoadAssets} disabled={loading || !assetsPath.trim()}>
                {loading ? 'Loading...' : 'Load Phenotypes'}
              </button>
            </div>
          </div>
        )}
        {filteredCatalog.length === 0 && catalog.length > 0 && (
          <div className="empty-state" style={{ padding: '24px', textAlign: 'center' }}>
            No phenotypes match the current filters.
          </div>
        )}
      </div>
    </div>
  );
};

export default PhenotypeLibraryPanel;
