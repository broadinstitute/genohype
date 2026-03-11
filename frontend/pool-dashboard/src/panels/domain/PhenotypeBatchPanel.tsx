import { useMemo, useState } from 'react';
import { useAtomValue } from 'jotai';
import { batchAtom, summaryAtom } from '../../atoms/dashboardAtoms';
import type { PhenotypeStatus } from '../../types';
import '../panels.css';

/**
 * Domain-specific panel for ManhattanBatch jobs.
 * Displays a filterable, sortable data table of phenotype state machines.
 * Only renders meaningful content when the job_spec indicates a batch job.
 */
export const PhenotypeBatchPanel: React.FC = () => {
  const summary = useAtomValue(summaryAtom);
  const batch = useAtomValue(batchAtom);

  const [filter, setFilter] = useState<string>('');
  const [stageFilter, setStageFilter] = useState<string>('all');
  const [sortBy, setSortBy] = useState<'id' | 'stage' | 'progress' | 'cpu'>('id');
  const [sortAsc, setSortAsc] = useState(true);

  // Check if this is a batch job
  const jobSpec = summary?.job_spec as { type?: string } | undefined;
  const isBatchJob = jobSpec?.type === 'ManhattanBatch';

  // Process and sort phenotypes
  const phenotypes = useMemo(() => {
    if (!batch?.phenotypes) return [];

    let filtered = batch.phenotypes;

    // Apply text filter
    if (filter) {
      const lowerFilter = filter.toLowerCase();
      filtered = filtered.filter((p) => p.id.toLowerCase().includes(lowerFilter));
    }

    // Apply stage filter
    if (stageFilter !== 'all') {
      filtered = filtered.filter((p) => p.stage === stageFilter);
    }

    // Sort
    return [...filtered].sort((a, b) => {
      let cmp = 0;
      switch (sortBy) {
        case 'id':
          cmp = a.id.localeCompare(b.id);
          break;
        case 'stage':
          cmp = a.stage.localeCompare(b.stage);
          break;
        case 'progress':
          cmp = (a.partitions_done / (a.partitions_total || 1)) -
                (b.partitions_done / (b.partitions_total || 1));
          break;
        case 'cpu':
          cmp = (a.cpu_core_secs ?? 0) - (b.cpu_core_secs ?? 0);
          break;
      }
      return sortAsc ? cmp : -cmp;
    });
  }, [batch, filter, stageFilter, sortBy, sortAsc]);

  // Stage counts for filter badges
  const stageCounts = useMemo(() => {
    if (!batch?.phenotypes) return {};
    return batch.phenotypes.reduce(
      (acc, p) => {
        acc[p.stage] = (acc[p.stage] || 0) + 1;
        return acc;
      },
      {} as Record<string, number>
    );
  }, [batch]);

  // Handle sort column click
  const handleSort = (column: typeof sortBy) => {
    if (sortBy === column) {
      setSortAsc(!sortAsc);
    } else {
      setSortBy(column);
      setSortAsc(true);
    }
  };

  if (!isBatchJob) {
    return (
      <div className="panel-container">
        <h2 className="panel-title">Phenotype Batch</h2>
        <div className="empty-state">
          This panel is only active for ManhattanBatch jobs.
        </div>
      </div>
    );
  }

  return (
    <div className="panel-container" style={{ display: 'flex', flexDirection: 'column' }}>
      <h2 className="panel-title">
        Phenotype Batch Progress ({phenotypes.length} / {batch?.phenotypes?.length || 0})
      </h2>

      {/* Filters */}
      <div
        style={{
          display: 'flex',
          gap: '12px',
          marginBottom: '12px',
          flexWrap: 'wrap',
          alignItems: 'center',
        }}
      >
        {/* Text search */}
        <input
          type="text"
          placeholder="Filter by name..."
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          style={{
            background: 'var(--bg)',
            border: '1px solid var(--border)',
            borderRadius: '4px',
            padding: '4px 8px',
            color: 'var(--text)',
            fontSize: '11px',
            minWidth: '150px',
          }}
        />

        {/* Stage filter badges */}
        <div style={{ display: 'flex', gap: '4px', flexWrap: 'wrap' }}>
          <StageFilterBadge
            stage="all"
            count={batch?.phenotypes?.length || 0}
            active={stageFilter === 'all'}
            onClick={() => setStageFilter('all')}
          />
          {['queued', 'scanning', 'aggregating', 'completed', 'failed'].map((stage) => (
            <StageFilterBadge
              key={stage}
              stage={stage}
              count={stageCounts[stage] || 0}
              active={stageFilter === stage}
              onClick={() => setStageFilter(stage)}
            />
          ))}
        </div>
      </div>

      {/* Data table */}
      <div style={{ flex: 1, overflowY: 'auto' }}>
        <table className="data-table">
          <thead>
            <tr>
              <SortableHeader
                label="Phenotype"
                sortKey="id"
                currentSort={sortBy}
                sortAsc={sortAsc}
                onClick={handleSort}
              />
              <SortableHeader
                label="Stage"
                sortKey="stage"
                currentSort={sortBy}
                sortAsc={sortAsc}
                onClick={handleSort}
              />
              <SortableHeader
                label="Progress"
                sortKey="progress"
                currentSort={sortBy}
                sortAsc={sortAsc}
                onClick={handleSort}
              />
              <SortableHeader
                label="CPU Time"
                sortKey="cpu"
                currentSort={sortBy}
                sortAsc={sortAsc}
                onClick={handleSort}
              />
              <th style={{ padding: '8px' }}>Duration</th>
            </tr>
          </thead>
          <tbody>
            {phenotypes.map((p) => (
              <PhenotypeRow key={p.id} phenotype={p} />
            ))}
          </tbody>
        </table>

        {phenotypes.length === 0 && (
          <div className="empty-state" style={{ height: 'auto', padding: '24px' }}>
            No phenotypes match the current filters
          </div>
        )}
      </div>
    </div>
  );
};

/**
 * Stage filter badge button.
 */
const StageFilterBadge: React.FC<{
  stage: string;
  count: number;
  active: boolean;
  onClick: () => void;
}> = ({ stage, count, active, onClick }) => {
  const getStageColor = (s: string): string => {
    switch (s) {
      case 'queued':
        return 'var(--text-dim)';
      case 'scanning':
        return 'var(--cyan)';
      case 'aggregating':
        return 'var(--orange)';
      case 'completed':
        return 'var(--green)';
      case 'failed':
        return 'var(--red)';
      default:
        return 'var(--accent)';
    }
  };

  return (
    <button
      onClick={onClick}
      style={{
        background: active ? 'rgba(255, 255, 255, 0.1)' : 'transparent',
        border: `1px solid ${active ? getStageColor(stage) : 'var(--border)'}`,
        borderRadius: '4px',
        padding: '2px 6px',
        color: getStageColor(stage),
        fontSize: '10px',
        fontWeight: 600,
        textTransform: 'uppercase',
        cursor: 'pointer',
        transition: 'all 0.2s',
      }}
    >
      {stage === 'all' ? 'All' : stage} ({count})
    </button>
  );
};

/**
 * Sortable table header.
 */
const SortableHeader: React.FC<{
  label: string;
  sortKey: 'id' | 'stage' | 'progress' | 'cpu';
  currentSort: string;
  sortAsc: boolean;
  onClick: (key: 'id' | 'stage' | 'progress' | 'cpu') => void;
}> = ({ label, sortKey, currentSort, sortAsc, onClick }) => {
  const isSorted = currentSort === sortKey;

  return (
    <th
      style={{
        padding: '8px',
        cursor: 'pointer',
        userSelect: 'none',
        color: isSorted ? 'var(--accent)' : 'var(--text-dim)',
      }}
      onClick={() => onClick(sortKey)}
    >
      {label}
      {isSorted && (
        <span style={{ marginLeft: '4px' }}>{sortAsc ? '\u25b2' : '\u25bc'}</span>
      )}
    </th>
  );
};

/**
 * Individual phenotype table row.
 */
const PhenotypeRow: React.FC<{ phenotype: PhenotypeStatus }> = ({ phenotype: p }) => {
  const formatDuration = (secs: number | undefined): string => {
    if (secs === undefined) return '--';
    if (secs >= 3600) {
      const h = Math.floor(secs / 3600);
      const m = Math.floor((secs % 3600) / 60);
      return `${h}h ${m}m`;
    }
    if (secs >= 60) {
      const m = Math.floor(secs / 60);
      const s = Math.floor(secs % 60);
      return `${m}m ${s}s`;
    }
    return `${secs.toFixed(1)}s`;
  };

  const progressPct =
    p.partitions_total > 0
      ? ((p.partitions_done / p.partitions_total) * 100).toFixed(0)
      : '--';

  // Extract short phenotype name from full path
  const shortName = p.id.split('/').pop() || p.id;

  return (
    <tr>
      <td style={{ padding: '8px', color: 'var(--accent)' }} title={p.id}>
        {shortName}
      </td>
      <td style={{ padding: '8px' }}>
        <span className={`stage-badge ${p.stage}`}>{p.stage}</span>
        {p.error && (
          <div style={{ fontSize: '10px', color: 'var(--red)', marginTop: '2px', maxWidth: '300px', wordBreak: 'break-word' }}>
            {p.error}
          </div>
        )}
      </td>
      <td style={{ padding: '8px' }}>
        {p.partitions_total > 0 ? (
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
            <div
              style={{
                width: '60px',
                height: '6px',
                background: 'var(--border)',
                borderRadius: '3px',
                overflow: 'hidden',
              }}
            >
              <div
                style={{
                  width: `${progressPct}%`,
                  height: '100%',
                  background:
                    p.stage === 'completed'
                      ? 'var(--green)'
                      : p.stage === 'failed'
                        ? 'var(--red)'
                        : 'var(--cyan)',
                }}
              />
            </div>
            <span style={{ fontSize: '10px' }}>
              {p.partitions_done}/{p.partitions_total}
            </span>
          </div>
        ) : (
          '--'
        )}
      </td>
      <td style={{ padding: '8px' }}>
        {p.cpu_core_secs !== undefined ? `${p.cpu_core_secs.toFixed(1)}s` : '--'}
      </td>
      <td style={{ padding: '8px' }}>{formatDuration(p.duration_secs)}</td>
    </tr>
  );
};

export default PhenotypeBatchPanel;
