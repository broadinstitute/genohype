import { useState, useMemo } from 'react';
import { useAtomValue } from 'jotai';
import { catalogAtom, clickhouseInfoAtom } from '../../atoms/dashboardAtoms';
import type { TableInfo, PartitionInfo, IngestedPhenotype } from '../../types';
import '../panels.css';

const formatBytes = (bytes: number): string => {
  if (bytes === 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  return `${(bytes / Math.pow(1024, i)).toFixed(1)} ${units[i]}`;
};

const formatNumber = (n: number): string => n.toLocaleString();

export const ClickHousePanel: React.FC = () => {
  const catalog = useAtomValue(catalogAtom);
  const info = useAtomValue(clickhouseInfoAtom);
  const [expandedTable, setExpandedTable] = useState<string | null>(null);

  // Cross-reference catalog phenotypes with ClickHouse ingested phenotypes
  const catalogPhenotypes = useMemo(() => {
    return new Set(catalog.map(c => `${c.id}::${c.ancestry}`));
  }, [catalog]);

  // Totals for table overview
  const totals = useMemo(() => {
    if (!info || info.error) return null;
    return (info.tables || []).reduce(
      (acc, t) => ({
        rows: acc.rows + t.rows,
        bytes_on_disk: acc.bytes_on_disk + t.bytes_on_disk,
        bytes_uncompressed: acc.bytes_uncompressed + t.bytes_uncompressed,
        part_count: acc.part_count + t.part_count,
        partition_count: acc.partition_count + t.partition_count,
      }),
      { rows: 0, bytes_on_disk: 0, bytes_uncompressed: 0, part_count: 0, partition_count: 0 }
    );
  }, [info]);

  if (info?.error) {
    return (
      <div className="panel-container">
        <h2 className="panel-title">ClickHouse</h2>
        <div style={{ padding: '16px', color: 'var(--text-dim)', fontSize: '12px' }}>
          {info.error}
        </div>
      </div>
    );
  }

  if (!info) {
    return (
      <div className="panel-container">
        <h2 className="panel-title">ClickHouse</h2>
        <div style={{ padding: '16px', color: 'var(--text-dim)', fontSize: '12px' }}>Loading...</div>
      </div>
    );
  }

  return (
    <div className="panel-container" style={{ display: 'flex', flexDirection: 'column' }}>
      <h2 className="panel-title" style={{ marginBottom: '12px' }}>ClickHouse Storage</h2>

      {/* Section 1: Table Overview */}
      <div style={{ marginBottom: '16px' }}>
        <table className="data-table">
          <thead>
            <tr>
              <th>Table</th>
              <th style={{ textAlign: 'right' }}>Rows</th>
              <th style={{ textAlign: 'right' }}>Compressed</th>
              <th style={{ textAlign: 'right' }}>Uncompressed</th>
              <th style={{ textAlign: 'right' }}>Ratio</th>
              <th style={{ textAlign: 'right' }}>Parts</th>
              <th style={{ textAlign: 'right' }}>Partitions</th>
            </tr>
          </thead>
          <tbody>
            {(info.tables || []).map((t: TableInfo) => (
              <tr
                key={t.table}
                style={{ cursor: 'pointer' }}
                onClick={() => setExpandedTable(expandedTable === t.table ? null : t.table)}
              >
                <td style={{ color: 'var(--cyan)', fontWeight: 600 }}>{t.table}</td>
                <td style={{ textAlign: 'right' }}>{formatNumber(t.rows)}</td>
                <td style={{ textAlign: 'right' }}>{formatBytes(t.bytes_on_disk)}</td>
                <td style={{ textAlign: 'right' }}>{formatBytes(t.bytes_uncompressed)}</td>
                <td style={{ textAlign: 'right' }}>{t.bytes_on_disk > 0 ? (t.bytes_uncompressed / t.bytes_on_disk).toFixed(1) + 'x' : '--'}</td>
                <td style={{ textAlign: 'right' }}>{t.part_count}</td>
                <td style={{ textAlign: 'right' }}>{t.partition_count}</td>
              </tr>
            ))}
            {totals && (
              <tr style={{ fontWeight: 'bold', borderTop: '2px solid var(--border)' }}>
                <td>Total</td>
                <td style={{ textAlign: 'right' }}>{formatNumber(totals.rows)}</td>
                <td style={{ textAlign: 'right' }}>{formatBytes(totals.bytes_on_disk)}</td>
                <td style={{ textAlign: 'right' }}>{formatBytes(totals.bytes_uncompressed)}</td>
                <td style={{ textAlign: 'right' }}>{totals.bytes_on_disk > 0 ? (totals.bytes_uncompressed / totals.bytes_on_disk).toFixed(1) + 'x' : '--'}</td>
                <td style={{ textAlign: 'right' }}>{totals.part_count}</td>
                <td style={{ textAlign: 'right' }}>{totals.partition_count}</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      {/* Section 2: Expanded Partition Detail */}
      {expandedTable && (
        <div style={{ marginBottom: '16px' }}>
          <h3 style={{ fontSize: '12px', color: 'var(--text-dim)', marginBottom: '8px' }}>
            Partitions for {expandedTable}
          </h3>
          <table className="data-table">
            <thead>
              <tr>
                <th>Phenotype</th>
                <th style={{ textAlign: 'right' }}>Rows</th>
                <th style={{ textAlign: 'right' }}>Size</th>
                <th>Status</th>
              </tr>
            </thead>
            <tbody>
              {(info.partitions || [])
                .filter((p: PartitionInfo) => p.table === expandedTable)
                .map((p: PartitionInfo) => {
                  const inCatalog = catalogPhenotypes.has(`${p.phenotype}::`) || catalog.some(c => c.id === p.phenotype);
                  const inCH = true; // It's in ClickHouse since it's in partitions
                  let color = 'var(--text-dim)'; // gray: catalog only
                  if (inCatalog && inCH) color = 'var(--green)'; // green: synced
                  else if (!inCatalog && inCH) color = 'var(--orange)'; // yellow: orphaned

                  return (
                    <tr key={`${p.table}-${p.phenotype}`}>
                      <td style={{ color }}>{p.phenotype}</td>
                      <td style={{ textAlign: 'right' }}>{formatNumber(p.rows)}</td>
                      <td style={{ textAlign: 'right' }}>{formatBytes(p.bytes_on_disk)}</td>
                      <td>
                        {inCatalog ? (
                          <span style={{ color: 'var(--green)', fontSize: '10px' }}>in catalog</span>
                        ) : (
                          <span style={{ color: 'var(--orange)', fontSize: '10px' }}>orphaned</span>
                        )}
                      </td>
                    </tr>
                  );
                })}
            </tbody>
          </table>
        </div>
      )}

      {/* Section 3: Pipeline Status */}
      {(info.ingested_phenotypes || []).length > 0 && (
        <div style={{ flex: 1, overflowY: 'auto' }}>
          <h3 style={{ fontSize: '12px', color: 'var(--text-dim)', marginBottom: '8px' }}>
            Pipeline Status ({(info.ingested_phenotypes || []).length} phenotypes)
          </h3>
          <table className="data-table">
            <thead>
              <tr>
                <th>Phenotype</th>
                <th>Ancestry</th>
                <th>Status</th>
                <th style={{ textAlign: 'right' }}>Loci</th>
                <th style={{ textAlign: 'right' }}>Sig. Variants</th>
              </tr>
            </thead>
            <tbody>
              {(info.ingested_phenotypes || []).map((p: IngestedPhenotype) => (
                <tr key={`${p.phenotype}-${p.ancestry}`}>
                  <td style={{ color: 'var(--cyan)' }}>{p.phenotype}</td>
                  <td>{p.ancestry}</td>
                  <td>
                    <span className={`stage-badge ${p.status.toLowerCase()}`}>{p.status}</span>
                  </td>
                  <td style={{ textAlign: 'right' }}>{formatNumber(p.loci_count)}</td>
                  <td style={{ textAlign: 'right' }}>{formatNumber(p.significant_variants)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
};

export default ClickHousePanel;
