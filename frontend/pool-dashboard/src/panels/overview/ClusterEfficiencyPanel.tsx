import { useAtomValue } from 'jotai';
import { bottleneckAtom, summaryAtom } from '../../atoms/dashboardAtoms';
import '../panels.css';

/**
 * Displays the bottleneck heuristics and CPU time breakdown.
 * Subscribes to bottleneckAtom and summaryAtom for efficiency metrics.
 */
export const ClusterEfficiencyPanel: React.FC = () => {
  const bottleneck = useAtomValue(bottleneckAtom);
  const summary = useAtomValue(summaryAtom);

  const getBottleneckClass = (type: string): string => {
    const normalized = type.toLowerCase();
    if (normalized.includes('cpu')) return 'cpu';
    if (normalized.includes('memory') || normalized.includes('mem')) return 'memory';
    if (normalized.includes('network')) return 'network';
    if (normalized.includes('io') || normalized.includes('i/o')) return 'io';
    return 'idle';
  };

  return (
    <div className="panel-container">
      <h2 className="panel-title">Cluster Efficiency</h2>

      {/* Bottleneck Badge */}
      {bottleneck ? (
        <div style={{ marginBottom: '16px' }}>
          <div
            style={{
              padding: '8px 12px',
              background: 'rgba(255, 255, 255, 0.05)',
              borderRadius: '4px',
            }}
          >
            <div style={{ marginBottom: '8px' }}>
              <span className={`bottleneck-badge ${getBottleneckClass(bottleneck.bottleneck)}`}>
                {bottleneck.bottleneck}
              </span>
            </div>
            <div style={{ fontSize: '11px', color: 'var(--text-dim)', marginBottom: '12px' }}>
              {bottleneck.description}
            </div>

            {/* Resource Utilization Grid */}
            <div
              style={{
                display: 'grid',
                gridTemplateColumns: 'repeat(2, 1fr)',
                gap: '8px',
                fontSize: '11px',
              }}
            >
              <div>
                <span style={{ color: 'var(--text-dim)' }}>Avg CPU: </span>
                <span
                  style={{
                    color:
                      bottleneck.avg_cpu_percent >= 90
                        ? 'var(--red)'
                        : bottleneck.avg_cpu_percent >= 70
                          ? 'var(--orange)'
                          : 'var(--cyan)',
                  }}
                >
                  {bottleneck.avg_cpu_percent.toFixed(1)}%
                </span>
              </div>
              <div>
                <span style={{ color: 'var(--text-dim)' }}>Avg MEM: </span>
                <span
                  style={{
                    color:
                      bottleneck.avg_mem_percent >= 90
                        ? 'var(--red)'
                        : bottleneck.avg_mem_percent >= 70
                          ? 'var(--orange)'
                          : 'var(--cyan)',
                  }}
                >
                  {bottleneck.avg_mem_percent.toFixed(1)}%
                </span>
              </div>
              <div>
                <span style={{ color: 'var(--text-dim)' }}>Net RX: </span>
                <span>{bottleneck.avg_network_rx_mb.toFixed(1)} MB/s</span>
              </div>
              <div>
                <span style={{ color: 'var(--text-dim)' }}>Net TX: </span>
                <span>{bottleneck.avg_network_tx_mb.toFixed(1)} MB/s</span>
              </div>
            </div>
          </div>
        </div>
      ) : (
        <div className="empty-state" style={{ height: 'auto', marginBottom: '16px' }}>
          Analyzing bottlenecks...
        </div>
      )}

      {/* CPU Time Breakdown (if available in summary) */}
      {summary && (summary as unknown as CpuBreakdown).scan_cpu_secs !== undefined && (
        <CpuTimeBreakdown summary={summary as unknown as CpuBreakdown} />
      )}
    </div>
  );
};

/**
 * CPU time breakdown interface (matches fields that may exist on summary).
 */
interface CpuBreakdown {
  scan_cpu_secs: number;
  aggregate_cpu_secs: number;
  wasted_cpu_secs: number;
}

/**
 * Renders a visual CPU time breakdown bar showing scan vs aggregate vs wasted time.
 */
const CpuTimeBreakdown: React.FC<{ summary: CpuBreakdown }> = ({ summary }) => {
  const scanSecs = summary.scan_cpu_secs || 0;
  const aggSecs = summary.aggregate_cpu_secs || 0;
  const wastedSecs = summary.wasted_cpu_secs || 0;
  const totalSecs = scanSecs + aggSecs + wastedSecs;

  if (totalSecs === 0) return null;

  const scanPct = (scanSecs / totalSecs) * 100;
  const aggPct = (aggSecs / totalSecs) * 100;
  const wastedPct = (wastedSecs / totalSecs) * 100;

  return (
    <div>
      <div
        style={{
          fontSize: '10px',
          textTransform: 'uppercase',
          color: 'var(--text-dim)',
          marginBottom: '6px',
        }}
      >
        CPU Time Breakdown
      </div>

      {/* Stacked Bar */}
      <div
        style={{
          height: '12px',
          display: 'flex',
          borderRadius: '3px',
          overflow: 'hidden',
          marginBottom: '8px',
        }}
      >
        {scanPct > 0 && (
          <div
            style={{
              width: `${scanPct}%`,
              background: 'var(--cyan)',
            }}
            title={`Scan: ${scanSecs.toFixed(0)}s (${scanPct.toFixed(1)}%)`}
          />
        )}
        {aggPct > 0 && (
          <div
            style={{
              width: `${aggPct}%`,
              background: 'var(--orange)',
            }}
            title={`Aggregate: ${aggSecs.toFixed(0)}s (${aggPct.toFixed(1)}%)`}
          />
        )}
        {wastedPct > 0 && (
          <div
            style={{
              width: `${wastedPct}%`,
              background: 'var(--red)',
            }}
            title={`Wasted: ${wastedSecs.toFixed(0)}s (${wastedPct.toFixed(1)}%)`}
          />
        )}
      </div>

      {/* Legend */}
      <div style={{ display: 'flex', gap: '12px', fontSize: '10px' }}>
        <span>
          <span
            style={{
              display: 'inline-block',
              width: '8px',
              height: '8px',
              background: 'var(--cyan)',
              borderRadius: '2px',
              marginRight: '4px',
            }}
          />
          Scan: {scanSecs.toFixed(0)}s
        </span>
        <span>
          <span
            style={{
              display: 'inline-block',
              width: '8px',
              height: '8px',
              background: 'var(--orange)',
              borderRadius: '2px',
              marginRight: '4px',
            }}
          />
          Agg: {aggSecs.toFixed(0)}s
        </span>
        <span>
          <span
            style={{
              display: 'inline-block',
              width: '8px',
              height: '8px',
              background: 'var(--red)',
              borderRadius: '2px',
              marginRight: '4px',
            }}
          />
          Wasted: {wastedSecs.toFixed(0)}s
        </span>
      </div>
    </div>
  );
};

export default ClusterEfficiencyPanel;
