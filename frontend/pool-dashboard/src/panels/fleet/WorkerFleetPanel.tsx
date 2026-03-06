import { useAtomValue } from 'jotai';
import { workersAtom } from '../../atoms/dashboardAtoms';
import type { DashboardWorker, TelemetrySnapshot, CoreTaskInfo } from '../../types';
import '../panels.css';

/**
 * Renders a btop-style grid of workers with per-core CPU visualization.
 * Integrates the core_tasks telemetry to show exactly which partition/task
 * is running on each CPU core.
 */
export const WorkerFleetPanel: React.FC = () => {
  const workers = useAtomValue(workersAtom);

  return (
    <div className="panel-container">
      <h2 className="panel-title">Worker Fleet ({workers.length})</h2>

      {workers.length === 0 ? (
        <div className="empty-state">No workers connected</div>
      ) : (
        <div className="workers-grid">
          {workers.map((worker) => (
            <WorkerCard key={worker.worker_id} worker={worker} />
          ))}
        </div>
      )}
    </div>
  );
};

/**
 * Individual worker card displaying status, hardware usage, and per-core utilization.
 */
const WorkerCard: React.FC<{ worker: DashboardWorker }> = ({ worker }) => {
  const t = worker.telemetry;

  return (
    <div className={`worker-card ${worker.status}`}>
      <div className="worker-header">
        <span style={{ fontWeight: 600, color: 'var(--cyan)' }}>{worker.worker_id}</span>
        <StatusBadge status={worker.status} />
      </div>

      <div className="worker-content">
        {t ? (
          <>
            {/* CPU Overview */}
            <div
              style={{
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'center',
                marginBottom: '8px',
              }}
            >
              <span style={{ fontSize: '10px', color: 'var(--cyan)', fontWeight: 600 }}>
                CPU
              </span>
              <span style={{ fontSize: '12px' }}>{t.cpu_percent?.toFixed(0) ?? '--'}%</span>
            </div>

            {/* Per-Core Visualization */}
            {t.cpu_per_core && t.cpu_per_core.length > 0 && (
              <CoresGrid cpuPerCore={t.cpu_per_core} coreTasks={t.core_tasks} />
            )}

            {/* Resource Stats */}
            <WorkerStats telemetry={t} partitionsCompleted={t.partitions_completed} />
          </>
        ) : (
          <div className="empty-state" style={{ height: 'auto' }}>
            No telemetry data
          </div>
        )}
      </div>
    </div>
  );
};

/**
 * Status badge for worker cards.
 */
const StatusBadge: React.FC<{ status: string }> = ({ status }) => {
  const getColor = () => {
    switch (status) {
      case 'active':
        return 'var(--green)';
      case 'idle':
        return 'var(--yellow)';
      case 'dead':
        return 'var(--red)';
      case 'draining':
        return 'var(--orange)';
      default:
        return 'var(--text-dim)';
    }
  };

  return (
    <span
      style={{
        fontSize: '9px',
        textTransform: 'uppercase',
        fontWeight: 600,
        color: getColor(),
        padding: '2px 6px',
        background: 'rgba(255, 255, 255, 0.05)',
        borderRadius: '3px',
      }}
    >
      {status}
    </span>
  );
};

/**
 * Per-core CPU visualization grid with task tooltips.
 */
const CoresGrid: React.FC<{
  cpuPerCore: number[];
  coreTasks?: Record<number, CoreTaskInfo>;
}> = ({ cpuPerCore, coreTasks }) => {
  const getCpuColor = (pct: number): string => {
    if (pct >= 90) return 'var(--red)';
    if (pct >= 70) return 'var(--orange)';
    if (pct >= 50) return 'var(--yellow)';
    return 'var(--cyan)';
  };

  const getTooltip = (coreIdx: number): string => {
    const taskInfo = coreTasks?.[coreIdx];
    if (!taskInfo) return 'Idle';

    let tooltip = `${taskInfo.task_type}: ${taskInfo.label || taskInfo.task_id}`;

    // Include parent context if available (e.g., phenotype for locus plots)
    if (taskInfo.parent) {
      tooltip += ` (${taskInfo.parent.task_type}: ${taskInfo.parent.label || taskInfo.parent.task_id})`;
    }

    return tooltip;
  };

  return (
    <div className="cores-grid" style={{ marginBottom: '12px' }}>
      {cpuPerCore.map((pct, idx) => (
        <div key={idx} className="core-item" title={getTooltip(idx)}>
          <span className="core-label">C{idx}</span>
          <div className="core-bar-outer">
            <div
              className="core-bar"
              style={{
                width: `${Math.min(pct, 100)}%`,
                backgroundColor: getCpuColor(pct),
              }}
            />
          </div>
        </div>
      ))}
    </div>
  );
};

/**
 * Worker resource statistics (memory, network, throughput).
 */
const WorkerStats: React.FC<{
  telemetry: TelemetrySnapshot;
  partitionsCompleted: number;
}> = ({ telemetry, partitionsCompleted }) => {
  const formatBytes = (bytes: number | undefined): string => {
    if (bytes === undefined) return '--';
    if (bytes >= 1e9) return `${(bytes / 1e9).toFixed(1)} GB`;
    if (bytes >= 1e6) return `${(bytes / 1e6).toFixed(1)} MB`;
    if (bytes >= 1e3) return `${(bytes / 1e3).toFixed(1)} KB`;
    return `${bytes} B`;
  };

  const formatBytesPerSec = (bytesPerSec: number | undefined): string => {
    if (bytesPerSec === undefined) return '--';
    if (bytesPerSec >= 1e9) return `${(bytesPerSec / 1e9).toFixed(1)} GB/s`;
    if (bytesPerSec >= 1e6) return `${(bytesPerSec / 1e6).toFixed(1)} MB/s`;
    if (bytesPerSec >= 1e3) return `${(bytesPerSec / 1e3).toFixed(1)} KB/s`;
    return `${bytesPerSec.toFixed(0)} B/s`;
  };

  const memUsed = telemetry.memory_used_bytes;
  const memTotal = telemetry.memory_total_bytes;
  const memPct =
    memUsed !== undefined && memTotal !== undefined && memTotal > 0
      ? ((memUsed / memTotal) * 100).toFixed(0)
      : '--';

  return (
    <div
      style={{
        display: 'grid',
        gridTemplateColumns: '1fr 1fr',
        gap: '8px',
        fontSize: '10px',
      }}
    >
      {/* Memory */}
      <div>
        <span style={{ color: 'var(--text-dim)' }}>RAM: </span>
        <span>
          {formatBytes(memUsed)} / {formatBytes(memTotal)} ({memPct}%)
        </span>
      </div>

      {/* Partitions Done */}
      <div>
        <span style={{ color: 'var(--text-dim)' }}>Parts Done: </span>
        <span style={{ color: 'var(--green)' }}>{partitionsCompleted}</span>
      </div>

      {/* Network RX */}
      <div>
        <span style={{ color: 'var(--text-dim)' }}>Net RX: </span>
        <span>{formatBytesPerSec(telemetry.network_rx_bytes_sec)}</span>
      </div>

      {/* Network TX */}
      <div>
        <span style={{ color: 'var(--text-dim)' }}>Net TX: </span>
        <span>{formatBytesPerSec(telemetry.network_tx_bytes_sec)}</span>
      </div>

      {/* Throughput */}
      <div>
        <span style={{ color: 'var(--text-dim)' }}>Rows/sec: </span>
        <span style={{ color: 'var(--cyan)' }}>
          {telemetry.items_per_sec.toLocaleString(undefined, { maximumFractionDigits: 0 })}
        </span>
      </div>

      {/* Disk I/O */}
      {(telemetry.disk_read_bytes_sec !== undefined ||
        telemetry.disk_write_bytes_sec !== undefined) && (
        <div>
          <span style={{ color: 'var(--text-dim)' }}>Disk: </span>
          <span>
            R: {formatBytesPerSec(telemetry.disk_read_bytes_sec)} / W:{' '}
            {formatBytesPerSec(telemetry.disk_write_bytes_sec)}
          </span>
        </div>
      )}
    </div>
  );
};

export default WorkerFleetPanel;
