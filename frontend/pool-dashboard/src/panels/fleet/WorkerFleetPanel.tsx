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
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          <span style={{ fontWeight: 600, color: 'var(--cyan)' }}>{worker.worker_id}</span>
          {t?.memory_used_bytes && t?.memory_total_bytes && (t.memory_used_bytes / t.memory_total_bytes) > 0.85 && (
            <span style={{ fontSize: '9px', fontWeight: 600, color: 'var(--bg)', background: 'var(--red)', padding: '1px 4px', borderRadius: '3px' }}>MEM BOUND</span>
          )}
        </div>
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
            <WorkerStats telemetry={t} partitionsCompleted={t.partitions_completed} currentBatch={worker.current_batch_size} />
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
 * Per-core CPU visualization grid with task labels.
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

  const getTaskColor = (taskType: string): string => {
    switch (taskType) {
      case 'partition':
        return 'var(--cyan)';
      case 'phenotype':
        return 'var(--green)';
      case 'locus_plot':
        return 'var(--magenta, #d19afc)';
      case 'aggregation':
        return 'var(--yellow)';
      default:
        return 'var(--text-dim)';
    }
  };

  const getTaskLabel = (coreIdx: number): { short: string; full: string } | null => {
    const taskInfo = coreTasks?.[coreIdx];
    if (!taskInfo) return null;

    // Build short label for display
    let short: string;
    if (taskInfo.task_type === 'partition') {
      short = `P${taskInfo.task_id}`;
    } else if (taskInfo.label) {
      // Truncate long labels
      short = taskInfo.label.length > 12 ? taskInfo.label.slice(0, 10) + '…' : taskInfo.label;
    } else {
      short = taskInfo.task_id.length > 8 ? taskInfo.task_id.slice(0, 6) + '…' : taskInfo.task_id;
    }

    // Build full tooltip
    let full = `${taskInfo.task_type}: ${taskInfo.label || taskInfo.task_id}`;
    if (taskInfo.parent) {
      full += ` (${taskInfo.parent.task_type}: ${taskInfo.parent.label || taskInfo.parent.task_id})`;
    }

    return { short, full };
  };

  return (
    <div className="cores-grid" style={{ marginBottom: '12px' }}>
      {cpuPerCore.map((pct, idx) => {
        const task = getTaskLabel(idx);
        const taskInfo = coreTasks?.[idx];
        return (
          <div
            key={idx}
            className="core-item"
            title={task?.full || 'Idle'}
            style={{ marginBottom: '4px' }}
          >
            <div style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
              <span className="core-label">C{idx}</span>
              <div className="core-bar-outer" style={{ flex: 1 }}>
                <div
                  className="core-bar"
                  style={{
                    width: `${Math.min(pct, 100)}%`,
                    backgroundColor: getCpuColor(pct),
                  }}
                />
              </div>
            </div>
            {task && (
              <div
                style={{
                  fontSize: '8px',
                  color: getTaskColor(taskInfo?.task_type || ''),
                  marginLeft: '20px',
                  overflow: 'hidden',
                  textOverflow: 'ellipsis',
                  whiteSpace: 'nowrap',
                }}
              >
                {task.short}
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
};

/**
 * Worker resource statistics (memory, network, throughput).
 */
const WorkerStats: React.FC<{
  telemetry: TelemetrySnapshot;
  partitionsCompleted: number;
  currentBatch?: number;
}> = ({ telemetry, partitionsCompleted, currentBatch }) => {
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
  const memRatio = memUsed !== undefined && memTotal !== undefined && memTotal > 0 ? (memUsed / memTotal) : 0;
  const memPct = memRatio > 0 ? (memRatio * 100).toFixed(0) : '--';
  const memColor = memRatio > 0.85 ? 'var(--red)' : memRatio > 0.75 ? 'var(--yellow)' : 'inherit';

  return (
    <div
      style={{
        display: 'grid',
        gridTemplateColumns: '1fr 1fr',
        gap: '8px',
        fontSize: '10px',
      }}
    >
      {/* Memory Bar */}
      <div style={{ gridColumn: '1 / -1' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '2px' }}>
          <span style={{ color: 'var(--text-dim)' }}>RAM</span>
          <span style={{ color: memColor, fontSize: '9px' }}>
            {formatBytes(memUsed)} / {formatBytes(memTotal)} ({memPct}%)
          </span>
        </div>
        <div className="core-bar-outer" style={{ height: '8px' }}>
          <div
            className="core-bar"
            style={{
              width: `${memRatio * 100}%`,
              backgroundColor: memColor === 'inherit' ? 'var(--cyan)' : memColor,
            }}
          />
        </div>
      </div>

      {/* Batch Size */}
      <div>
        <span style={{ color: 'var(--text-dim)' }}>Batch Size: </span>
        <span style={{ color: 'var(--cyan)' }}>{currentBatch ?? '--'}</span>
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
