import { useState } from 'react';
import { useAtomValue } from 'jotai';
import { workersAtom, summaryAtom } from '../../atoms/dashboardAtoms';
import type { DashboardWorker, TelemetrySnapshot, CoreTaskInfo } from '../../types';
import '../panels.css';

/**
 * Renders a btop-style grid of workers with per-core CPU visualization.
 * Integrates the core_tasks telemetry to show exactly which partition/task
 * is running on each CPU core.
 */
export const WorkerFleetPanel: React.FC = () => {
  const workers = useAtomValue(workersAtom);
  const summary = useAtomValue(summaryAtom);
  const coordinatorVersion = summary?.build_version;

  return (
    <div className="panel-container">
      <h2 className="panel-title">Worker Fleet ({workers.length})</h2>

      {workers.length === 0 ? (
        <div className="empty-state">No workers connected</div>
      ) : (
        <div className="workers-grid">
          {workers.map((worker) => (
            <WorkerCard key={worker.worker_id} worker={worker} coordinatorVersion={coordinatorVersion} />
          ))}
        </div>
      )}
    </div>
  );
};

/**
 * Detailed info about a unique task (may be running on multiple cores).
 */
interface TaskDetail {
  task_id: string;
  label?: string;
  parent?: CoreTaskInfo;
  cores: number[]; // Which CPU cores are running this task
}

/**
 * Summarizes active tasks by type, with full details per task.
 */
interface TaskTypeSummary {
  type: string;
  count: number; // Total cores running this type
  tasks: TaskDetail[]; // Unique tasks with their details
}

/**
 * Extracts task summaries grouped by type from core_tasks.
 */
const getTaskTypeSummaries = (
  coreTasks?: Record<number, CoreTaskInfo>
): TaskTypeSummary[] => {
  if (!coreTasks) return [];

  // Group by task_type, then by task_id within each type
  const typeMap = new Map<string, Map<string, TaskDetail>>();

  Object.entries(coreTasks).forEach(([coreIdxStr, task]) => {
    const coreIdx = parseInt(coreIdxStr, 10);

    if (!typeMap.has(task.task_type)) {
      typeMap.set(task.task_type, new Map());
    }
    const tasksForType = typeMap.get(task.task_type)!;

    const existing = tasksForType.get(task.task_id);
    if (existing) {
      existing.cores.push(coreIdx);
    } else {
      tasksForType.set(task.task_id, {
        task_id: task.task_id,
        label: task.label,
        parent: task.parent,
        cores: [coreIdx],
      });
    }
  });

  return Array.from(typeMap.entries())
    .map(([type, tasksMap]) => {
      const tasks = Array.from(tasksMap.values()).sort((a, b) =>
        (a.label || a.task_id).localeCompare(b.label || b.task_id)
      );
      const totalCores = tasks.reduce((sum, t) => sum + t.cores.length, 0);
      return {
        type,
        count: totalCores,
        tasks,
      };
    })
    .sort((a, b) => b.count - a.count); // Most active types first
};

/**
 * Gets human-readable name for task type.
 */
const getTaskTypeName = (type: string): string => {
  switch (type) {
    case 'partition':
      return 'Partitions';
    case 'phenotype':
      return 'Phenotypes';
    case 'locus_plot':
      return 'Locus Plots';
    case 'aggregation':
      return 'Aggregation';
    case 'stress':
      return 'Stress Test';
    default:
      return type.charAt(0).toUpperCase() + type.slice(1);
  }
};

/**
 * Gets the color for a task type.
 */
const getTaskTypeColor = (taskType: string): string => {
  switch (taskType) {
    case 'partition':
      return 'var(--cyan)';
    case 'phenotype':
      return 'var(--green)';
    case 'locus_plot':
      return 'var(--magenta, #d19afc)';
    case 'aggregation':
      return 'var(--yellow)';
    case 'stress':
      return 'var(--orange)';
    default:
      return 'var(--text-dim)';
  }
};

/**
 * Formats core indices for display (e.g., "C0, C1, C2" or "C0-C3").
 */
const formatCores = (cores: number[]): string => {
  if (cores.length === 0) return '';
  if (cores.length === 1) return `C${cores[0]}`;
  if (cores.length <= 3) return cores.map((c) => `C${c}`).join(', ');

  // Check if consecutive
  const sorted = [...cores].sort((a, b) => a - b);
  const isConsecutive = sorted.every((c, i) => i === 0 || c === sorted[i - 1] + 1);
  if (isConsecutive) {
    return `C${sorted[0]}-C${sorted[sorted.length - 1]}`;
  }
  return `${cores.length} cores`;
};

/**
 * Badge component displaying a task type summary with hover popover.
 */
const TaskTypeBadge: React.FC<{ summary: TaskTypeSummary }> = ({ summary }) => {
  const [isHovered, setIsHovered] = useState(false);
  const color = getTaskTypeColor(summary.type);
  const typeName = getTaskTypeName(summary.type);

  // Get preview labels for badge
  const previewLabels = summary.tasks
    .slice(0, 2)
    .map((t) => t.label || `#${t.task_id}`);

  return (
    <div
      style={{ position: 'relative' }}
      onMouseEnter={() => setIsHovered(true)}
      onMouseLeave={() => setIsHovered(false)}
    >
      {/* Badge */}
      <div
        style={{
          display: 'flex',
          flexDirection: 'column',
          gap: '2px',
          padding: '4px 8px',
          background: isHovered ? 'rgba(255, 255, 255, 0.06)' : 'rgba(255, 255, 255, 0.03)',
          borderRadius: '4px',
          borderLeft: `3px solid ${color}`,
          minWidth: '80px',
          cursor: 'pointer',
          transition: 'background 0.15s ease',
        }}
      >
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <span style={{ fontSize: '9px', fontWeight: 600, color: color, textTransform: 'uppercase' }}>
            {typeName}
          </span>
          <span
            style={{
              fontSize: '10px',
              fontWeight: 600,
              color: 'var(--text)',
              background: 'rgba(255, 255, 255, 0.1)',
              padding: '0 4px',
              borderRadius: '3px',
            }}
          >
            {summary.count}
          </span>
        </div>
        <div
          style={{
            fontSize: '8px',
            color: 'var(--text-dim)',
            overflow: 'hidden',
            textOverflow: 'ellipsis',
            whiteSpace: 'nowrap',
          }}
        >
          {previewLabels.join(', ')}
          {summary.tasks.length > 2 && ' …'}
        </div>
      </div>

      {/* Hover Popover */}
      {isHovered && (
        <div
          style={{
            position: 'absolute',
            top: '100%',
            left: 0,
            marginTop: '4px',
            zIndex: 100,
            minWidth: '240px',
            maxWidth: '360px',
            maxHeight: '280px',
            overflowY: 'auto',
            background: 'var(--bg-secondary, #1a1a2e)',
            border: `1px solid ${color}`,
            borderRadius: '6px',
            boxShadow: '0 4px 12px rgba(0, 0, 0, 0.4)',
          }}
        >
          {/* Header */}
          <div
            style={{
              padding: '8px 12px',
              borderBottom: '1px solid rgba(255, 255, 255, 0.1)',
              background: 'rgba(255, 255, 255, 0.02)',
            }}
          >
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <span style={{ fontSize: '11px', fontWeight: 600, color: color }}>
                {typeName}
              </span>
              <span style={{ fontSize: '9px', color: 'var(--text-dim)' }}>
                {summary.count} core{summary.count !== 1 ? 's' : ''} • {summary.tasks.length} task{summary.tasks.length !== 1 ? 's' : ''}
              </span>
            </div>
          </div>

          {/* Task List */}
          <div style={{ padding: '4px 0' }}>
            {summary.tasks.map((task, idx) => (
              <div
                key={task.task_id}
                style={{
                  padding: '6px 12px',
                  borderBottom: idx < summary.tasks.length - 1 ? '1px solid rgba(255, 255, 255, 0.05)' : 'none',
                }}
              >
                {/* Task Label & ID */}
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', gap: '8px' }}>
                  <div style={{ flex: 1, minWidth: 0 }}>
                    <div style={{ fontSize: '10px', color: 'var(--text)', fontWeight: 500 }}>
                      {task.label || `Task ${task.task_id}`}
                    </div>
                    {task.label && (
                      <div style={{ fontSize: '8px', color: 'var(--text-dim)', marginTop: '1px' }}>
                        ID: {task.task_id}
                      </div>
                    )}
                  </div>
                  {/* Core badges */}
                  <div
                    style={{
                      fontSize: '8px',
                      color: color,
                      background: 'rgba(255, 255, 255, 0.05)',
                      padding: '2px 5px',
                      borderRadius: '3px',
                      whiteSpace: 'nowrap',
                    }}
                  >
                    {formatCores(task.cores)}
                  </div>
                </div>

                {/* Parent info */}
                {task.parent && (
                  <div
                    style={{
                      fontSize: '8px',
                      color: 'var(--text-dim)',
                      marginTop: '3px',
                      paddingLeft: '8px',
                      borderLeft: '2px solid rgba(255, 255, 255, 0.1)',
                    }}
                  >
                    ↳ {task.parent.task_type}: {task.parent.label || task.parent.task_id}
                  </div>
                )}
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
};

/**
 * Displays the current active task summary for a worker.
 */
const ActiveTaskSummary: React.FC<{
  coreTasks?: Record<number, CoreTaskInfo>;
  status: string;
}> = ({ coreTasks, status }) => {
  const taskSummaries = getTaskTypeSummaries(coreTasks);
  const activeCoreCount = coreTasks ? Object.keys(coreTasks).length : 0;

  if (status !== 'active' || taskSummaries.length === 0) {
    return null;
  }

  return (
    <div style={{ marginBottom: '10px' }}>
      <div
        style={{
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          marginBottom: '6px',
        }}
      >
        <span style={{ fontSize: '9px', color: 'var(--text-dim)', textTransform: 'uppercase' }}>
          Active Work
        </span>
        <span style={{ fontSize: '9px', color: 'var(--cyan)' }}>
          {activeCoreCount} core{activeCoreCount !== 1 ? 's' : ''} busy
        </span>
      </div>
      <div style={{ display: 'flex', flexWrap: 'wrap', gap: '6px' }}>
        {taskSummaries.map((summary) => (
          <TaskTypeBadge key={summary.type} summary={summary} />
        ))}
      </div>
    </div>
  );
};

/**
 * Individual worker card displaying status, hardware usage, and per-core utilization.
 */
const WorkerCard: React.FC<{ worker: DashboardWorker; coordinatorVersion?: string }> = ({ worker, coordinatorVersion }) => {
  const t = worker.telemetry;
  const workerVersion = worker.build_version;
  const versionMismatch = workerVersion && coordinatorVersion && workerVersion !== coordinatorVersion;

  return (
    <div className={`worker-card ${worker.status}`}>
      <div className="worker-header">
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          <span style={{ fontWeight: 600, color: 'var(--cyan)' }}>{worker.worker_id}</span>
          {t?.memory_used_bytes && t?.memory_total_bytes && (t.memory_used_bytes / t.memory_total_bytes) > 0.85 && (
            <span style={{ fontSize: '9px', fontWeight: 600, color: 'var(--bg)', background: 'var(--red)', padding: '1px 4px', borderRadius: '3px' }}>MEM BOUND</span>
          )}
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          {workerVersion && (
            <span
              style={{
                fontSize: '9px',
                color: versionMismatch ? 'var(--yellow)' : 'var(--text-dim)',
                fontFamily: 'monospace',
              }}
              title={versionMismatch ? `Worker version (${workerVersion.slice(0, 7)}) differs from coordinator (${coordinatorVersion?.slice(0, 7)})` : `Worker version: ${workerVersion}`}
            >
              {workerVersion.slice(0, 7)}
              {versionMismatch && ' ⚠'}
            </span>
          )}
          <StatusBadge status={worker.status} />
        </div>
      </div>

      <div className="worker-content">
        {t ? (
          <>
            {/* Active Task Summary (new) */}
            <ActiveTaskSummary coreTasks={t.core_tasks} status={worker.status} />

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
            <WorkerStats
              workerId={worker.worker_id}
              telemetry={t}
              partitionsCompleted={t.partitions_completed}
              currentBatch={worker.current_batch_size}
              maxBatchCapacity={worker.max_batch_capacity}
            />
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
  workerId: string;
  telemetry: TelemetrySnapshot;
  partitionsCompleted: number;
  currentBatch?: number;
  maxBatchCapacity?: number;
}> = ({ workerId, telemetry, partitionsCompleted, currentBatch, maxBatchCapacity }) => {
  const handleResetCapacity = async () => {
    try {
      await fetch(`/api/workers/${workerId}/reset-capacity`, { method: 'POST' });
    } catch (e) {
      console.error("Failed to reset capacity:", e);
    }
  };
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
      <div style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
        <span style={{ color: 'var(--text-dim)' }}>Batch: </span>
        <span style={{ color: maxBatchCapacity ? 'var(--yellow)' : 'var(--cyan)' }}>
          {currentBatch ?? '--'}
          {maxBatchCapacity && ` / ${maxBatchCapacity}`}
        </span>
        {maxBatchCapacity && (
          <button
            onClick={handleResetCapacity}
            title="Reset capacity limit"
            style={{
              background: 'rgba(255,255,255,0.1)',
              border: 'none',
              color: 'var(--text)',
              borderRadius: '3px',
              cursor: 'pointer',
              padding: '1px 4px',
              fontSize: '9px',
              marginLeft: '2px',
            }}
          >
            ↻
          </button>
        )}
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
