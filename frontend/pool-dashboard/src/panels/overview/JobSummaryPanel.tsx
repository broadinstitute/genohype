import { useAtomValue } from 'jotai';
import { summaryAtom } from '../../atoms/dashboardAtoms';
import '../panels.css';

/**
 * Displays basic job info, global progress bar, ETA, and a "Cancel Job" button.
 * Subscribes to summaryAtom for all job-level state.
 */
export const JobSummaryPanel: React.FC = () => {
  const summary = useAtomValue(summaryAtom);

  const handleCancel = async () => {
    if (!window.confirm('Are you sure you want to cancel the job?')) return;

    try {
      await fetch('/api/cancel', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ reason: 'UI Request' }),
      });
    } catch (err) {
      console.error('Failed to cancel job:', err);
    }
  };

  if (!summary) {
    return (
      <div className="panel-container">
        <h2 className="panel-title">Job Summary</h2>
        <div className="empty-state">Waiting for job data...</div>
      </div>
    );
  }

  const formatDuration = (seconds: number): string => {
    const h = Math.floor(seconds / 3600);
    const m = Math.floor((seconds % 3600) / 60);
    const s = Math.floor(seconds % 60);
    if (h > 0) {
      return `${h}:${m.toString().padStart(2, '0')}:${s.toString().padStart(2, '0')}`;
    }
    return `${m}:${s.toString().padStart(2, '0')}`;
  };

  const formatPath = (path: string): string => {
    // Truncate long GCS paths for display
    if (path.length > 60) {
      return path.slice(0, 30) + '...' + path.slice(-27);
    }
    return path;
  };

  const failedPercent =
    summary.total_tasks > 0
      ? (summary.failed_tasks / summary.total_tasks) * 100
      : 0;

  return (
    <div className="panel-container">
      <h2 className="panel-title">Job Summary</h2>

      {/* Job Info Section */}
      <div style={{ marginBottom: '12px' }}>
        <div style={{ marginBottom: '4px' }}>
          <span style={{ color: 'var(--text-dim)' }}>Input: </span>
          <span style={{ wordBreak: 'break-all' }} title={summary.input_path}>
            {formatPath(summary.input_path)}
          </span>
        </div>
        {summary.backup_path && (
          <div style={{ marginBottom: '4px' }}>
            <span style={{ color: 'var(--text-dim)' }}>Backup: </span>
            <span style={{ wordBreak: 'break-all', color: 'var(--green)' }} title={summary.backup_path}>
              {formatPath(summary.backup_path)}
            </span>
            {summary.last_backup_at && (
              <span style={{ color: 'var(--text-dim)', marginLeft: '8px', fontSize: '10px' }}>
                (last: {new Date(summary.last_backup_at).toLocaleTimeString()})
              </span>
            )}
          </div>
        )}
        <div style={{ display: 'flex', gap: '16px', fontSize: '11px', flexWrap: 'wrap' }}>
          <span>
            <span style={{ color: 'var(--text-dim)' }}>Status: </span>
            <span
              style={{
                color: summary.is_complete
                  ? 'var(--green)'
                  : summary.idle
                    ? 'var(--yellow)'
                    : 'var(--cyan)',
              }}
            >
              {summary.is_complete ? 'Complete' : summary.idle ? 'Idle' : 'Running'}
            </span>
          </span>
          <span>
            <span style={{ color: 'var(--text-dim)' }}>Elapsed: </span>
            {formatDuration(summary.elapsed_secs)}
          </span>
          {summary.batch_size && summary.batch_size > 0 && (
            <span>
              <span style={{ color: 'var(--text-dim)' }}>Batch: </span>
              {summary.batch_size}
            </span>
          )}
        </div>
      </div>

      {/* Progress Bar */}
      <div className="progress-bar-outer">
        <div
          className="progress-bar-inner"
          style={{ width: `${summary.progress_percent}%` }}
        />
        {failedPercent > 0 && (
          <div
            className="progress-bar-failed"
            style={{
              width: `${failedPercent}%`,
              left: `${summary.progress_percent}%`,
            }}
          />
        )}
        <span className="progress-label">{summary.progress_percent.toFixed(1)}%</span>
      </div>

      {/* Task Stats */}
      <div
        style={{
          display: 'flex',
          gap: '16px',
          marginTop: '8px',
          fontSize: '11px',
          flexWrap: 'wrap',
        }}
      >
        <span>
          <strong style={{ color: 'var(--green)' }}>{summary.completed_tasks}</strong>{' '}
          done
        </span>
        <span>
          <strong style={{ color: 'var(--cyan)' }}>{summary.processing_tasks}</strong>{' '}
          processing
        </span>
        <span>
          <strong style={{ color: 'var(--text-dim)' }}>{summary.pending_tasks}</strong>{' '}
          pending
        </span>
        {summary.failed_tasks > 0 && (
          <span>
            <strong style={{ color: 'var(--red)' }}>{summary.failed_tasks}</strong> failed
          </span>
        )}
      </div>

      {/* Throughput & ETA */}
      <div className="stats-grid" style={{ marginTop: '12px' }}>
        <div className="stat-item">
          <span className="stat-label">Throughput</span>
          <span className="stat-value cyan">
            {summary.cluster_items_per_sec.toLocaleString(undefined, {
              maximumFractionDigits: 0,
            })}
          </span>
          <span style={{ fontSize: '10px', color: 'var(--text-dim)' }}>rows/sec</span>
        </div>
        <div className="stat-item">
          <span className="stat-label">ETA</span>
          <span className="stat-value">
            {summary.eta_secs ? formatDuration(summary.eta_secs) : '--'}
          </span>
        </div>
        <div className="stat-item">
          <span className="stat-label">Total Items</span>
          <span className="stat-value">
            {summary.total_items.toLocaleString()}
          </span>
        </div>
      </div>

      {/* Cancel Button */}
      <div style={{ marginTop: '16px' }}>
        <button
          className="btn btn-danger"
          onClick={handleCancel}
          disabled={summary.is_complete}
        >
          Cancel Job
        </button>
      </div>
    </div>
  );
};

export default JobSummaryPanel;
