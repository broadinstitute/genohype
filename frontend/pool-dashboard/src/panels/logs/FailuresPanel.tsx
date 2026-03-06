import { useAtomValue } from 'jotai';
import { failuresAtom } from '../../atoms/dashboardAtoms';
import type { FailureRecord } from '../../types';
import '../panels.css';

/**
 * Displays a dedicated panel for critical errors, timeouts, and stack traces.
 * Separates failure records from routine events for better visibility.
 */
export const FailuresPanel: React.FC = () => {
  const failures = useAtomValue(failuresAtom);

  return (
    <div className="panel-container">
      <h2 className="panel-title" style={{ color: 'var(--red)' }}>
        Critical Failures ({failures.length})
      </h2>
      <div className="log-list">
        {failures.length === 0 ? (
          <div className="empty-state" style={{ height: 'auto', color: 'var(--green)' }}>
            No failures reported
          </div>
        ) : (
          // Show failures in reverse chronological order (newest first)
          [...failures].reverse().map((failure, idx) => (
            <FailureItem key={`${failure.timestamp_ms}-${idx}`} failure={failure} />
          ))
        )}
      </div>
    </div>
  );
};

/**
 * Individual failure record with detailed error information.
 */
const FailureItem: React.FC<{ failure: FailureRecord }> = ({ failure }) => {
  const formatTimestamp = (ms: number): string => {
    return new Date(ms).toLocaleTimeString('en-US', {
      hour12: false,
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
    });
  };

  const formatDuration = (ms: number): string => {
    if (ms >= 60000) {
      return `${(ms / 60000).toFixed(1)}m`;
    }
    return `${(ms / 1000).toFixed(1)}s`;
  };

  return (
    <div className="log-item error">
      {/* Header row */}
      <div style={{ marginBottom: '4px', display: 'flex', gap: '12px', flexWrap: 'wrap' }}>
        <span className="log-timestamp">[{formatTimestamp(failure.timestamp_ms)}]</span>
        <span style={{ color: 'var(--cyan)' }}>
          <strong>Worker:</strong> {failure.worker_id}
        </span>
        {failure.phenotype_id && (
          <span style={{ color: 'var(--purple)' }}>
            <strong>Pheno:</strong> {failure.phenotype_id.split('/').pop()}
          </span>
        )}
        <span style={{ color: 'var(--yellow)' }}>
          <strong>Partitions:</strong> [{failure.partitions.join(', ')}]
        </span>
      </div>

      {/* Meta row */}
      <div
        style={{
          marginBottom: '8px',
          fontSize: '10px',
          display: 'flex',
          gap: '12px',
          color: 'var(--text-dim)',
        }}
      >
        <span>Retry #{failure.retry_count}</span>
        <span>Wasted: {formatDuration(failure.wasted_duration_ms)}</span>
      </div>

      {/* Error message */}
      <div
        style={{
          color: 'var(--red)',
          whiteSpace: 'pre-wrap',
          wordBreak: 'break-word',
          paddingLeft: '8px',
          borderLeft: '2px solid var(--red)',
          fontFamily: 'monospace',
          fontSize: '11px',
          background: 'rgba(248, 81, 73, 0.05)',
          padding: '8px',
          borderRadius: '0 4px 4px 0',
        }}
      >
        {failure.error}
      </div>
    </div>
  );
};

export default FailuresPanel;
