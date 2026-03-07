import { useAtomValue, useSetAtom } from 'jotai';
import {
  jobsListAtom,
  selectedJobIdAtom,
  layoutPresetAtom,
  fetchDashboardDataAtom,
} from '../../atoms/dashboardAtoms';
import type { JobRecord } from '../../types';
import '../panels.css';

/**
 * Displays a table of historical jobs with the ability to select one for viewing.
 * Clicking a job switches the dashboard to view that job's historical data.
 */
export const JobHistoryPanel: React.FC = () => {
  const jobs = useAtomValue(jobsListAtom);
  const selectedJobId = useAtomValue(selectedJobIdAtom);
  const setSelectedJobId = useSetAtom(selectedJobIdAtom);
  const setLayoutPreset = useSetAtom(layoutPresetAtom);
  const fetchDashboardData = useSetAtom(fetchDashboardDataAtom);

  const handleJobSelect = (jobId: string) => {
    setSelectedJobId(jobId);
    // Switch to overview to see the job's data
    setLayoutPreset('overview');
  };

  const handleReturnToLive = () => {
    setSelectedJobId('active');
  };

  const handleDeleteJob = async (jobId: string) => {
    if (!window.confirm('Are you sure you want to delete this job and all its metrics?')) {
      return;
    }
    try {
      const res = await fetch(`/api/history/jobs/${jobId}`, { method: 'DELETE' });
      if (res.ok) {
        // If we deleted the currently selected job, return to live view
        if (selectedJobId === jobId) {
          setSelectedJobId('active');
          setLayoutPreset('overview');
        }
        // Refresh the jobs list
        fetchDashboardData();
      } else {
        console.error('Failed to delete job');
      }
    } catch (e) {
      console.error('Failed to delete job:', e);
    }
  };

  return (
    <div className="panel-container">
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '12px' }}>
        <h2 className="panel-title" style={{ margin: 0 }}>Job History</h2>
        {selectedJobId !== 'active' && (
          <button
            onClick={handleReturnToLive}
            style={{
              background: 'var(--orange)',
              color: '#fff',
              border: 'none',
              padding: '4px 12px',
              borderRadius: '4px',
              cursor: 'pointer',
              fontSize: '11px',
              fontWeight: 600,
            }}
          >
            Return to Live
          </button>
        )}
      </div>
      <div className="table-container">
        {jobs.length === 0 ? (
          <div className="empty-state">No job history available</div>
        ) : (
          <table className="data-table">
            <thead>
              <tr>
                <th>Status</th>
                <th>Type</th>
                <th>Started</th>
                <th>Duration</th>
                <th>Tasks</th>
                <th>Input</th>
                <th style={{ width: '60px' }}>Actions</th>
              </tr>
            </thead>
            <tbody>
              {jobs.map((job) => (
                <JobHistoryRow
                  key={job.job_id}
                  job={job}
                  isSelected={job.job_id === selectedJobId}
                  onSelect={() => handleJobSelect(job.job_id)}
                  onDelete={() => handleDeleteJob(job.job_id)}
                />
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
};

interface JobHistoryRowProps {
  job: JobRecord;
  isSelected: boolean;
  onSelect: () => void;
  onDelete: () => void;
}

const JobHistoryRow: React.FC<JobHistoryRowProps> = ({ job, isSelected, onSelect, onDelete }) => {
  const getStatusStyle = (status: string): React.CSSProperties => {
    switch (status) {
      case 'completed':
        return { color: 'var(--green)' };
      case 'running':
        return { color: 'var(--cyan)' };
      case 'failed':
        return { color: 'var(--red)' };
      case 'cancelled':
        return { color: 'var(--yellow)' };
      default:
        return { color: 'var(--text-dim)' };
    }
  };

  const formatDate = (ms: number): string => {
    return new Date(ms).toLocaleString('en-US', {
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
      hour12: false,
    });
  };

  const formatDuration = (startMs: number, endMs?: number): string => {
    const end = endMs ?? Date.now();
    const durationSecs = (end - startMs) / 1000;

    if (durationSecs < 60) return `${Math.round(durationSecs)}s`;
    if (durationSecs < 3600) return `${Math.round(durationSecs / 60)}m`;
    const hours = Math.floor(durationSecs / 3600);
    const mins = Math.round((durationSecs % 3600) / 60);
    return `${hours}h ${mins}m`;
  };

  const getShortInputPath = (path: string): string => {
    // Extract just the filename or last path segment
    const parts = path.split('/');
    const last = parts.pop() || path;
    // If it's a GCS path, show bucket + filename
    if (path.startsWith('gs://')) {
      const bucket = parts[2] || '';
      return `${bucket}/.../${last}`;
    }
    return last.length > 30 ? `...${last.slice(-27)}` : last;
  };

  return (
    <tr
      onClick={onSelect}
      style={{
        cursor: 'pointer',
        background: isSelected ? 'rgba(var(--accent-rgb), 0.2)' : 'transparent',
      }}
    >
      <td>
        <span style={getStatusStyle(job.status)}>
          {job.status === 'running' ? '● ' : ''}{job.status}
        </span>
      </td>
      <td>{job.job_type || 'unknown'}</td>
      <td>{formatDate(job.start_time_ms)}</td>
      <td>{formatDuration(job.start_time_ms, job.end_time_ms)}</td>
      <td>{job.total_tasks.toLocaleString()}</td>
      <td title={job.input_path}>{getShortInputPath(job.input_path)}</td>
      <td>
        <button
          onClick={(e) => {
            e.stopPropagation();
            onDelete();
          }}
          style={{
            background: 'transparent',
            color: 'var(--red)',
            border: '1px solid var(--red)',
            padding: '2px 6px',
            borderRadius: '4px',
            cursor: 'pointer',
            fontSize: '10px',
            fontWeight: 500,
            transition: 'all 0.2s',
          }}
          onMouseEnter={(e) => {
            e.currentTarget.style.background = 'var(--red)';
            e.currentTarget.style.color = '#fff';
          }}
          onMouseLeave={(e) => {
            e.currentTarget.style.background = 'transparent';
            e.currentTarget.style.color = 'var(--red)';
          }}
        >
          Delete
        </button>
      </td>
    </tr>
  );
};

export default JobHistoryPanel;
