import { useAtomValue } from 'jotai';
import {
  useDashboardPolling,
  summaryAtom,
  workersAtom,
  isLoadedAtom,
} from './atoms/dashboardAtoms';
import './App.css';

/**
 * Root application component.
 *
 * Mounts the dashboard polling hook and renders a basic status display.
 * This will be replaced with the full Dockview layout in Phase 3/4.
 */
function App() {
  // Initialize polling on mount
  useDashboardPolling(2000);

  const isLoaded = useAtomValue(isLoadedAtom);
  const summary = useAtomValue(summaryAtom);
  const workers = useAtomValue(workersAtom);

  return (
    <div className="app">
      <header className="app-header">
        <h1>Genohype Pool Dashboard</h1>
        <span className="status-badge">
          {summary?.is_complete ? 'Complete' : summary?.idle ? 'Idle' : 'Running'}
        </span>
      </header>

      <main className="app-main">
        {!isLoaded ? (
          <div className="loading">
            <p>Connecting to coordinator...</p>
          </div>
        ) : (
          <div className="dashboard-content">
            {/* Summary Section */}
            <section className="panel summary-panel">
              <h2>Job Summary</h2>
              {summary && (
                <div className="stats-grid">
                  <div className="stat">
                    <span className="stat-label">Progress</span>
                    <span className="stat-value">
                      {summary.progress_percent.toFixed(1)}%
                    </span>
                  </div>
                  <div className="stat">
                    <span className="stat-label">Partitions</span>
                    <span className="stat-value">
                      {summary.completed_partitions} / {summary.total_partitions}
                    </span>
                  </div>
                  <div className="stat">
                    <span className="stat-label">Throughput</span>
                    <span className="stat-value">
                      {summary.cluster_items_per_sec.toLocaleString(undefined, {
                        maximumFractionDigits: 0,
                      })}{' '}
                      rows/s
                    </span>
                  </div>
                  <div className="stat">
                    <span className="stat-label">ETA</span>
                    <span className="stat-value">
                      {summary.eta_secs
                        ? formatDuration(summary.eta_secs)
                        : '--'}
                    </span>
                  </div>
                </div>
              )}
              {summary && (
                <div className="progress-bar-container">
                  <div
                    className="progress-bar"
                    style={{ width: `${summary.progress_percent}%` }}
                  />
                </div>
              )}
            </section>

            {/* Workers Section */}
            <section className="panel workers-panel">
              <h2>Workers ({workers.length})</h2>
              <div className="workers-grid">
                {workers.map((worker) => (
                  <div
                    key={worker.worker_id}
                    className={`worker-card ${worker.status}`}
                  >
                    <div className="worker-header">
                      <span className="worker-id">{worker.worker_id}</span>
                      <span className={`worker-status status-${worker.status}`}>
                        {worker.status}
                      </span>
                    </div>
                    {worker.telemetry && (
                      <div className="worker-stats">
                        <span>
                          CPU: {worker.telemetry.cpu_percent?.toFixed(0) ?? '--'}%
                        </span>
                        <span>
                          {worker.telemetry.items_per_sec.toLocaleString(undefined, {
                            maximumFractionDigits: 0,
                          })}{' '}
                          rows/s
                        </span>
                      </div>
                    )}
                  </div>
                ))}
              </div>
            </section>
          </div>
        )}
      </main>
    </div>
  );
}

/**
 * Format seconds as HH:MM:SS or MM:SS.
 */
function formatDuration(seconds: number): string {
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  const s = Math.floor(seconds % 60);

  if (h > 0) {
    return `${h}:${m.toString().padStart(2, '0')}:${s.toString().padStart(2, '0')}`;
  }
  return `${m}:${s.toString().padStart(2, '0')}`;
}

export default App;
