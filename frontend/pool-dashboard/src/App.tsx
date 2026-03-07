import { useAtomValue } from 'jotai';
import {
  useDashboardPolling,
  summaryAtom,
  isLoadedAtom,
} from './atoms/dashboardAtoms';
import { DockViewLayout } from './DockViewLayout';
import './App.css';

/**
 * Root application component.
 *
 * Mounts the dashboard polling hook and renders the Dockview workspace.
 */
function App() {
  // Initialize polling on mount
  useDashboardPolling(2000);

  const isLoaded = useAtomValue(isLoadedAtom);
  const summary = useAtomValue(summaryAtom);

  return (
    <div className="app">
      <header className="app-header">
        <h1>Genohype Pool Dashboard</h1>
        <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
          {summary?.build_version && (
            <span style={{ fontSize: '11px', color: 'var(--text-dim)' }}>
              v: {summary.build_version.slice(0, 7)}
            </span>
          )}
          <span className="status-badge">
            {summary?.is_complete ? 'Complete' : summary?.idle ? 'Idle' : 'Running'}
          </span>
        </div>
      </header>

      <main className="app-main" style={{ padding: 0 }}>
        {!isLoaded ? (
          <div className="loading">
            <p>Connecting to coordinator...</p>
          </div>
        ) : (
          <DockViewLayout />
        )}
      </main>
    </div>
  );
}

export default App;
