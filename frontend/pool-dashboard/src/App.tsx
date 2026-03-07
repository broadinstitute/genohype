import { useAtomValue, useAtom, useSetAtom } from 'jotai';
import {
  useDashboardPolling,
  summaryAtom,
  isLoadedAtom,
  selectedJobIdAtom,
  chartZoomRangeAtom,
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
  const [selectedJobId, setSelectedJobId] = useAtom(selectedJobIdAtom);
  const zoomRange = useAtomValue(chartZoomRangeAtom);
  const setZoomRange = useSetAtom(chartZoomRangeAtom);

  const isViewingHistory = selectedJobId !== 'active';
  const isZoomed = zoomRange !== null;

  return (
    <div className="app">
      <header className="app-header">
        <h1>Genohype Pool Dashboard</h1>
        <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
          {isViewingHistory && (
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <span style={{ fontSize: '12px', color: 'var(--text-dim)' }}>
                Viewing completed job (ID:{' '}
                <span style={{ fontFamily: 'monospace', color: 'var(--text)' }}>
                  {selectedJobId.slice(0, 8)}
                </span>
                )
              </span>
              <button
                onClick={() => setSelectedJobId('active')}
                style={{
                  background: 'transparent',
                  color: 'var(--text)',
                  border: '1px solid var(--border)',
                  padding: '4px 12px',
                  borderRadius: '4px',
                  cursor: 'pointer',
                  fontSize: '11px',
                  fontWeight: 500,
                  transition: 'all 0.2s',
                }}
                onMouseEnter={(e) =>
                  (e.currentTarget.style.background = 'rgba(255,255,255,0.05)')
                }
                onMouseLeave={(e) => (e.currentTarget.style.background = 'transparent')}
              >
                Return to live dashboard
              </button>
            </div>
          )}
          {isZoomed && (
            <button
              onClick={() => setZoomRange(null)}
              style={{
                background: 'transparent',
                color: 'var(--text)',
                border: '1px solid var(--border)',
                padding: '4px 12px',
                borderRadius: '4px',
                cursor: 'pointer',
                fontSize: '11px',
                fontWeight: 500,
                transition: 'all 0.2s',
              }}
              onMouseEnter={(e) =>
                (e.currentTarget.style.background = 'rgba(255,255,255,0.05)')
              }
              onMouseLeave={(e) => (e.currentTarget.style.background = 'transparent')}
            >
              Reset Zoom
            </button>
          )}
          {summary?.build_version && (
            <span style={{ fontSize: '11px', color: 'var(--text-dim)' }}>
              v: {summary.build_version.slice(0, 7)}
            </span>
          )}
          <span className="status-badge">
            {isViewingHistory
              ? 'Historical'
              : summary?.is_complete
              ? 'Complete'
              : summary?.idle
              ? 'Idle'
              : 'Running'}
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
