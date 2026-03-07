import { useAtom } from 'jotai';
import { layoutPresetAtom } from './atoms/dashboardAtoms';

/**
 * Toolbar for switching between workspace layout presets.
 * Reads and writes to layoutPresetAtom, which is persisted to localStorage.
 */
export const WorkspaceToolbar: React.FC = () => {
  const [preset, setPreset] = useAtom(layoutPresetAtom);

  const handlePresetChange = (newPreset: string) => {
    setPreset(newPreset);
  };

  const getButtonStyle = (buttonPreset: string): React.CSSProperties => ({
    background: preset === buttonPreset ? 'var(--accent, #58a6ff)' : 'transparent',
    color: preset === buttonPreset ? '#fff' : 'var(--text, #e6edf3)',
    border: '1px solid var(--border, #30363d)',
    padding: '4px 12px',
    borderRadius: '4px',
    cursor: 'pointer',
    fontSize: '11px',
    fontWeight: preset === buttonPreset ? 600 : 400,
    transition: 'all 0.2s ease',
  });

  return (
    <div
      style={{
        display: 'flex',
        gap: '8px',
        padding: '8px 12px',
        background: 'var(--surface, #161b22)',
        borderBottom: '1px solid var(--border, #30363d)',
      }}
    >
      <span
        style={{
          color: 'var(--text-dim, #7d8590)',
          alignSelf: 'center',
          marginRight: '4px',
          fontSize: '11px',
          textTransform: 'uppercase',
          letterSpacing: '0.5px',
        }}
      >
        Workspace:
      </span>
      <button onClick={() => handlePresetChange('overview')} style={getButtonStyle('overview')}>
        Overview
      </button>
      <button
        onClick={() => handlePresetChange('performance')}
        style={getButtonStyle('performance')}
      >
        Performance
      </button>
      <button onClick={() => handlePresetChange('debug')} style={getButtonStyle('debug')}>
        Debug
      </button>
      <button onClick={() => handlePresetChange('fleet')} style={getButtonStyle('fleet')}>
        Fleet
      </button>
    </div>
  );
};

export default WorkspaceToolbar;
