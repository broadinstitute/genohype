import { useState, useMemo } from 'react';
import { useAtomValue } from 'jotai';
import { catalogAtom, summaryAtom } from '../../atoms/dashboardAtoms';
import '../panels.css';

export const PhenotypeLibraryPanel: React.FC = () => {
  const catalog = useAtomValue(catalogAtom);
  const summary = useAtomValue(summaryAtom);
  const [configPath, setConfigPath] = useState('/Users/msolomon/data/axaou-local/phenotype-data.toml');
  const [filter, setFilter] = useState('');
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());

  const handleLoadConfig = async () => {
    try {
      const res = await fetch('/api/catalog/load', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ config_path: configPath }),
      });
      const data = await res.json();
      if (!data.success) {
        alert('Failed to load config: ' + data.error);
      }
    } catch (e) {
      console.error(e);
      alert('Error loading config.');
    }
  };

  const handleAction = async (endpoint: string) => {
    if (selectedIds.size === 0) return;

    const phenotypes = Array.from(selectedIds).map(key => {
      const [id, ancestry] = key.split('::');
      return [id, ancestry];
    });

    try {
      const res = await fetch(endpoint, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ phenotypes }),
      });
      const data = await res.json();
      if (data.success) {
        // Clear selection on success
        setSelectedIds(new Set());
      } else {
        alert('Action failed: ' + data.error);
      }
    } catch (e) {
      console.error(e);
      alert('Error communicating with server.');
    }
  };

  const filteredCatalog = useMemo(() => {
    if (!filter) return catalog;
    const lower = filter.toLowerCase();
    return catalog.filter(c =>
      c.id.toLowerCase().includes(lower) ||
      (c.description && c.description.toLowerCase().includes(lower))
    );
  }, [catalog, filter]);

  const toggleSelect = (key: string) => {
    const next = new Set(selectedIds);
    if (next.has(key)) next.delete(key);
    else next.add(key);
    setSelectedIds(next);
  };

  const toggleAll = () => {
    if (selectedIds.size === filteredCatalog.length) {
      setSelectedIds(new Set());
    } else {
      setSelectedIds(new Set(filteredCatalog.map(c => `${c.id}::${c.ancestry}`)));
    }
  };

  const isBusy = summary && !summary.idle;

  return (
    <div className="panel-container" style={{ display: 'flex', flexDirection: 'column' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '12px', alignItems: 'center' }}>
        <h2 className="panel-title" style={{ margin: 0 }}>Phenotype Library ({catalog.length})</h2>
        <div style={{ display: 'flex', gap: '8px' }}>
          <input
            value={configPath}
            onChange={e => setConfigPath(e.target.value)}
            placeholder="TOML Config Path"
            style={{ padding: '4px 8px', background: 'var(--bg)', color: 'var(--text)', border: '1px solid var(--border)', borderRadius: '4px', width: '300px', fontSize: '11px' }}
          />
          <button className="btn btn-primary" onClick={handleLoadConfig}>Load Config</button>
        </div>
      </div>

      <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '12px', alignItems: 'center' }}>
        <input
          value={filter}
          onChange={e => setFilter(e.target.value)}
          placeholder="Filter by ID or Description..."
          style={{ padding: '4px 8px', background: 'var(--bg)', color: 'var(--text)', border: '1px solid var(--border)', borderRadius: '4px', fontSize: '11px', width: '200px' }}
        />
        <div style={{ display: 'flex', gap: '8px' }}>
          <button
            className="btn btn-primary"
            disabled={selectedIds.size === 0}
            onClick={() => handleAction('/api/catalog/process')}
          >
            Process Selected ({selectedIds.size})
          </button>
          <button
            className="btn btn-primary"
            style={{ background: 'var(--orange)' }}
            disabled={selectedIds.size === 0 || !!isBusy}
            title={isBusy ? "Coordinator is busy" : ""}
            onClick={() => handleAction('/api/catalog/ingest')}
          >
            Ingest Selected ({selectedIds.size})
          </button>
        </div>
      </div>

      <div style={{ flex: 1, overflowY: 'auto' }}>
        <table className="data-table">
          <thead>
            <tr>
              <th style={{ width: '30px', textAlign: 'center' }}>
                <input type="checkbox" checked={selectedIds.size > 0 && selectedIds.size === filteredCatalog.length} onChange={toggleAll} />
              </th>
              <th>Status</th>
              <th>Phenotype ID</th>
              <th>Ancestry</th>
              <th>Description</th>
              <th>Trait Type</th>
              <th>Cases / Controls</th>
              <th>Assets</th>
            </tr>
          </thead>
          <tbody>
            {filteredCatalog.map(entry => {
              const key = `${entry.id}::${entry.ancestry}`;
              const isSelected = selectedIds.has(key);

              return (
                <tr key={key} style={{ background: isSelected ? 'rgba(88, 166, 255, 0.1)' : undefined }} onClick={() => toggleSelect(key)}>
                  <td style={{ textAlign: 'center' }} onClick={e => e.stopPropagation()}>
                    <input type="checkbox" checked={isSelected} onChange={() => toggleSelect(key)} />
                  </td>
                  <td>
                    {entry.status !== 'idle' && (
                      <span className={`stage-badge ${entry.status}`}>{entry.status}</span>
                    )}
                  </td>
                  <td style={{ color: 'var(--cyan)', fontWeight: 600 }}>{entry.id}</td>
                  <td>{entry.ancestry}</td>
                  <td>{entry.description || '--'}</td>
                  <td>{entry.trait_type || '--'}</td>
                  <td>{entry.n_cases ? `${entry.n_cases} / ${entry.n_controls || 0}` : '--'}</td>
                  <td>
                    <div style={{ display: 'flex', gap: '4px' }}>
                      {entry.has_exome && <span style={{ background: 'var(--cyan)', color: '#000', padding: '2px 4px', borderRadius: '3px', fontSize: '9px', fontWeight: 'bold' }}>E</span>}
                      {entry.has_genome && <span style={{ background: 'var(--cyan)', color: '#000', padding: '2px 4px', borderRadius: '3px', fontSize: '9px', fontWeight: 'bold' }}>G</span>}
                      {entry.has_gene_burden && <span style={{ background: 'var(--purple)', color: '#000', padding: '2px 4px', borderRadius: '3px', fontSize: '9px', fontWeight: 'bold' }}>B</span>}
                    </div>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
        {filteredCatalog.length === 0 && (
          <div className="empty-state" style={{ padding: '24px' }}>
            No phenotypes available. Load a config file first.
          </div>
        )}
      </div>
    </div>
  );
};

export default PhenotypeLibraryPanel;
