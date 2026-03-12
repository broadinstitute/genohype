import { useAtomValue } from 'jotai';
import { useState, useEffect } from 'react';
import {
  clusterConfigAtom,
  clusterVmsAtom,
  workersAtom,
} from '../../atoms/dashboardAtoms';
import type { ScaleResponse } from '../../types';
import '../panels.css';

export const ClusterManagerPanel: React.FC = () => {
  const config = useAtomValue(clusterConfigAtom);
  const vms = useAtomValue(clusterVmsAtom);
  const workers = useAtomValue(workersAtom);

  const [targetWorkers, setTargetWorkers] = useState<number>(0);
  const [loading, setLoading] = useState(false);
  const [statusMessage, setStatusMessage] = useState<string | null>(null);

  const workerVms = vms.filter((v) => v.name.includes('-worker-'));
  const coordinatorVms = vms.filter((v) => v.name.includes('-coordinator'));
  const connectedWorkerCount = workers.length;

  // Derive machine type and spot from actual worker VMs
  const workerMachineType = workerVms[0]?.machine_type
    ? shortName(workerVms[0].machine_type)
    : config?.machine_type ?? '—';
  const isSpot = config?.spot ?? true;

  // Initialize target from current VM count
  useEffect(() => {
    if (config) {
      setTargetWorkers(workerVms.length);
    }
  }, [config?.pool_name]); // Only re-init when pool changes

  function shortName(fullPath: string) {
    const parts = fullPath.split('/');
    return parts[parts.length - 1] || fullPath;
  }

  const shortZone = (zone: string) => {
    const parts = zone.split('/');
    return parts[parts.length - 1] || zone;
  };

  const handleApply = async () => {
    setLoading(true);
    setStatusMessage(null);
    try {
      const response = await fetch('/api/cluster/scale', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ target_workers: targetWorkers }),
      });
      const result: ScaleResponse = await response.json();
      setStatusMessage(result.message);
    } catch (e) {
      setStatusMessage(`Error: ${e}`);
    } finally {
      setLoading(false);
    }
  };

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'RUNNING': return 'var(--green)';
      case 'PROVISIONING': case 'STAGING': return 'var(--yellow)';
      case 'TERMINATED': case 'STOPPED': return 'var(--red)';
      default: return 'var(--text-dim)';
    }
  };

  const configRow = (label: string, value: string) => (
    <div style={{ display: 'flex', justifyContent: 'space-between', padding: '4px 0', borderBottom: '1px solid var(--border)' }}>
      <span style={{ color: 'var(--text-dim)' }}>{label}</span>
      <span>{value}</span>
    </div>
  );

  return (
    <div className="panel-container" style={{ display: 'flex', gap: '16px' }}>
      {/* Left Pane: Configuration */}
      <div style={{ flex: '0 0 320px', borderRight: '1px solid var(--border)', paddingRight: '16px' }}>
        <div className="panel-title">Cluster Configuration</div>

        <div style={{ marginBottom: '12px' }}>
          {configRow('Pool', config?.pool_name ?? '—')}
          {configRow('Project', config?.gcp_project ?? '—')}
          {configRow('Zone', config?.gcp_zone ?? (vms.length > 0 ? shortZone(vms[0].zone) : '—'))}
          {configRow('Machine Type', workerMachineType)}
          {configRow('Spot', isSpot ? 'Yes' : 'No')}
          {configRow('Network', config?.network ?? '—')}
        </div>

        <div className="panel-title" style={{ marginTop: '8px' }}>Scale Workers</div>

        <label style={{ display: 'block', marginBottom: '8px' }}>
          <span style={{ color: 'var(--text-dim)', fontSize: '11px', display: 'block', marginBottom: '2px' }}>
            Target Workers (currently {workerVms.length})
          </span>
          <input
            type="number"
            min={0}
            max={50}
            value={targetWorkers}
            onChange={(e) => setTargetWorkers(parseInt(e.target.value) || 0)}
            style={{
              width: '100%',
              padding: '6px 8px',
              background: 'var(--bg)',
              border: '1px solid var(--border)',
              borderRadius: '4px',
              color: 'var(--text)',
              fontSize: '12px',
            }}
          />
        </label>

        <button
          className="btn-primary"
          onClick={handleApply}
          disabled={loading || targetWorkers === workerVms.length}
          style={{
            width: '100%',
            padding: '8px',
            borderRadius: '4px',
            border: 'none',
            cursor: loading ? 'wait' : 'pointer',
            fontSize: '12px',
            fontWeight: 600,
            opacity: loading || targetWorkers === workerVms.length ? 0.6 : 1,
          }}
        >
          {loading
            ? 'Applying...'
            : targetWorkers === workerVms.length
              ? 'No Change'
              : targetWorkers > workerVms.length
                ? `Scale Up (+${targetWorkers - workerVms.length})`
                : `Scale Down (-${workerVms.length - targetWorkers})`}
        </button>

        {statusMessage && (
          <div style={{
            marginTop: '8px',
            padding: '8px',
            background: 'var(--bg)',
            border: '1px solid var(--border)',
            borderRadius: '4px',
            fontSize: '11px',
            color: statusMessage.startsWith('Error') ? 'var(--red)' : 'var(--green)',
          }}>
            {statusMessage}
          </div>
        )}
      </div>

      {/* Right Pane: GCP Status */}
      <div style={{ flex: 1, overflow: 'auto' }}>
        <div className="panel-title">GCP VM Status</div>

        {/* Summary cards */}
        <div style={{ display: 'flex', gap: '12px', marginBottom: '12px' }}>
          {[
            { value: workerVms.length, label: 'Provisioned', color: 'var(--cyan)' },
            { value: connectedWorkerCount, label: 'Connected', color: 'var(--green)' },
            { value: vms.length, label: 'Total VMs', color: 'var(--text-dim)' },
          ].map(({ value, label, color }) => (
            <div key={label} style={{
              flex: 1, padding: '10px', background: 'var(--bg)',
              border: '1px solid var(--border)', borderRadius: '4px', textAlign: 'center',
            }}>
              <div style={{ fontSize: '20px', fontWeight: 700, color }}>{value}</div>
              <div style={{ fontSize: '10px', color: 'var(--text-dim)', textTransform: 'uppercase' }}>{label}</div>
            </div>
          ))}
        </div>

        {/* Coordinator */}
        {coordinatorVms.length > 0 && (
          <div style={{ marginBottom: '12px' }}>
            <div style={{ fontSize: '11px', color: 'var(--text-dim)', marginBottom: '4px', textTransform: 'uppercase' }}>
              Coordinator
            </div>
            {coordinatorVms.map((vm) => (
              <div key={vm.name} style={{
                display: 'flex', alignItems: 'center', gap: '8px',
                padding: '6px 8px', background: 'var(--bg)',
                border: '1px solid var(--border)', borderRadius: '4px', fontSize: '12px',
              }}>
                <span style={{ width: '8px', height: '8px', borderRadius: '50%', background: getStatusColor(vm.status), flexShrink: 0 }} />
                <span style={{ flex: 1 }}>{vm.name}</span>
                <span style={{ color: 'var(--text-dim)' }}>{shortZone(vm.zone)}</span>
                <span style={{ color: getStatusColor(vm.status) }}>{vm.status}</span>
                <span style={{ color: 'var(--text-dim)' }}>{vm.networkInterfaces?.[0]?.networkIP ?? '—'}</span>
              </div>
            ))}
          </div>
        )}

        {/* Workers table */}
        <div style={{ fontSize: '11px', color: 'var(--text-dim)', marginBottom: '4px', textTransform: 'uppercase' }}>
          Workers ({workerVms.length})
        </div>
        <table className="data-table">
          <thead>
            <tr>
              <th style={{ width: '24px' }}></th>
              <th>Name</th>
              <th>Zone</th>
              <th>Status</th>
              <th>IP</th>
            </tr>
          </thead>
          <tbody>
            {[...workerVms]
              .sort((a, b) => a.name.localeCompare(b.name))
              .map((vm) => (
                <tr key={vm.name}>
                  <td>
                    <span style={{
                      display: 'inline-block', width: '8px', height: '8px',
                      borderRadius: '50%', background: getStatusColor(vm.status),
                    }} />
                  </td>
                  <td>{vm.name}</td>
                  <td style={{ color: 'var(--text-dim)' }}>{shortZone(vm.zone)}</td>
                  <td style={{ color: getStatusColor(vm.status) }}>{vm.status}</td>
                  <td style={{ color: 'var(--text-dim)' }}>{vm.networkInterfaces?.[0]?.networkIP ?? '—'}</td>
                </tr>
              ))}
            {workerVms.length === 0 && (
              <tr>
                <td colSpan={5} style={{ textAlign: 'center', color: 'var(--text-dim)', padding: '16px' }}>
                  No worker VMs found
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
};

export default ClusterManagerPanel;
