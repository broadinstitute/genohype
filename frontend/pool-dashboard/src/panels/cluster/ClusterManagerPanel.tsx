import { useAtomValue } from 'jotai';
import { useState, useEffect } from 'react';
import {
  clusterConfigAtom,
  clusterVmsAtom,
  workersAtom,
  activeWorkerCountAtom,
} from '../../atoms/dashboardAtoms';
import type { ScaleRequest, ScaleResponse } from '../../types';
import '../panels.css';

export const ClusterManagerPanel: React.FC = () => {
  const config = useAtomValue(clusterConfigAtom);
  const vms = useAtomValue(clusterVmsAtom);
  const workers = useAtomValue(workersAtom);
  const activeWorkerCount = useAtomValue(activeWorkerCountAtom);

  // Form state
  const [targetWorkers, setTargetWorkers] = useState<number>(0);
  const [machineType, setMachineType] = useState<string>('');
  const [spot, setSpot] = useState<boolean>(true);
  const [loading, setLoading] = useState(false);
  const [statusMessage, setStatusMessage] = useState<string | null>(null);

  // Initialize form from config
  useEffect(() => {
    if (config) {
      // Count current worker VMs to set initial target
      const workerVmCount = vms.filter((v) => v.name.includes('-worker-')).length;
      setTargetWorkers(workerVmCount);
      if (config.machine_type) setMachineType(config.machine_type);
      if (config.spot !== null) setSpot(config.spot);
    }
  }, [config?.pool_name]); // Only re-init when pool changes, not on every poll

  const workerVms = vms.filter((v) => v.name.includes('-worker-'));
  const coordinatorVms = vms.filter((v) => v.name.includes('-coordinator'));

  // Set of active worker IDs from dashboard
  const activeWorkerIds = new Set(
    workers.filter((w) => w.status === 'active').map((w) => w.worker_id)
  );

  const handleApply = async () => {
    setLoading(true);
    setStatusMessage(null);
    try {
      const req: ScaleRequest = {
        target_workers: targetWorkers,
        machine_type: machineType || undefined,
        spot,
      };
      const response = await fetch('/api/cluster/scale', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(req),
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
      case 'RUNNING':
        return 'var(--green)';
      case 'PROVISIONING':
      case 'STAGING':
        return 'var(--yellow)';
      case 'TERMINATED':
      case 'STOPPED':
        return 'var(--red)';
      default:
        return 'var(--text-dim)';
    }
  };

  const isVmConnected = (vmName: string): boolean => {
    // Worker IDs in the dashboard match instance names
    return activeWorkerIds.has(vmName);
  };

  return (
    <div className="panel-container" style={{ display: 'flex', gap: '16px' }}>
      {/* Left Pane: Configuration */}
      <div style={{ flex: '0 0 320px', borderRight: '1px solid var(--border)', paddingRight: '16px' }}>
        <div className="panel-title">Cluster Configuration</div>

        {/* Read-only fields */}
        <div style={{ marginBottom: '12px' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', padding: '4px 0', borderBottom: '1px solid var(--border)' }}>
            <span style={{ color: 'var(--text-dim)' }}>Pool</span>
            <span>{config?.pool_name ?? '—'}</span>
          </div>
          <div style={{ display: 'flex', justifyContent: 'space-between', padding: '4px 0', borderBottom: '1px solid var(--border)' }}>
            <span style={{ color: 'var(--text-dim)' }}>Project</span>
            <span>{config?.gcp_project ?? '—'}</span>
          </div>
          <div style={{ display: 'flex', justifyContent: 'space-between', padding: '4px 0', borderBottom: '1px solid var(--border)' }}>
            <span style={{ color: 'var(--text-dim)' }}>Zone</span>
            <span>{config?.gcp_zone ?? '—'}</span>
          </div>
          <div style={{ display: 'flex', justifyContent: 'space-between', padding: '4px 0', borderBottom: '1px solid var(--border)' }}>
            <span style={{ color: 'var(--text-dim)' }}>Network</span>
            <span>{config?.network ?? 'default'}</span>
          </div>
        </div>

        {/* Editable fields */}
        <div style={{ marginBottom: '12px' }}>
          <div className="panel-title" style={{ marginTop: '8px' }}>Scale Controls</div>

          <label style={{ display: 'block', marginBottom: '8px' }}>
            <span style={{ color: 'var(--text-dim)', fontSize: '11px', display: 'block', marginBottom: '2px' }}>Target Workers</span>
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

          <label style={{ display: 'block', marginBottom: '8px' }}>
            <span style={{ color: 'var(--text-dim)', fontSize: '11px', display: 'block', marginBottom: '2px' }}>Machine Type</span>
            <input
              type="text"
              value={machineType}
              onChange={(e) => setMachineType(e.target.value)}
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

          <label style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '12px' }}>
            <input
              type="checkbox"
              checked={spot}
              onChange={(e) => setSpot(e.target.checked)}
            />
            <span style={{ color: 'var(--text-dim)', fontSize: '11px' }}>Spot Instances</span>
          </label>

          <button
            className="btn-primary"
            onClick={handleApply}
            disabled={loading}
            style={{
              width: '100%',
              padding: '8px',
              borderRadius: '4px',
              border: 'none',
              cursor: loading ? 'wait' : 'pointer',
              fontSize: '12px',
              fontWeight: 600,
              opacity: loading ? 0.6 : 1,
            }}
          >
            {loading ? 'Applying...' : 'Apply Changes'}
          </button>

          {statusMessage && (
            <div
              style={{
                marginTop: '8px',
                padding: '8px',
                background: 'var(--bg)',
                border: '1px solid var(--border)',
                borderRadius: '4px',
                fontSize: '11px',
                color: statusMessage.startsWith('Error') ? 'var(--red)' : 'var(--green)',
              }}
            >
              {statusMessage}
            </div>
          )}
        </div>
      </div>

      {/* Right Pane: GCP Status */}
      <div style={{ flex: 1, overflow: 'auto' }}>
        <div className="panel-title">GCP VM Status</div>

        {/* Summary cards */}
        <div style={{ display: 'flex', gap: '12px', marginBottom: '12px' }}>
          <div
            style={{
              flex: 1,
              padding: '10px',
              background: 'var(--bg)',
              border: '1px solid var(--border)',
              borderRadius: '4px',
              textAlign: 'center',
            }}
          >
            <div style={{ fontSize: '20px', fontWeight: 700, color: 'var(--accent)' }}>
              {targetWorkers}
            </div>
            <div style={{ fontSize: '10px', color: 'var(--text-dim)', textTransform: 'uppercase' }}>
              Target
            </div>
          </div>
          <div
            style={{
              flex: 1,
              padding: '10px',
              background: 'var(--bg)',
              border: '1px solid var(--border)',
              borderRadius: '4px',
              textAlign: 'center',
            }}
          >
            <div style={{ fontSize: '20px', fontWeight: 700, color: 'var(--cyan)' }}>
              {workerVms.length}
            </div>
            <div style={{ fontSize: '10px', color: 'var(--text-dim)', textTransform: 'uppercase' }}>
              Provisioned
            </div>
          </div>
          <div
            style={{
              flex: 1,
              padding: '10px',
              background: 'var(--bg)',
              border: '1px solid var(--border)',
              borderRadius: '4px',
              textAlign: 'center',
            }}
          >
            <div style={{ fontSize: '20px', fontWeight: 700, color: 'var(--green)' }}>
              {activeWorkerCount}
            </div>
            <div style={{ fontSize: '10px', color: 'var(--text-dim)', textTransform: 'uppercase' }}>
              Connected
            </div>
          </div>
        </div>

        {/* Coordinator section */}
        {coordinatorVms.length > 0 && (
          <div style={{ marginBottom: '12px' }}>
            <div style={{ fontSize: '11px', color: 'var(--text-dim)', marginBottom: '4px', textTransform: 'uppercase' }}>
              Coordinator
            </div>
            {coordinatorVms.map((vm) => (
              <div
                key={vm.name}
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: '8px',
                  padding: '6px 8px',
                  background: 'var(--bg)',
                  border: '1px solid var(--border)',
                  borderRadius: '4px',
                  fontSize: '12px',
                }}
              >
                <span
                  style={{
                    width: '8px',
                    height: '8px',
                    borderRadius: '50%',
                    background: getStatusColor(vm.status),
                    flexShrink: 0,
                  }}
                />
                <span style={{ flex: 1 }}>{vm.name}</span>
                <span style={{ color: 'var(--text-dim)' }}>{vm.zone}</span>
                <span style={{ color: getStatusColor(vm.status) }}>{vm.status}</span>
                <span style={{ color: 'var(--text-dim)' }}>
                  {vm.networkInterfaces?.[0]?.networkIP ?? '—'}
                </span>
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
              <th>Dashboard</th>
            </tr>
          </thead>
          <tbody>
            {workerVms
              .sort((a, b) => a.name.localeCompare(b.name))
              .map((vm) => {
                const connected = isVmConnected(vm.name);
                return (
                  <tr key={vm.name}>
                    <td>
                      <span
                        style={{
                          display: 'inline-block',
                          width: '8px',
                          height: '8px',
                          borderRadius: '50%',
                          background:
                            vm.status === 'RUNNING' && connected
                              ? 'var(--green)'
                              : getStatusColor(vm.status),
                        }}
                      />
                    </td>
                    <td>{vm.name}</td>
                    <td style={{ color: 'var(--text-dim)' }}>{vm.zone}</td>
                    <td style={{ color: getStatusColor(vm.status) }}>{vm.status}</td>
                    <td style={{ color: 'var(--text-dim)' }}>
                      {vm.networkInterfaces?.[0]?.networkIP ?? '—'}
                    </td>
                    <td>
                      {connected ? (
                        <span style={{ color: 'var(--green)' }}>connected</span>
                      ) : vm.status === 'RUNNING' ? (
                        <span style={{ color: 'var(--yellow)' }}>not connected</span>
                      ) : (
                        <span style={{ color: 'var(--text-dim)' }}>—</span>
                      )}
                    </td>
                  </tr>
                );
              })}
            {workerVms.length === 0 && (
              <tr>
                <td colSpan={6} style={{ textAlign: 'center', color: 'var(--text-dim)', padding: '16px' }}>
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
