import { useEffect, useState } from 'react';
import { DockviewReact, DockviewReadyEvent, DockviewApi } from 'dockview';
import 'dockview/dist/styles/dockview.css';
import { useAtom, useAtomValue } from 'jotai';
import { layoutPresetAtom, summaryAtom } from './atoms/dashboardAtoms';
import { WorkspaceToolbar } from './WorkspaceToolbar';

import {
  JobSummaryPanel,
  ClusterEfficiencyPanel,
  WorkerFleetPanel,
  ThroughputChartPanel,
  CpuChartPanel,
  MemoryChartPanel,
  NetworkChartPanel,
  BatchSizeChartPanel,
  EventLogPanel,
  FailuresPanel,
  PhenotypeBatchPanel,
  PhenotypeLibraryPanel,
  JobConfigPanel,
  JobHistoryPanel,
} from './panels';

// Map React components to string identifiers for Dockview
const components: Record<string, React.FC> = {
  jobSummary: JobSummaryPanel,
  clusterEfficiency: ClusterEfficiencyPanel,
  workerFleet: WorkerFleetPanel,
  throughputChart: ThroughputChartPanel,
  cpuChart: CpuChartPanel,
  memoryChart: MemoryChartPanel,
  networkChart: NetworkChartPanel,
  batchSizeChart: BatchSizeChartPanel,
  eventLog: EventLogPanel,
  failures: FailuresPanel,
  phenotypeBatch: PhenotypeBatchPanel,
  phenotypeLibrary: PhenotypeLibraryPanel,
  jobConfig: JobConfigPanel,
  jobHistory: JobHistoryPanel,
};

/**
 * Clears all panels from the Dockview instance.
 * Uses a shallow copy to iterate since closing panels mutates the array.
 */
export const clearPanels = (api: DockviewApi) => {
  const panels = [...api.panels];
  panels.forEach((panel) => {
    panel.api.close();
  });
};

/**
 * Applies the Overview layout preset:
 * - Left sidebar: Job Summary (top), Cluster Efficiency (bottom)
 * - Main area: Worker Fleet (top), optionally Phenotype Batch as tab
 * - Bottom panel: Event Log
 */
export const applyOverviewLayout = (api: DockviewApi, isBatchJob: boolean) => {
  clearPanels(api);

  const summary = api.addPanel({
    id: 'job_summary',
    component: 'jobSummary',
    title: 'Job Summary',
  });

  const fleet = api.addPanel({
    id: 'worker_fleet',
    component: 'workerFleet',
    title: 'Worker Fleet',
    position: { direction: 'right', referencePanel: summary.id },
  });

  // Always add Library to let user enqueue jobs dynamically
  const library = api.addPanel({
    id: 'phenotype_library',
    component: 'phenotypeLibrary',
    title: 'Phenotype Library',
    position: { direction: 'within', referencePanel: fleet.id },
  });

  // Dynamically add domain-specific batch panel if the job requires them
  if (isBatchJob) {
    api.addPanel({
      id: 'phenotype_batch',
      component: 'phenotypeBatch',
      title: 'Phenotype Batch',
      position: { direction: 'within', referencePanel: library.id },
    });
  }

  api.addPanel({
    id: 'cluster_efficiency',
    component: 'clusterEfficiency',
    title: 'Cluster Efficiency',
    position: { direction: 'below', referencePanel: summary.id },
  });

  api.addPanel({
    id: 'event_log',
    component: 'eventLog',
    title: 'Event Log',
    position: { direction: 'below', referencePanel: fleet.id },
  });
};

/**
 * Applies the Performance layout preset:
 * 2-column layout:
 * - Column 1: Job Summary/Cluster Efficiency (tabbed) → Batch Size → Throughput
 * - Column 2: CPU → Memory → Network
 */
export const applyPerformanceLayout = (api: DockviewApi) => {
  clearPanels(api);

  // Column 1: Job Summary (root)
  const jobSummary = api.addPanel({
    id: 'job_summary',
    component: 'jobSummary',
    title: 'Job Summary',
  });

  // Column 1: Cluster Efficiency (tabbed with Job Summary)
  api.addPanel({
    id: 'cluster_efficiency',
    component: 'clusterEfficiency',
    title: 'Cluster Efficiency',
    position: { direction: 'within', referencePanel: jobSummary.id },
  });

  // Column 2: CPU (right of Job Summary, creates 2-column split)
  const cpuChart = api.addPanel({
    id: 'cpu_chart',
    component: 'cpuChart',
    title: 'CPU',
    position: { direction: 'right', referencePanel: jobSummary.id },
  });

  // Column 2: Memory (below CPU)
  const memoryChart = api.addPanel({
    id: 'memory_chart',
    component: 'memoryChart',
    title: 'Memory',
    position: { direction: 'below', referencePanel: cpuChart.id },
  });

  // Column 1: Batch Size (below Job Summary/Efficiency tabs)
  const batchSize = api.addPanel({
    id: 'batch_size_chart',
    component: 'batchSizeChart',
    title: 'Batch Size',
    position: { direction: 'below', referencePanel: jobSummary.id },
  });

  // Column 2: Network (below Memory)
  api.addPanel({
    id: 'network_chart',
    component: 'networkChart',
    title: 'Network',
    position: { direction: 'below', referencePanel: memoryChart.id },
  });

  // Column 1: Throughput (below Batch Size)
  api.addPanel({
    id: 'throughput_chart',
    component: 'throughputChart',
    title: 'Throughput',
    position: { direction: 'below', referencePanel: batchSize.id },
  });
};

/**
 * Applies the Debug layout preset:
 * - Top area: Worker Fleet
 * - Bottom area: Failures (left), Event Log (right)
 */
export const applyDebugLayout = (api: DockviewApi) => {
  clearPanels(api);

  const fleet = api.addPanel({
    id: 'worker_fleet',
    component: 'workerFleet',
    title: 'Worker Fleet',
  });

  const failures = api.addPanel({
    id: 'failures',
    component: 'failures',
    title: 'Failures',
    position: { direction: 'below', referencePanel: fleet.id },
  });

  api.addPanel({
    id: 'event_log',
    component: 'eventLog',
    title: 'Event Log',
    position: { direction: 'right', referencePanel: failures.id },
  });
};

/**
 * Applies the Fleet layout preset:
 * - Full-screen Worker Fleet panel for detailed worker monitoring
 */
export const applyFleetLayout = (api: DockviewApi) => {
  clearPanels(api);

  api.addPanel({
    id: 'worker_fleet',
    component: 'workerFleet',
    title: 'Worker Fleet',
  });
};

/**
 * Applies the Phenotype layout preset:
 * - Left sidebar: Job Config
 * - Main area: Phenotype Library (top), Phenotype Batch (tabbed)
 * - Bottom: Event Log
 */
export const applyPhenotypeLayout = (api: DockviewApi) => {
  clearPanels(api);

  const config = api.addPanel({
    id: 'job_config',
    component: 'jobConfig',
    title: 'Job Config',
  });

  const library = api.addPanel({
    id: 'phenotype_library',
    component: 'phenotypeLibrary',
    title: 'Phenotype Library',
    position: { direction: 'right', referencePanel: config.id },
  });

  api.addPanel({
    id: 'phenotype_batch',
    component: 'phenotypeBatch',
    title: 'Phenotype Batch',
    position: { direction: 'within', referencePanel: library.id },
  });

  api.addPanel({
    id: 'event_log',
    component: 'eventLog',
    title: 'Event Log',
    position: { direction: 'below', referencePanel: library.id },
  });
};

/**
 * Applies the History layout preset:
 * - Full-screen Job History panel for browsing past jobs
 */
export const applyHistoryLayout = (api: DockviewApi) => {
  clearPanels(api);

  api.addPanel({
    id: 'job_history',
    component: 'jobHistory',
    title: 'Job History',
  });
};

/**
 * Main Dockview layout component.
 * Registers all Phase 3 panels and reacts to layoutPresetAtom changes
 * to apply the corresponding workspace layout.
 */
export const DockViewLayout: React.FC = () => {
  const [api, setApi] = useState<DockviewApi>();
  const [preset] = useAtom(layoutPresetAtom);
  const summary = useAtomValue(summaryAtom);

  // Determine if this is a ManhattanBatch job for conditional panel rendering
  const isBatchJob =
    summary?.job_spec && (summary.job_spec as Record<string, unknown>).type === 'ManhattanBatch';

  // Listen for preset changes or dynamic job spec changes and adjust the layout
  useEffect(() => {
    if (!api) return;

    switch (preset) {
      case 'performance':
        applyPerformanceLayout(api);
        break;
      case 'debug':
        applyDebugLayout(api);
        break;
      case 'fleet':
        applyFleetLayout(api);
        break;
      case 'phenotype':
        applyPhenotypeLayout(api);
        break;
      case 'history':
        applyHistoryLayout(api);
        break;
      case 'overview':
      default:
        applyOverviewLayout(api, !!isBatchJob);
        break;
    }
  }, [api, preset, isBatchJob]);

  const onReady = (event: DockviewReadyEvent) => {
    setApi(event.api);
  };

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%', width: '100%' }}>
      <WorkspaceToolbar />
      <div style={{ flexGrow: 1, position: 'relative' }}>
        <DockviewReact
          components={components}
          onReady={onReady}
          className="dockview-theme-dark"
        />
      </div>
    </div>
  );
};

export default DockViewLayout;
