/**
 * Barrel exports for all panel components.
 *
 * These components are designed to be registered with Dockview and can be
 * arranged flexibly in the workspace layout system.
 */

// Overview panels
export { JobSummaryPanel, ClusterEfficiencyPanel } from './overview';

// Worker fleet panel
export { WorkerFleetPanel } from './fleet';

// Granular metric chart panels
export {
  ThroughputChartPanel,
  CpuChartPanel,
  MemoryChartPanel,
  NetworkChartPanel,
  BatchSizeChartPanel,
} from './metrics';

// Log panels
export { EventLogPanel, FailuresPanel } from './logs';

// Domain-specific panels
export { PhenotypeBatchPanel, PhenotypeLibraryPanel, JobConfigPanel, ClickHousePanel } from './domain';

// History panels
export { JobHistoryPanel } from './history';
