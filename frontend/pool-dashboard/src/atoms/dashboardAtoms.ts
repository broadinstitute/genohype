import { atom, useAtomValue, useSetAtom } from 'jotai';
import { atomWithStorage } from 'jotai/utils';
import { useEffect, useRef } from 'react';
import type {
  DashboardSummary,
  DashboardWorker,
  DashboardMetrics,
  DashboardBottleneck,
  BatchStatusResponse,
  JobEvent,
  FailureRecord,
  JobRecord,
  ClickHouseInfo,
  ClusterConfig,
  GcpVm,
} from '../types';

// ============================================================================
// Core State Atoms
// ============================================================================

/** Overall job summary (progress, ETA, partition counts) */
export const summaryAtom = atom<DashboardSummary | null>(null);

/** List of workers with their current status and telemetry */
export const workersAtom = atom<DashboardWorker[]>([]);

/** Time-series metrics for charts (per-worker telemetry history) */
export const metricsAtom = atom<DashboardMetrics | null>(null);

/** Current bottleneck analysis (CPU, Memory, Network, etc.) */
export const bottleneckAtom = atom<DashboardBottleneck | null>(null);

/** Batch phenotype status (only populated for ManhattanBatch jobs) */
export const batchAtom = atom<BatchStatusResponse | null>(null);

/** Historical cluster events (assignments, completions, requeues) */
export const eventsAtom = atom<JobEvent[]>([]);

/** Task failure records with error details */
export const failuresAtom = atom<FailureRecord[]>([]);

/** Catalog of available phenotypes */
export const catalogAtom = atom<import('../types').CatalogEntry[]>([]);

/** ClickHouse storage info */
export const clickhouseInfoAtom = atom<ClickHouseInfo | null>(null);

/** Cluster configuration from coordinator */
export const clusterConfigAtom = atom<ClusterConfig | null>(null);

/** GCP VM instances in the cluster */
export const clusterVmsAtom = atom<GcpVm[]>([]);

// ============================================================================
// Phenotype Library State Atoms (preserve state across tab switches)
// ============================================================================

export const libraryFilterAtom = atom<string>('');
export const libraryAncestryFilterAtom = atom<string>('meta');
export const libraryTraitTypeFilterAtom = atom<string>('');
export const libraryAssetFilterAtom = atom<string>('');
export const libraryStatusFilterAtom = atom<string>('');
export const librarySelectedCategoriesAtom = atom<Set<string>>(new Set<string>());
export const librarySelectedIdsAtom = atom<Set<string>>(new Set<string>());
export const librarySortKeyAtom = atom<'status' | 'id' | 'ancestry' | 'description' | 'trait_type' | 'cases' | 'assets'>('id');
export const librarySortDirAtom = atom<'asc' | 'desc'>('asc');
export const libraryRandomSampleAtom = atom<number>(100);
export const libraryLimitCountAtom = atom<string>('');

// ============================================================================
// UI Preference Atoms (persisted to localStorage)
// ============================================================================

/** Currently active workspace layout preset */
export const layoutPresetAtom = atomWithStorage<string>(
  'dashboardLayoutPreset',
  'overview'
);

/** Whether performance charts should be displayed as stacked area charts */
export const chartsStackedAtom = atomWithStorage<boolean>(
  'chartsStacked',
  false
);

/** Which job is currently being viewed ('active' for live job, or a specific job UUID) */
export const selectedJobIdAtom = atom<string>('active');

/** List of historical jobs */
export const jobsListAtom = atom<JobRecord[]>([]);

/** Shared zoom range for synchronized chart zooming (timestamp bounds) */
export interface ZoomRange {
  min: number;
  max: number;
}
export const chartZoomRangeAtom = atom<ZoomRange | null>(null);

// ============================================================================
// Derived State
// ============================================================================

/** Whether any data has been loaded from the API */
export const isLoadedAtom = atom((get) => get(summaryAtom) !== null);

/** Whether the job is complete */
export const isCompleteAtom = atom((get) => get(summaryAtom)?.is_complete ?? false);

/** Count of active workers */
export const activeWorkerCountAtom = atom((get) => {
  const workers = get(workersAtom);
  return workers.filter((w) => w.status === 'active').length;
});

// ============================================================================
// Polling Architecture & Data Fetching
// ============================================================================

const pollCursors = {
  jobId: '',
  events: 0,
  metrics: 0,
};

/**
 * Helper function to fetch JSON from an endpoint, returning null on failure.
 * Errors are silently logged to keep polling alive when endpoints are unavailable.
 */
async function fetchJson<T>(url: string): Promise<T | null> {
  try {
    const response = await fetch(url);
    if (response.ok) {
      return await response.json();
    }
    // 404 or other errors return null (e.g., /batch returns 404 when not a batch job)
    return null;
  } catch (error) {
    // Network errors are silently ignored to keep polling alive
    console.debug(`Poll skipped or failed for ${url}:`, error);
    return null;
  }
}

// Legacy export to keep other components from breaking, triggers just the fast loop
export const fetchDashboardDataAtom = atom(null, async (_get, _set) => {
  // Provided for backward compatibility / manual refresh logic
});

/**
 * React hook that initiates and manages multi-tiered dashboard polling.
 * Should be mounted once at the top level of the application (e.g., in App.tsx).
 */
export function useDashboardPolling(intervalMs = 2000): void {
  const setSummary = useSetAtom(summaryAtom);
  const setWorkers = useSetAtom(workersAtom);
  const setMetrics = useSetAtom(metricsAtom);
  const setBottleneck = useSetAtom(bottleneckAtom);
  const setBatch = useSetAtom(batchAtom);
  const setEvents = useSetAtom(eventsAtom);
  const setFailures = useSetAtom(failuresAtom);
  const setJobsList = useSetAtom(jobsListAtom);
  const setCatalog = useSetAtom(catalogAtom);
  const setClickhouseInfo = useSetAtom(clickhouseInfoAtom);
  const setClusterConfig = useSetAtom(clusterConfigAtom);
  const setClusterVms = useSetAtom(clusterVmsAtom);

  const selectedJobId = useAtomValue(selectedJobIdAtom);

  // Refs to control the recursive setTimeouts
  const isMounted = useRef(true);
  const fastTimer = useRef<number | null>(null);
  const slowTimer = useRef<number | null>(null);

  useEffect(() => {
    isMounted.current = true;
    return () => {
      isMounted.current = false;
      if (fastTimer.current) clearTimeout(fastTimer.current);
      if (slowTimer.current) clearTimeout(slowTimer.current);
    };
  }, []);

  // Detect job switches and reset cursors
  useEffect(() => {
    if (pollCursors.jobId !== selectedJobId) {
      pollCursors.jobId = selectedJobId;
      pollCursors.events = 0;
      pollCursors.metrics = 0;

      // Clear data to prevent flashing old state
      setMetrics(null);
      setEvents([]);

      // Fire static tier immediately on job switch
      fetchStaticTier();
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedJobId]);

  // 1. Static Tier (Fetch Once / On Job Switch)
  const fetchStaticTier = async () => {
    if (selectedJobId !== 'active') return; // History mode doesn't load these
    const [catalogList, clusterConfig] = await Promise.all([
      fetchJson<import('../types').CatalogEntry[]>('/api/catalog'),
      fetchJson<ClusterConfig>('/api/cluster/config')
    ]);
    if (catalogList) setCatalog(catalogList);
    if (clusterConfig) setClusterConfig(clusterConfig);
  };

  // 2. Slow Tier (Every 15s)
  const fetchSlowTier = async () => {
    if (!isMounted.current) return;
    const isHistoryView = selectedJobId !== 'active';

    const [chInfo, jobsList, vmsResp] = await Promise.all([
      isHistoryView ? Promise.resolve(null) : fetchJson<ClickHouseInfo>('/api/clickhouse/info'),
      fetchJson<JobRecord[]>('/api/history/jobs'),
      isHistoryView ? Promise.resolve(null) : fetchJson<{ vms: GcpVm[] }>('/api/cluster/vms'),
    ]);

    if (chInfo) setClickhouseInfo(chInfo);
    if (jobsList) setJobsList(jobsList);
    if (vmsResp) setClusterVms(vmsResp.vms ?? []);

    if (isMounted.current) {
      slowTimer.current = window.setTimeout(fetchSlowTier, 15000);
    }
  };

  // 3. Fast Tier (Every 2s)
  const fetchFastTier = async () => {
    if (!isMounted.current) return;
    const isHistoryView = selectedJobId !== 'active';

    const basePath = isHistoryView ? `/api/history/jobs/${selectedJobId}` : '/api/dashboard';
    const eventsPath = isHistoryView ? `/api/history/jobs/${selectedJobId}/events` : '/api/events';
    const failuresPath = isHistoryView ? `/api/history/jobs/${selectedJobId}/failures` : '/api/failures';

    const [summary, workers, metricsResp, bottleneck, batch, eventsResp, failuresResp] = await Promise.all([
      fetchJson<DashboardSummary>(`${basePath}/summary`),
      isHistoryView ? Promise.resolve([] as DashboardWorker[]) : fetchJson<DashboardWorker[]>('/api/dashboard/workers'),
      fetchJson<DashboardMetrics>(`${basePath}/metrics?since_ms=${pollCursors.metrics}`),
      isHistoryView ? Promise.resolve(null) : fetchJson<DashboardBottleneck>('/api/dashboard/bottlenecks'),
      fetchJson<BatchStatusResponse>(`${basePath}/batch`),
      fetchJson<{ events: JobEvent[] }>(`${eventsPath}?since_ms=${pollCursors.events}`),
      fetchJson<{ failures: FailureRecord[] }>(failuresPath),
    ]);

    if (summary) setSummary(summary);
    if (workers) setWorkers(workers);
    if (bottleneck) setBottleneck(bottleneck);
    if (batch) setBatch(batch);
    if (failuresResp) setFailures(failuresResp.failures);

    // Merge Incremental Events
    if (eventsResp) {
      setEvents(prev => {
        const newEvents = pollCursors.events === 0 ? eventsResp.events : [...prev, ...eventsResp.events];
        if (newEvents.length > 0) {
          pollCursors.events = Math.max(...newEvents.map(e => e.timestamp_ms));
        }
        return newEvents.slice(-1000); // Keep max 1000 items
      });
    }

    // Merge Incremental Metrics
    if (metricsResp) {
      setMetrics(prev => {
        if (pollCursors.metrics === 0 || !prev) {
          // Find max timestamp to set cursor
          let maxTs = 0;
          metricsResp.workers.forEach(w => {
            w.snapshots.forEach(s => { maxTs = Math.max(maxTs, s.timestamp_ms); });
          });
          if (maxTs > 0) pollCursors.metrics = maxTs;
          return metricsResp;
        }

        // Deep merge
        const updatedWorkers = [...prev.workers];
        let newMaxTs = pollCursors.metrics;

        metricsResp.workers.forEach(incomingWorker => {
          if (incomingWorker.snapshots.length === 0) return;

          const existingIdx = updatedWorkers.findIndex(w => w.worker_id === incomingWorker.worker_id);
          if (existingIdx >= 0) {
            const mergedSnapshots = [...updatedWorkers[existingIdx].snapshots, ...incomingWorker.snapshots].slice(-300);
            updatedWorkers[existingIdx] = { ...updatedWorkers[existingIdx], snapshots: mergedSnapshots };
          } else {
            updatedWorkers.push({ ...incomingWorker, snapshots: incomingWorker.snapshots.slice(-300) });
          }

          incomingWorker.snapshots.forEach(s => {
            newMaxTs = Math.max(newMaxTs, s.timestamp_ms);
          });
        });

        pollCursors.metrics = newMaxTs;
        return { workers: updatedWorkers };
      });
    }

    if (isMounted.current) {
      fastTimer.current = window.setTimeout(fetchFastTier, intervalMs);
    }
  };

  // Kickoff loops once on mount
  useEffect(() => {
    fetchStaticTier();
    fetchSlowTier();
    fetchFastTier();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);
}
