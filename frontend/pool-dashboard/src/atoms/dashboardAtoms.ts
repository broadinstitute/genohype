import { atom, useSetAtom } from 'jotai';
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
// Action Atom: Fetch All Dashboard Data
// ============================================================================

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

/**
 * Action atom that fetches all dashboard endpoints concurrently.
 * Uses Promise.all so failures on optional endpoints (like /batch) don't
 * break updates for other atoms.
 *
 * Supports viewing either the "active" live job or a historical job by ID.
 */
export const fetchDashboardDataAtom = atom(null, async (get, set) => {
  const jobId = get(selectedJobIdAtom);
  const isHistoryView = jobId !== 'active';

  // Determine API base paths based on whether we're viewing history
  const basePath = isHistoryView ? `/api/history/jobs/${jobId}` : '/api/dashboard';
  const eventsPath = isHistoryView ? `/api/history/jobs/${jobId}/events` : '/api/events';
  const failuresPath = isHistoryView ? `/api/history/jobs/${jobId}/failures` : '/api/failures';

  // Fetch all endpoints concurrently
  const [summary, workers, metrics, bottleneck, batch, eventsResp, failuresResp, jobsList, catalogList, clickhouseInfo] =
    await Promise.all([
      fetchJson<DashboardSummary>(`${basePath}/summary`),
      // Workers endpoint doesn't exist for history (workers are transient)
      isHistoryView ? Promise.resolve([] as DashboardWorker[]) : fetchJson<DashboardWorker[]>('/api/dashboard/workers'),
      fetchJson<DashboardMetrics>(`${basePath}/metrics`),
      // Bottleneck is a live metric, not available for history
      isHistoryView ? Promise.resolve(null) : fetchJson<DashboardBottleneck>('/api/dashboard/bottlenecks'),
      fetchJson<BatchStatusResponse>(`${basePath}/batch`),
      fetchJson<{ events: JobEvent[] }>(eventsPath),
      fetchJson<{ failures: FailureRecord[] }>(failuresPath),
      // Always fetch jobs list for the history panel
      fetchJson<JobRecord[]>('/api/history/jobs'),
      // Fetch catalog (only for live view)
      isHistoryView ? Promise.resolve([]) : fetchJson<import('../types').CatalogEntry[]>('/api/catalog'),
      // Fetch clickhouse info (only for live view)
      isHistoryView ? Promise.resolve(null) : fetchJson<ClickHouseInfo>('/api/clickhouse/info'),
    ]);

  // Update atoms with fetched data (only if fetch succeeded)
  if (summary !== null) set(summaryAtom, summary);
  if (workers !== null) set(workersAtom, workers);
  if (metrics !== null) set(metricsAtom, metrics);
  if (bottleneck !== null) set(bottleneckAtom, bottleneck);
  if (batch !== null) set(batchAtom, batch);
  if (eventsResp !== null) set(eventsAtom, eventsResp.events);
  if (failuresResp !== null) set(failuresAtom, failuresResp.failures);
  if (jobsList !== null) set(jobsListAtom, jobsList);
  if (catalogList !== null) set(catalogAtom, catalogList);
  if (clickhouseInfo !== null) set(clickhouseInfoAtom, clickhouseInfo);
});

// ============================================================================
// React Hook: Dashboard Polling
// ============================================================================

/**
 * React hook that initiates and manages dashboard data polling.
 * Should be mounted once at the top level of the application (e.g., in App.tsx).
 *
 * @param intervalMs - Polling interval in milliseconds (default: 2000)
 */
export function useDashboardPolling(intervalMs = 2000): void {
  const fetchDashboardData = useSetAtom(fetchDashboardDataAtom);
  const intervalRef = useRef<number | null>(null);

  useEffect(() => {
    // Immediate fetch on mount
    fetchDashboardData();

    // Setup recurring interval
    intervalRef.current = window.setInterval(() => {
      fetchDashboardData();
    }, intervalMs);

    // Cleanup on unmount
    return () => {
      if (intervalRef.current !== null) {
        window.clearInterval(intervalRef.current);
      }
    };
  }, [fetchDashboardData, intervalMs]);
}
