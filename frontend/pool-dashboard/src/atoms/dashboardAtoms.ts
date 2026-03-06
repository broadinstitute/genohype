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

// ============================================================================
// UI Preference Atoms (persisted to localStorage)
// ============================================================================

/** Currently active workspace layout preset */
export const layoutPresetAtom = atomWithStorage<string>(
  'dashboardLayoutPreset',
  'overview'
);

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
 */
export const fetchDashboardDataAtom = atom(null, async (_get, set) => {
  // Fetch all endpoints concurrently
  const [summary, workers, metrics, bottleneck, batch, eventsResp, failuresResp] =
    await Promise.all([
      fetchJson<DashboardSummary>('/api/dashboard/summary'),
      fetchJson<DashboardWorker[]>('/api/dashboard/workers'),
      fetchJson<DashboardMetrics>('/api/dashboard/metrics'),
      fetchJson<DashboardBottleneck>('/api/dashboard/bottlenecks'),
      fetchJson<BatchStatusResponse>('/api/dashboard/batch'),
      fetchJson<{ events: JobEvent[] }>('/api/events'),
      fetchJson<{ failures: FailureRecord[] }>('/api/failures'),
    ]);

  // Update atoms with fetched data (only if fetch succeeded)
  if (summary !== null) set(summaryAtom, summary);
  if (workers !== null) set(workersAtom, workers);
  if (metrics !== null) set(metricsAtom, metrics);
  if (bottleneck !== null) set(bottleneckAtom, bottleneck);
  if (batch !== null) set(batchAtom, batch);
  if (eventsResp !== null) set(eventsAtom, eventsResp.events);
  if (failuresResp !== null) set(failuresAtom, failuresResp.failures);
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
