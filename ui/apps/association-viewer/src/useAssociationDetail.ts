import { useCallback, useEffect, useMemo, useState } from 'react';
import {
  ManifestValidationError,
  parseAssociationRegionRenderManifest,
  parseAssociationVisualizationManifest,
  resolveAssociationResourceUrl,
  validateAssociationRegionPair,
  type AssociationRegionOverlayV1,
  type AssociationRegionRenderManifestV1,
  type AssociationVisualizationManifestV1,
} from '@genohype/association-viz';
import type { ViewerRoute } from './route.js';

export const DETAIL_RESIZE_DEBOUNCE_MS = 150;
export const DETAIL_HEIGHT = 320;
export const MAX_DETAIL_REQUEST_WIDTH = 4050;

export interface DetailRequestMetrics {
  width: number;
  height: number;
  dpr: number;
}

export function detailRequestMetrics(viewportWidth: number, devicePixelRatio: number): DetailRequestMetrics {
  const finiteWidth = Number.isFinite(viewportWidth) ? Math.floor(viewportWidth) : 50;
  const width = Math.max(50, Math.min(MAX_DETAIL_REQUEST_WIDTH, finiteWidth - 80));
  const finiteDpr = Number.isFinite(devicePixelRatio) ? Math.round(devicePixelRatio) : 1;
  return { width, height: DETAIL_HEIGHT, dpr: Math.max(1, Math.min(4, finiteDpr)) };
}

export type AssociationDetailState =
  | { kind: 'idle' }
  | { kind: 'loading' }
  | {
    kind: 'chromosome-ready';
    manifest: AssociationVisualizationManifestV1;
    rasterUrl: string;
  }
  | {
    kind: 'locus-ready';
    manifest: AssociationRegionRenderManifestV1;
    overlay: AssociationRegionOverlayV1;
    rasterUrl: string;
  }
  | { kind: 'schema-error'; message: string }
  | { kind: 'resource-error'; message: string; retriable?: boolean }
  | { kind: 'bounded-too-dense'; message: string };

function requestError(status: number): AssociationDetailState {
  if (status === 422) {
    return { kind: 'bounded-too-dense', message: 'This region has too many required interactive points to display safely.' };
  }
  if (status === 400) return { kind: 'resource-error', message: 'The detail request was not accepted.' };
  if (status === 404) return { kind: 'resource-error', message: 'The requested association detail was not found.' };
  if (status === 429 || status === 503) {
    return { kind: 'resource-error', message: 'The association server is busy. Try this detail again shortly.', retriable: true };
  }
  return { kind: 'resource-error', message: `Association detail request failed with HTTP ${status}.` };
}

function schemaError(error: unknown): AssociationDetailState {
  const message = error instanceof Error ? error.message : 'Association detail validation failed.';
  return {
    kind: error instanceof ManifestValidationError && error.code === 'resource' ? 'resource-error' : 'schema-error',
    message,
  };
}

async function responseJson(response: Response): Promise<unknown> {
  try {
    return await response.json();
  } catch {
    throw new Error('Association detail response was not valid JSON.');
  }
}

export function chromosomeManifestUrl(root: URL, datasetId: string, contig: string): URL {
  return new URL(
    `api/datasets/${encodeURIComponent(datasetId)}/chromosomes/${encodeURIComponent(contig)}/manifest`,
    root,
  );
}

export function regionManifestUrl(
  root: URL,
  datasetId: string,
  route: Extract<ViewerRoute, { kind: 'locus' }>,
  metrics: DetailRequestMetrics,
): URL {
  const url = new URL(`api/datasets/${encodeURIComponent(datasetId)}/region/manifest`, root);
  url.searchParams.set('contig', route.contig);
  url.searchParams.set('start', String(route.start));
  url.searchParams.set('end', String(route.end));
  url.searchParams.set('width', String(metrics.width));
  url.searchParams.set('height', String(metrics.height));
  url.searchParams.set('dpr', String(metrics.dpr));
  if (route.selectedSourceOrdinal !== null) url.searchParams.set('selected', String(route.selectedSourceOrdinal));
  return url;
}

export async function loadChromosomeDetail(
  url: URL,
  rootManifest: AssociationVisualizationManifestV1,
  expectedContig: string,
  signal?: AbortSignal,
): Promise<AssociationDetailState> {
  let response: Response;
  try {
    response = await fetch(url, { signal, headers: { Accept: 'application/json' } });
  } catch (error) {
    if (signal?.aborted) throw error;
    return { kind: 'resource-error', message: 'Chromosome manifest request failed.' };
  }
  if (!response.ok) return requestError(response.status);
  let manifest: AssociationVisualizationManifestV1;
  try {
    manifest = parseAssociationVisualizationManifest(await responseJson(response));
  } catch (error) {
    return schemaError(error);
  }
  const onlyContig = manifest.layout.contigs.length === 1 ? manifest.layout.contigs[0] : null;
  const rootContig = rootManifest.layout.contigs.find((entry) => entry.name === expectedContig);
  if (!onlyContig || !rootContig || onlyContig.name !== expectedContig || onlyContig.length !== rootContig.length
      || manifest.plot_id !== rootManifest.plot_id || manifest.assembly !== rootManifest.assembly
      || manifest.source_fingerprint !== rootManifest.source_fingerprint) {
    return { kind: 'schema-error', message: 'Chromosome manifest does not match the loaded dataset.' };
  }
  try {
    const finalUrl = response.url ? new URL(response.url) : url;
    return {
      kind: 'chromosome-ready',
      manifest,
      rasterUrl: resolveAssociationResourceUrl(manifest.resources.raster, finalUrl).href,
    };
  } catch (error) {
    return schemaError(error);
  }
}

export async function loadLocusDetail(
  url: URL,
  rootManifest: AssociationVisualizationManifestV1,
  route: Extract<ViewerRoute, { kind: 'locus' }>,
  metrics: DetailRequestMetrics,
  signal?: AbortSignal,
): Promise<AssociationDetailState> {
  let response: Response;
  try {
    response = await fetch(url, { signal, headers: { Accept: 'application/json' } });
  } catch (error) {
    if (signal?.aborted) throw error;
    return { kind: 'resource-error', message: 'Region manifest request failed.' };
  }
  if (!response.ok) return requestError(response.status);
  let rawManifest: unknown;
  try {
    rawManifest = await responseJson(response);
  } catch (error) {
    return { kind: 'schema-error', message: error instanceof Error ? error.message : 'Invalid region manifest.' };
  }
  let manifest: AssociationRegionRenderManifestV1;
  let rasterUrl: URL;
  let overlayUrl: URL;
  try {
    manifest = parseAssociationRegionRenderManifest(rawManifest);
    const finalUrl = response.url ? new URL(response.url) : url;
    rasterUrl = resolveAssociationResourceUrl(manifest.resources.raster, finalUrl);
    overlayUrl = resolveAssociationResourceUrl(manifest.resources.overlay, finalUrl);
  } catch (error) {
    return schemaError(error);
  }
  if (manifest.dataset_id !== rootManifest.plot_id || manifest.assembly !== rootManifest.assembly
      || manifest.source_fingerprint !== rootManifest.source_fingerprint
      || manifest.layout.contig !== route.contig || manifest.layout.requested_start !== route.start
      || manifest.layout.requested_end !== route.end || manifest.layout.requested_width !== metrics.width
      || manifest.layout.height !== metrics.height || manifest.layout.dpr !== metrics.dpr) {
    return { kind: 'schema-error', message: 'Region manifest does not match the requested dataset and viewport.' };
  }
  let overlayResponse: Response;
  try {
    overlayResponse = await fetch(overlayUrl, { signal, headers: { Accept: 'application/json' } });
  } catch (error) {
    if (signal?.aborted) throw error;
    return { kind: 'resource-error', message: 'Region overlay request failed.' };
  }
  if (!overlayResponse.ok) return requestError(overlayResponse.status);
  try {
    const pair = validateAssociationRegionPair(manifest, await responseJson(overlayResponse));
    return { kind: 'locus-ready', manifest: pair.manifest, overlay: pair.overlay, rasterUrl: rasterUrl.href };
  } catch (error) {
    return schemaError(error);
  }
}

function currentMetrics(): DetailRequestMetrics {
  return detailRequestMetrics(window.innerWidth, window.devicePixelRatio);
}

export function useAssociationDetail(
  route: ViewerRoute,
  root: URL,
  rootManifest: AssociationVisualizationManifestV1,
): { state: AssociationDetailState; metrics: DetailRequestMetrics; retry: () => void } {
  const [metrics, setMetrics] = useState(currentMetrics);
  const [state, setState] = useState<AssociationDetailState>({ kind: 'idle' });
  const [retryCount, setRetryCount] = useState(0);

  useEffect(() => {
    let timeout: number | undefined;
    const resize = () => {
      window.clearTimeout(timeout);
      timeout = window.setTimeout(() => {
        const next = currentMetrics();
        setMetrics((prior) => prior.width === next.width && prior.height === next.height && prior.dpr === next.dpr ? prior : next);
      }, DETAIL_RESIZE_DEBOUNCE_MS);
    };
    window.addEventListener('resize', resize);
    return () => {
      window.removeEventListener('resize', resize);
      window.clearTimeout(timeout);
    };
  }, []);

  const requestUrl = useMemo(() => {
    if (route.kind === 'chromosome') return chromosomeManifestUrl(root, rootManifest.plot_id, route.contig);
    if (route.kind === 'locus') return regionManifestUrl(root, rootManifest.plot_id, route, metrics);
    return null;
  }, [metrics, root, rootManifest.plot_id, route]);

  useEffect(() => {
    if (!requestUrl || route.kind === 'root') {
      setState({ kind: 'idle' });
      return;
    }
    const controller = new AbortController();
    setState({ kind: 'loading' });
    const load = route.kind === 'chromosome'
      ? loadChromosomeDetail(requestUrl, rootManifest, route.contig, controller.signal)
      : loadLocusDetail(requestUrl, rootManifest, route, metrics, controller.signal);
    void load.then((next) => {
      if (!controller.signal.aborted) setState(next);
    }).catch(() => {
      if (!controller.signal.aborted) setState({ kind: 'resource-error', message: 'Association detail request failed.' });
    });
    return () => controller.abort();
  }, [metrics, requestUrl, retryCount, rootManifest, route]);

  const retry = useCallback(() => setRetryCount((count) => count + 1), []);
  return { state, metrics, retry };
}
