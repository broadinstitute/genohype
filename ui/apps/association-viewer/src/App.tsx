import { useCallback, useEffect, useMemo, useState } from 'react';
import {
  AssociationHitTable,
  AssociationLocusViewer,
  AssociationManhattanViewer,
  ManifestValidationError,
  parseAssociationVisualizationManifest,
  resolveAssociationResourceUrl,
  stableHitKey,
  type AssociationPointV1,
  type AssociationVisualizationManifestV1,
} from '@genohype/association-viz';
import {
  canonicalizeViewerRoute,
  locusWindowForHit,
  parseViewerRoute,
  tokenRootFromManifestUrl,
  tokenRootUrl,
  viewerRouteUrl,
  withSelectedOrdinal,
  type ParsedViewerRoute,
  type ViewerRoute,
} from './route.js';
import { useAssociationDetail } from './useAssociationDetail.js';
import { useAssociationTablePage } from './useAssociationTablePage.js';
import '@genohype/association-viz/styles.css';
import './app.css';

export type ViewerState =
  | { kind: 'loading' }
  | {
    kind: 'ready';
    manifest: AssociationVisualizationManifestV1;
    rasterUrl: string;
    tableApiUrl: URL | null;
    regionApiUrl: URL | null;
    tokenRoot: URL;
  }
  | { kind: 'empty'; manifest: AssociationVisualizationManifestV1 }
  | { kind: 'schema-error'; message: string }
  | { kind: 'resource-error'; message: string };

export function associationManifestUrl(base: string | URL = window.location.href): URL {
  return new URL('api/manifest', tokenRootUrl(base));
}

export async function loadAssociationViewer(
  manifestUrl: URL,
  signal?: AbortSignal,
): Promise<Exclude<ViewerState, { kind: 'loading' }>> {
  let response: Response;
  try {
    response = await fetch(manifestUrl, { signal, headers: { Accept: 'application/json' } });
  } catch (error) {
    if (signal?.aborted) throw error;
    return { kind: 'resource-error', message: `Manifest request failed: ${error instanceof Error ? error.message : String(error)}` };
  }
  if (!response.ok) return { kind: 'resource-error', message: `Manifest request failed with HTTP ${response.status}` };
  let input: unknown;
  try {
    input = await response.json();
  } catch {
    return { kind: 'schema-error', message: 'Manifest response was not valid JSON.' };
  }
  let manifest: AssociationVisualizationManifestV1;
  try {
    manifest = parseAssociationVisualizationManifest(input);
  } catch (error) {
    if (error instanceof ManifestValidationError && error.code === 'resource') {
      return { kind: 'resource-error', message: error.message };
    }
    return { kind: 'schema-error', message: error instanceof Error ? error.message : 'Manifest validation failed.' };
  }
  if (manifest.valid_row_count === 0) return { kind: 'empty', manifest };
  try {
    const fetchedUrl = response.url ? new URL(response.url) : manifestUrl;
    return {
      kind: 'ready',
      manifest,
      rasterUrl: resolveAssociationResourceUrl(manifest.resources.raster, fetchedUrl).href,
      tableApiUrl: manifest.resources.table_api
        ? resolveAssociationResourceUrl(manifest.resources.table_api, fetchedUrl)
        : null,
      regionApiUrl: manifest.resources.region_api
        ? resolveAssociationResourceUrl(manifest.resources.region_api, fetchedUrl)
        : null,
      tokenRoot: tokenRootFromManifestUrl(fetchedUrl),
    };
  } catch (error) {
    return { kind: 'resource-error', message: error instanceof Error ? error.message : 'Association resource URL could not be resolved.' };
  }
}

export interface AppProps {
  manifestUrl?: URL;
  onOpenLocus?: (hit: AssociationPointV1) => void;
}

interface ServerAssociationTableProps {
  tableApiUrl: URL;
  fallbackRows: readonly AssociationPointV1[];
  total: number;
  selectedHitKey: string | null;
  selectedSourceOrdinal: number | null;
  requestKind?: 'table' | 'region';
  onSelectedHitChange: (hit: AssociationPointV1) => void;
  onSelectedPointAvailable: (hit: AssociationPointV1) => void;
  onOpenLocus?: (hit: AssociationPointV1) => void;
}

function ServerAssociationTable({
  tableApiUrl,
  fallbackRows,
  total,
  selectedHitKey,
  selectedSourceOrdinal,
  requestKind = 'table',
  onSelectedHitChange,
  onSelectedPointAvailable,
  onOpenLocus,
}: ServerAssociationTableProps) {
  const page = useAssociationTablePage({ tableApiUrl, fallbackRows, total, requestKind });

  useEffect(() => {
    if (selectedSourceOrdinal === null) return;
    const selected = page.rows.find((hit) => hit.source_ordinal === selectedSourceOrdinal);
    if (selected) onSelectedPointAvailable(selected);
  }, [onSelectedPointAvailable, page.rows, selectedSourceOrdinal]);

  if (page.error) {
    return (
      <>
        <div className="association-table-fallback" role="alert">
          <span>{page.error}</span>
          <button type="button" onClick={page.retry}>Retry table request</button>
        </div>
        <AssociationHitTable
          hits={fallbackRows}
          selectedHitKey={selectedHitKey}
          onSelectedHitChange={onSelectedHitChange}
          onOpenLocus={onOpenLocus}
        />
      </>
    );
  }

  return (
    <AssociationHitTable
      mode="server"
      rows={page.rows}
      total={page.total}
      offset={page.offset}
      limit={page.limit}
      query={page.query}
      sort={page.sort}
      direction={page.direction}
      loading={page.loading}
      onQueryChange={page.setQuery}
      onSortChange={page.setSort}
      onOffsetChange={page.setOffset}
      selectedHitKey={selectedHitKey}
      onSelectedHitChange={onSelectedHitChange}
      onOpenLocus={onOpenLocus}
    />
  );
}

function fixedRegionApiUrl(base: URL, contig: string, start: number, end: number): URL {
  const url = new URL(base.href);
  url.search = '';
  url.searchParams.set('contig', contig);
  url.searchParams.set('start', String(start));
  url.searchParams.set('end', String(end));
  return url;
}

interface ReadyViewerProps {
  state: Extract<ViewerState, { kind: 'ready' }>;
  parsedRoute: ParsedViewerRoute;
  onRootResourceError: (error: Error) => void;
  onOpenLocus?: (hit: AssociationPointV1) => void;
}

function ReadyViewer({ state, parsedRoute, onRootResourceError, onOpenLocus }: ReadyViewerProps) {
  const route = useMemo(
    () => canonicalizeViewerRoute(parsedRoute, state.manifest),
    [parsedRoute, state.manifest],
  );
  const [selectedPoint, setSelectedPoint] = useState<AssociationPointV1 | null>(null);

  useEffect(() => {
    if (route.kind === 'invalid') return;
    const canonicalUrl = viewerRouteUrl(state.tokenRoot, route);
    if (canonicalUrl.href !== window.location.href) window.history.replaceState(null, '', canonicalUrl);
  }, [route, state.tokenRoot]);

  useEffect(() => {
    if (route.kind === 'invalid') return;
    if (route.kind !== 'root') {
      if (selectedPoint?.source_ordinal !== route.selectedSourceOrdinal) setSelectedPoint(null);
      return;
    }
    if (selectedPoint?.source_ordinal !== route.selectedSourceOrdinal) {
      const topHit = route.selectedSourceOrdinal === null
        ? null
        : state.manifest.top_hits.find((hit) => hit.source_ordinal === route.selectedSourceOrdinal) ?? null;
      setSelectedPoint(topHit);
    }
  }, [route, selectedPoint?.source_ordinal, state.manifest.top_hits]);

  if (route.kind === 'invalid') {
    return <main className="viewer-state viewer-error" data-state="route-error" role="alert"><h1>Invalid association view</h1><p>{route.message}</p></main>;
  }

  return (
    <ReadyRoute
      state={state}
      route={route}
      selectedPoint={selectedPoint}
      setSelectedPoint={setSelectedPoint}
      onRootResourceError={onRootResourceError}
      onOpenLocus={onOpenLocus}
    />
  );
}

interface ReadyRouteProps {
  state: Extract<ViewerState, { kind: 'ready' }>;
  route: ViewerRoute;
  selectedPoint: AssociationPointV1 | null;
  setSelectedPoint: (point: AssociationPointV1 | null) => void;
  onRootResourceError: (error: Error) => void;
  onOpenLocus?: (hit: AssociationPointV1) => void;
}

function ReadyRoute({
  state,
  route,
  selectedPoint,
  setSelectedPoint,
  onRootResourceError,
  onOpenLocus,
}: ReadyRouteProps) {
  const detail = useAssociationDetail(route, state.tokenRoot, state.manifest);
  const selectedHitKey = selectedPoint && selectedPoint.source_ordinal === route.selectedSourceOrdinal
    ? stableHitKey(selectedPoint)
    : null;

  const commitRoute = useCallback((next: ViewerRoute, replace = false) => {
    const url = viewerRouteUrl(state.tokenRoot, next);
    window.history[replace ? 'replaceState' : 'pushState'](null, '', url);
    window.dispatchEvent(new PopStateEvent('popstate'));
  }, [state.tokenRoot]);

  const select = useCallback((hit: AssociationPointV1) => {
    setSelectedPoint(hit);
    commitRoute(withSelectedOrdinal(route, hit.source_ordinal), true);
  }, [commitRoute, route, setSelectedPoint]);

  const hydrateSelection = useCallback((hit: AssociationPointV1) => {
    if (hit.source_ordinal === route.selectedSourceOrdinal
        && (!selectedPoint || stableHitKey(selectedPoint) !== stableHitKey(hit))) setSelectedPoint(hit);
  }, [route.selectedSourceOrdinal, selectedPoint, setSelectedPoint]);

  const openLocus = useCallback((hit: AssociationPointV1) => {
    const locus = locusWindowForHit(hit, state.manifest);
    if (!locus) return;
    setSelectedPoint(hit);
    onOpenLocus?.(hit);
    commitRoute(locus);
  }, [commitRoute, onOpenLocus, setSelectedPoint, state.manifest]);

  const wholeGenome = useCallback(() => {
    commitRoute({ kind: 'root', selectedSourceOrdinal: route.selectedSourceOrdinal });
  }, [commitRoute, route.selectedSourceOrdinal]);

  useEffect(() => {
    if (route.selectedSourceOrdinal === null) return;
    const selected = detail.state.kind === 'locus-ready'
      ? detail.state.overlay.points.find((entry) => entry.point.source_ordinal === route.selectedSourceOrdinal)?.point
      : detail.state.kind === 'chromosome-ready'
        ? detail.state.manifest.top_hits.find((hit) => hit.source_ordinal === route.selectedSourceOrdinal)
        : undefined;
    if (selected) hydrateSelection(selected);
  }, [detail.state, hydrateSelection, route.selectedSourceOrdinal]);

  const controls = route.kind === 'root' ? null : (
    <nav className="association-viewer-navigation" aria-label="Association viewer navigation">
      <button type="button" onClick={() => window.history.back()}>Back</button>
      <button type="button" onClick={wholeGenome}>Whole genome</button>
    </nav>
  );

  if (route.kind === 'root') {
    return (
      <main className="association-viewer-app" data-state="ready" data-route="root">
        <header>
          <h1>{state.manifest.title}</h1>
          <p>{state.manifest.assembly} · {state.manifest.valid_row_count.toLocaleString()} valid associations</p>
        </header>
        <AssociationManhattanViewer
          manifest={state.manifest}
          rasterUrl={state.rasterUrl}
          selectedHitKey={selectedHitKey}
          onHitClick={select}
          onChromosomeClick={(contig) => commitRoute({ kind: 'chromosome', contig, selectedSourceOrdinal: route.selectedSourceOrdinal })}
          onOpenLocus={openLocus}
          onResourceError={onRootResourceError}
        />
        {state.tableApiUrl ? (
          <ServerAssociationTable
            tableApiUrl={state.tableApiUrl}
            fallbackRows={state.manifest.top_hits}
            total={state.manifest.valid_row_count}
            selectedHitKey={selectedHitKey}
            selectedSourceOrdinal={route.selectedSourceOrdinal}
            onSelectedHitChange={select}
            onSelectedPointAvailable={hydrateSelection}
            onOpenLocus={openLocus}
          />
        ) : (
          <AssociationHitTable
            hits={state.manifest.top_hits}
            selectedHitKey={selectedHitKey}
            onSelectedHitChange={select}
            onOpenLocus={openLocus}
          />
        )}
      </main>
    );
  }

  if (detail.state.kind === 'loading' || detail.state.kind === 'idle') {
    return <main className="viewer-state" data-state="detail-loading" role="status">{controls}Loading association detail…</main>;
  }
  if (detail.state.kind === 'schema-error') {
    return <main className="viewer-state viewer-error" data-state="schema-error" role="alert">{controls}<h1>Unsupported association detail</h1><p>{detail.state.message}</p></main>;
  }
  if (detail.state.kind === 'bounded-too-dense') {
    return <main className="viewer-state viewer-error" data-state="bounded-too-dense" role="alert">{controls}<h1>Region too dense</h1><p>{detail.state.message}</p></main>;
  }
  if (detail.state.kind === 'resource-error') {
    return <main className="viewer-state viewer-error" data-state="resource-error" role="alert">{controls}<h1>Association detail unavailable</h1><p>{detail.state.message}</p></main>;
  }

  if (route.kind === 'chromosome' && detail.state.kind === 'chromosome-ready') {
    const contig = detail.state.manifest.layout.contigs[0];
    const regionTable = state.regionApiUrl && contig
      ? fixedRegionApiUrl(state.regionApiUrl, route.contig, 1, contig.length)
      : null;
    return (
      <main className="association-viewer-app" data-state="ready" data-route="chromosome">
        {controls}
        <header>
          <h1>{detail.state.manifest.title}</h1>
          <p>Chromosome {route.contig} · {detail.state.manifest.valid_row_count.toLocaleString()} valid associations</p>
        </header>
        {detail.state.manifest.valid_row_count === 0 && <p className="association-detail-empty" role="status">No associations in this chromosome.</p>}
        <AssociationManhattanViewer
          manifest={detail.state.manifest}
          rasterUrl={detail.state.rasterUrl}
          selectedHitKey={selectedHitKey}
          selectedContig={route.contig}
          onHitClick={select}
          onOpenLocus={openLocus}
          onResourceError={onRootResourceError}
        />
        {regionTable && (
          <ServerAssociationTable
            tableApiUrl={regionTable}
            fallbackRows={detail.state.manifest.top_hits}
            total={detail.state.manifest.valid_row_count}
            requestKind="region"
            selectedHitKey={selectedHitKey}
            selectedSourceOrdinal={route.selectedSourceOrdinal}
            onSelectedHitChange={select}
            onSelectedPointAvailable={hydrateSelection}
            onOpenLocus={openLocus}
          />
        )}
      </main>
    );
  }

  if (route.kind === 'locus' && detail.state.kind === 'locus-ready') {
    const regionTable = state.regionApiUrl
      ? fixedRegionApiUrl(
        state.regionApiUrl,
        route.contig,
        detail.state.manifest.layout.effective_start,
        detail.state.manifest.layout.effective_end,
      )
      : null;
    const fallbackRows = detail.state.overlay.points.map((entry) => entry.point);
    return (
      <main className="association-viewer-app" data-state="ready" data-route="locus">
        {controls}
        <header>
          <h1>{detail.state.manifest.title}</h1>
          <p>{state.manifest.assembly} · requested {route.contig}:{route.start.toLocaleString()}–{route.end.toLocaleString()}</p>
        </header>
        {detail.state.manifest.counts.source_point_count === 0 && <p className="association-detail-empty" role="status">No associations in this locus.</p>}
        <AssociationLocusViewer
          manifest={detail.state.manifest}
          overlay={detail.state.overlay}
          rasterUrl={detail.state.rasterUrl}
          selectedSourceOrdinal={route.selectedSourceOrdinal}
          selectedPoint={selectedPoint}
          onPointSelect={select}
        />
        {regionTable && (
          <ServerAssociationTable
            tableApiUrl={regionTable}
            fallbackRows={fallbackRows}
            total={detail.state.manifest.counts.source_point_count}
            requestKind="region"
            selectedHitKey={selectedHitKey}
            selectedSourceOrdinal={route.selectedSourceOrdinal}
            onSelectedHitChange={select}
            onSelectedPointAvailable={hydrateSelection}
            onOpenLocus={openLocus}
          />
        )}
      </main>
    );
  }

  return <main className="viewer-state viewer-error" data-state="schema-error" role="alert">Association detail did not match the route.</main>;
}

export function App({ manifestUrl = associationManifestUrl(), onOpenLocus }: AppProps) {
  const [state, setState] = useState<ViewerState>({ kind: 'loading' });
  const [parsedRoute, setParsedRoute] = useState<ParsedViewerRoute>(() => parseViewerRoute(window.location.href));

  useEffect(() => {
    const popstate = () => setParsedRoute(parseViewerRoute(window.location.href));
    window.addEventListener('popstate', popstate);
    return () => window.removeEventListener('popstate', popstate);
  }, []);

  useEffect(() => {
    const controller = new AbortController();
    setState({ kind: 'loading' });
    loadAssociationViewer(manifestUrl, controller.signal)
      .then(setState)
      .catch((error) => {
        if (!controller.signal.aborted) setState({ kind: 'resource-error', message: String(error) });
      });
    return () => controller.abort();
  }, [manifestUrl.href]);

  if (state.kind === 'loading') return <main className="viewer-state" data-state="loading" role="status">Loading association manifest…</main>;
  if (state.kind === 'empty') {
    return <main className="viewer-state" data-state="empty"><h1>{state.manifest.title}</h1><p>No valid association results were returned.</p></main>;
  }
  if (state.kind === 'schema-error') {
    return <main className="viewer-state viewer-error" data-state="schema-error" role="alert"><h1>Unsupported association data</h1><p>{state.message}</p></main>;
  }
  if (state.kind === 'resource-error') {
    return <main className="viewer-state viewer-error" data-state="resource-error" role="alert"><h1>Association resource unavailable</h1><p>{state.message}</p></main>;
  }

  return (
    <ReadyViewer
      state={state}
      parsedRoute={parsedRoute}
      onRootResourceError={(error) => setState({ kind: 'resource-error', message: error.message })}
      onOpenLocus={onOpenLocus}
    />
  );
}
