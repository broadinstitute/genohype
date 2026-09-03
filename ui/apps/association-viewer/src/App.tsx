import { useEffect, useState } from 'react';
import {
  AssociationHitTable,
  AssociationManhattanViewer,
  ManifestValidationError,
  parseAssociationVisualizationManifest,
  resolveAssociationResourceUrl,
  stableHitKey,
  type AssociationPointV1,
  type AssociationVisualizationManifestV1,
} from '@genohype/association-viz';
import '@genohype/association-viz/styles.css';
import './app.css';

export type ViewerState =
  | { kind: 'loading' }
  | { kind: 'ready'; manifest: AssociationVisualizationManifestV1; rasterUrl: string }
  | { kind: 'empty'; manifest: AssociationVisualizationManifestV1 }
  | { kind: 'schema-error'; message: string }
  | { kind: 'resource-error'; message: string };

export function associationManifestUrl(base = document.baseURI): URL {
  return new URL('api/manifest', base);
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
    };
  } catch (error) {
    return { kind: 'resource-error', message: error instanceof Error ? error.message : 'Raster URL could not be resolved.' };
  }
}

export interface AppProps {
  manifestUrl?: URL;
  onOpenLocus?: (hit: AssociationPointV1) => void;
}

export function App({ manifestUrl = associationManifestUrl(), onOpenLocus }: AppProps) {
  const [state, setState] = useState<ViewerState>({ kind: 'loading' });
  const [selectedHitKey, setSelectedHitKey] = useState<string | null>(null);
  const [selectedContig, setSelectedContig] = useState<string | null>(null);

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

  const select = (hit: AssociationPointV1) => setSelectedHitKey(stableHitKey(hit));
  return (
    <main className="association-viewer-app" data-state="ready">
      <header>
        <h1>{state.manifest.title}</h1>
        <p>{state.manifest.assembly} · {state.manifest.valid_row_count.toLocaleString()} valid associations</p>
      </header>
      <AssociationManhattanViewer
        manifest={state.manifest}
        rasterUrl={state.rasterUrl}
        selectedHitKey={selectedHitKey}
        selectedContig={selectedContig}
        onHitClick={select}
        onChromosomeClick={setSelectedContig}
        onOpenLocus={onOpenLocus}
        onResourceError={(error) => setState({ kind: 'resource-error', message: error.message })}
      />
      <AssociationHitTable
        hits={state.manifest.top_hits}
        selectedHitKey={selectedHitKey}
        onSelectedHitChange={select}
        onOpenLocus={onOpenLocus}
      />
    </main>
  );
}
