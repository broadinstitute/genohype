import {
  canonicalContig,
  type AssociationPointV1,
  type AssociationVisualizationManifestV1,
} from '@genohype/association-viz';

export type ViewerRoute =
  | { kind: 'root'; selectedSourceOrdinal: number | null }
  | { kind: 'chromosome'; contig: string; selectedSourceOrdinal: number | null }
  | { kind: 'locus'; contig: string; start: number; end: number; selectedSourceOrdinal: number | null };

export type ParsedViewerRoute = ViewerRoute | { kind: 'invalid'; message: string };

function decodeSegment(segment: string): string | null {
  try {
    return decodeURIComponent(segment);
  } catch {
    return null;
  }
}

function selectedOrdinal(url: URL): number | null | undefined {
  const values = url.searchParams.getAll('selected');
  if ([...url.searchParams.keys()].some((key) => key !== 'selected') || values.length > 1) return undefined;
  if (values.length === 0) return null;
  if (!/^(0|[1-9][0-9]*)$/.test(values[0] ?? '')) return undefined;
  const value = Number(values[0]);
  return Number.isSafeInteger(value) ? value : undefined;
}

/** Parse only the token-root, chromosome, and inclusive locus URL shapes. */
export function parseViewerRoute(input: string | URL): ParsedViewerRoute {
  let url: URL;
  try {
    url = input instanceof URL ? new URL(input.href) : new URL(input);
  } catch {
    return { kind: 'invalid', message: 'The viewer URL is invalid.' };
  }
  const segments = url.pathname.split('/').filter(Boolean);
  if (segments.length < 1) return { kind: 'invalid', message: 'The viewer token is missing.' };
  const selected = selectedOrdinal(url);
  if (selected === undefined) return { kind: 'invalid', message: 'The selected association in the URL is invalid.' };
  if (segments.length === 1) return { kind: 'root', selectedSourceOrdinal: selected };
  if (segments.length === 3 && segments[1] === 'chromosome') {
    const contig = decodeSegment(segments[2] ?? '');
    return contig
      ? { kind: 'chromosome', contig, selectedSourceOrdinal: selected }
      : { kind: 'invalid', message: 'The chromosome in the URL is invalid.' };
  }
  if (segments.length === 4 && segments[1] === 'locus') {
    const contig = decodeSegment(segments[2] ?? '');
    const match = /^(0|[1-9][0-9]*)-(0|[1-9][0-9]*)$/.exec(segments[3] ?? '');
    if (!contig || !match) return { kind: 'invalid', message: 'The locus in the URL is invalid.' };
    const start = Number(match[1]);
    const end = Number(match[2]);
    if (!Number.isSafeInteger(start) || !Number.isSafeInteger(end)) {
      return { kind: 'invalid', message: 'The locus bounds in the URL are invalid.' };
    }
    return { kind: 'locus', contig, start, end, selectedSourceOrdinal: selected };
  }
  return { kind: 'invalid', message: 'This viewer route is not supported.' };
}

/** The token is always the first URL path segment, independent of a deep-link directory. */
export function tokenRootUrl(input: string | URL): URL {
  const url = input instanceof URL ? new URL(input.href) : new URL(input);
  const token = url.pathname.split('/').filter(Boolean)[0];
  if (!token) throw new Error('viewer token is missing');
  return new URL(`/${token}/`, url.origin);
}

/** Prefer the final root-manifest response path when deriving token-scoped APIs. */
export function tokenRootFromManifestUrl(input: string | URL): URL {
  const url = input instanceof URL ? new URL(input.href) : new URL(input);
  const suffix = '/api/manifest';
  if (url.pathname.endsWith(suffix)) {
    url.pathname = `${url.pathname.slice(0, -suffix.length)}/`;
    url.search = '';
    url.hash = '';
    return url;
  }
  return tokenRootUrl(url);
}

export function canonicalizeViewerRoute(
  route: ParsedViewerRoute,
  manifest: AssociationVisualizationManifestV1,
): ParsedViewerRoute {
  if (route.kind === 'invalid' || route.kind === 'root') return route;
  const canonical = canonicalContig(route.contig);
  const contig = canonical && manifest.layout.contigs.find((entry) => entry.name === canonical);
  if (!contig) return { kind: 'invalid', message: 'The requested chromosome is not in this dataset.' };
  if (route.kind === 'chromosome') return { ...route, contig: contig.name };
  if (route.start < 1 || route.end < route.start || route.end > contig.length) {
    return { kind: 'invalid', message: 'The requested locus is outside this chromosome.' };
  }
  return { ...route, contig: contig.name };
}

export function viewerRouteUrl(root: URL, route: ViewerRoute): URL {
  const url = new URL(root.href);
  if (route.kind === 'chromosome') url.pathname += `chromosome/${encodeURIComponent(route.contig)}`;
  if (route.kind === 'locus') url.pathname += `locus/${encodeURIComponent(route.contig)}/${route.start}-${route.end}`;
  if (route.selectedSourceOrdinal !== null) url.searchParams.set('selected', String(route.selectedSourceOrdinal));
  return url;
}

export function withSelectedOrdinal(route: ViewerRoute, selectedSourceOrdinal: number | null): ViewerRoute {
  return { ...route, selectedSourceOrdinal };
}

/** Return an inclusive, deterministic window of at most one million bases around a hit. */
export function locusWindowForHit(
  hit: AssociationPointV1,
  manifest: AssociationVisualizationManifestV1,
): Extract<ViewerRoute, { kind: 'locus' }> | null {
  const canonical = canonicalContig(hit.contig);
  const contig = canonical && manifest.layout.contigs.find((entry) => entry.name === canonical);
  if (!contig || !Number.isSafeInteger(hit.position) || hit.position < 1 || hit.position > contig.length) return null;
  const width = Math.min(1_000_000, contig.length);
  const unclampedStart = hit.position - Math.floor((width - 1) / 2);
  const start = Math.max(1, Math.min(unclampedStart, contig.length - width + 1));
  return {
    kind: 'locus',
    contig: contig.name,
    start,
    end: start + width - 1,
    selectedSourceOrdinal: hit.source_ordinal,
  };
}
