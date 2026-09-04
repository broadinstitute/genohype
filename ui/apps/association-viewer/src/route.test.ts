import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { describe, expect, it } from 'vitest';
import type { AssociationVisualizationManifestV1 } from '@genohype/association-viz';
import {
  canonicalizeViewerRoute,
  locusWindowForHit,
  parseViewerRoute,
  tokenRootFromManifestUrl,
  tokenRootUrl,
  viewerRouteUrl,
} from './route.js';

function manifest(): AssociationVisualizationManifestV1 {
  return JSON.parse(readFileSync(
    resolve(process.cwd(), '../../../association/tests/fixtures/association-visualization-v1.json'),
    'utf8',
  ));
}

describe('viewer routes', () => {
  it('parses only root, chromosome, and inclusive locus paths under the first token', () => {
    expect(parseViewerRoute('https://host.test/token/?selected=7')).toEqual({ kind: 'root', selectedSourceOrdinal: 7 });
    expect(parseViewerRoute('https://host.test/token/chromosome/chr2')).toEqual({ kind: 'chromosome', contig: 'chr2', selectedSourceOrdinal: null });
    expect(parseViewerRoute('https://host.test/token/locus/2/1-20?selected=0')).toEqual({ kind: 'locus', contig: '2', start: 1, end: 20, selectedSourceOrdinal: 0 });
    expect(parseViewerRoute('https://host.test/token/other')).toMatchObject({ kind: 'invalid' });
    expect(parseViewerRoute('https://host.test/token/locus/2/20-1')).toMatchObject({ kind: 'locus' });
    expect(parseViewerRoute('https://host.test/token/?selected=-1')).toMatchObject({ kind: 'invalid' });
    expect(parseViewerRoute('https://host.test/token/?unexpected=1')).toMatchObject({ kind: 'invalid' });
  });

  it('canonicalizes aliases and validates contigs and bounds against the root manifest', () => {
    expect(canonicalizeViewerRoute(parseViewerRoute('https://host.test/token/chromosome/chr02'), manifest()))
      .toEqual({ kind: 'chromosome', contig: '2', selectedSourceOrdinal: null });
    expect(canonicalizeViewerRoute(parseViewerRoute('https://host.test/token/locus/2/1-20'), manifest()))
      .toEqual({ kind: 'locus', contig: '2', start: 1, end: 20, selectedSourceOrdinal: null });
    expect(canonicalizeViewerRoute(parseViewerRoute('https://host.test/token/locus/2/20-1'), manifest()))
      .toMatchObject({ kind: 'invalid' });
    expect(canonicalizeViewerRoute(parseViewerRoute('https://host.test/token/chromosome/X'), manifest()))
      .toMatchObject({ kind: 'invalid' });
  });

  it('derives token roots independently of detail directories and prefers final manifest URLs', () => {
    expect(tokenRootUrl('https://host.test/token/locus/4/1-2').href).toBe('https://host.test/token/');
    expect(tokenRootFromManifestUrl('https://host.test/redirected/api/manifest').href).toBe('https://host.test/redirected/');
    expect(viewerRouteUrl(new URL('https://host.test/token/'), {
      kind: 'chromosome', contig: 'X PAR', selectedSourceOrdinal: 12,
    }).href).toBe('https://host.test/token/chromosome/X%20PAR?selected=12');
  });

  it('creates a clamped deterministic inclusive million-base hit window', () => {
    const root = manifest();
    expect(locusWindowForHit(root.top_hits[0], root)).toEqual({
      kind: 'locus', contig: '1', start: 1, end: 10, selectedSourceOrdinal: 1,
    });
    root.layout.contigs[0].length = 2_000_000;
    const middle = { ...root.top_hits[0], position: 1_000_000 };
    expect(locusWindowForHit(middle, root)).toEqual({
      kind: 'locus', contig: '1', start: 500_001, end: 1_500_000, selectedSourceOrdinal: 1,
    });
    expect(locusWindowForHit({ ...middle, position: 1 }, root)).toEqual({
      kind: 'locus', contig: '1', start: 1, end: 1_000_000, selectedSourceOrdinal: 1,
    });
  });
});
