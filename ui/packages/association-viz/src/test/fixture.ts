import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import type { AssociationVisualizationManifestV1 } from '../contracts/types.js';

export function rawGoldenManifest(): unknown {
  return JSON.parse(readFileSync(
    resolve(process.cwd(), '../../../association/tests/fixtures/association-visualization-v1.json'),
    'utf8',
  ));
}

export function rawGoldenRegionManifest(): unknown {
  return JSON.parse(readFileSync(
    resolve(process.cwd(), '../../../association/tests/fixtures/association-region-render-v1.json'),
    'utf8',
  ));
}

export function rawGoldenRegionOverlay(): unknown {
  return JSON.parse(readFileSync(
    resolve(process.cwd(), '../../../association/tests/fixtures/association-region-overlay-v1.json'),
    'utf8',
  ));
}

export function goldenManifest(): AssociationVisualizationManifestV1 {
  return rawGoldenManifest() as AssociationVisualizationManifestV1;
}

export function fullGrch38Manifest(): AssociationVisualizationManifestV1 {
  return JSON.parse(readFileSync(
    resolve(process.cwd(), '../../../association/tests/fixtures/association-visualization-v1-phase1-grch38-3000x800.json'),
    'utf8',
  )) as AssociationVisualizationManifestV1;
}
