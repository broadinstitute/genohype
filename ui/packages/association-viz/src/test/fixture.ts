import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import type { AssociationVisualizationManifestV1 } from '../contracts/types.js';

export function rawGoldenManifest(): unknown {
  return JSON.parse(readFileSync(
    resolve(process.cwd(), '../../../association/tests/fixtures/association-visualization-v1.json'),
    'utf8',
  ));
}

export function goldenManifest(): AssociationVisualizationManifestV1 {
  return rawGoldenManifest() as AssociationVisualizationManifestV1;
}
