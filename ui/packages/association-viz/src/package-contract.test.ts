import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { describe, expect, it } from 'vitest';
import {
  parseAssociationRegionOverlay,
  parseAssociationRegionRenderManifest,
  validateAssociationRegionPair,
  type AssociationRegionCountsV1,
  type AssociationRegionOverlayPointV1,
  type AssociationRegionOverlayV1,
  type AssociationRegionRenderManifestV1,
  type AssociationRegionResourcesV1,
  type DatasetIdV1,
  type GeneAnnotationStateV1,
  type GeneAnnotationsV1,
  type GeneFeatureV1,
  type GeneStrandV1,
  type RegionAggregationDisclosureV1,
  type RegionLayoutV1,
} from './index.js';

type PublicRegionContracts = [
  AssociationRegionCountsV1,
  AssociationRegionOverlayPointV1,
  AssociationRegionOverlayV1,
  AssociationRegionRenderManifestV1,
  AssociationRegionResourcesV1,
  DatasetIdV1,
  GeneAnnotationStateV1,
  GeneAnnotationsV1,
  GeneFeatureV1,
  GeneStrandV1,
  RegionAggregationDisclosureV1,
  RegionLayoutV1,
];

describe('package isolation contract', () => {
  it('exports the complete region contract and parser surface from package root', () => {
    const parsers: readonly unknown[] = [
      parseAssociationRegionOverlay,
      parseAssociationRegionRenderManifest,
      validateAssociationRegionPair,
    ];
    const compileOnly: PublicRegionContracts | undefined = undefined;
    expect(parsers.every((value) => typeof value === 'function')).toBe(true);
    expect(compileOnly).toBeUndefined();
  });

  it('has only React 18/19 peers and no product-state dependencies', () => {
    const pkg = JSON.parse(readFileSync(resolve(process.cwd(), 'package.json'), 'utf8'));
    expect(pkg.peerDependencies).toEqual({
      react: '^18.0.0 || ^19.0.0',
      'react-dom': '^18.0.0 || ^19.0.0',
    });
    const dependencyNames = Object.keys(pkg.dependencies ?? {});
    for (const forbidden of ['recoil', 'pouchdb', 'react-router', '@copilotkit/react-core']) {
      expect(dependencyNames).not.toContain(forbidden);
    }
    expect(pkg.exports['./styles.css']).toBe('./dist/styles.css');
    expect(pkg.scripts['contracts:generate']).toBe('node scripts/generate-contracts.mjs');
    expect(pkg.scripts['contracts:check']).toBe('node scripts/generate-contracts.mjs --check');
    for (const lifecycle of ['build', 'typecheck', 'test']) {
      expect(pkg.scripts[lifecycle]).toContain('npm run contracts:check');
    }
  });
});
