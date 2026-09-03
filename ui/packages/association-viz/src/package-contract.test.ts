import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { describe, expect, it } from 'vitest';

describe('package isolation contract', () => {
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
  });
});
