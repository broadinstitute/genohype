import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { describe, expect, it } from 'vitest';
import parity from '../test/visual-parity-v1.json';
import { goldenManifest } from '../test/fixture.js';
import { generateYAxisTicks } from './YAxis.js';
import { computeDisplayHits } from './layout.js';

describe('reviewed deterministic visual parity fixture', () => {
  it('locks layout, axis, and sparse overlay pixels at multiple viewports', () => {
    const manifest = goldenManifest();
    expect(manifest.layout_hash).toBe(parity.layout_hash);
    expect(generateYAxisTicks(manifest.layout.y_scale)).toEqual(parity.axis_ticks);
    const hits = computeDisplayHits(manifest.top_hits, manifest.layout);
    for (const viewport of parity.viewports) {
      expect(hits.map((hit) => ({
        key: hit.key,
        x: hit.x_normalized * viewport.width,
        y: hit.y_normalized * viewport.height,
      }))).toEqual(viewport.hits);
    }
    expect(hits).toHaveLength(manifest.top_hits.length);
    expect(hits.length).toBeLessThan(manifest.rendered_point_count + 1);
  });

  it('keeps parity styling generic and avoids the old raster clipping workaround', () => {
    const css = readFileSync(resolve(process.cwd(), 'src/styles.css'), 'utf8');
    expect(css).toContain('--association-raster-filter: none');
    expect(css).toContain('.association-overlay');
    expect(css).toContain('.association-hit-table');
    expect(css).not.toContain('clip-path');
  });
});
