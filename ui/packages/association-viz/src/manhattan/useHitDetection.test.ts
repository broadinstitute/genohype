import { describe, expect, it } from 'vitest';
import { createHitIndex } from './useHitDetection.js';

const hits = [
  { key: 'b', x_normalized: 0.5, y_normalized: 0.5 },
  { key: 'a', x_normalized: 0.5, y_normalized: 0.5 },
  { key: 'far', x_normalized: 0.9, y_normalized: 0.1 },
];

describe('sparse hit detection', () => {
  it('finds the nearest hit and deterministically breaks equal-distance ties', () => {
    const index = createHitIndex(hits, 200, 100);
    expect(index.findHit(100, 50)?.key).toBe('a');
    expect(index.findHit(180, 10)?.key).toBe('far');
    expect(index.findHit(0, 0)).toBeNull();
  });

  it('reindexes coordinates when viewport dimensions change', () => {
    expect(createHitIndex(hits, 200, 100).findHit(100, 50)?.key).toBe('a');
    expect(createHitIndex(hits, 400, 200).findHit(100, 50)).toBeNull();
    expect(createHitIndex(hits, 400, 200).findHit(200, 100)?.key).toBe('a');
  });

  it('returns no hit without positive dimensions', () => {
    expect(createHitIndex(hits, 0, 100).findHit(0, 0)).toBeNull();
    expect(createHitIndex(hits, 100, 0).findHit(0, 0)).toBeNull();
  });
});
