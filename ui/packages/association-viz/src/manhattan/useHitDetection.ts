import { useCallback, useMemo } from 'react';

export interface PositionedHit {
  key: string;
  x_normalized: number;
  y_normalized: number;
}

export interface HitDetectionOptions {
  cellSize?: number;
  hitRadius?: number;
}

interface IndexedHit<T> {
  hit: T;
  x: number;
  y: number;
}

export interface HitIndex<T> {
  findHit(x: number, y: number): T | null;
}

/** Build the sparse 20 px grid used by the viewer; ties resolve by stable hit key. */
export function createHitIndex<T extends PositionedHit>(
  hits: readonly T[],
  width: number,
  height: number,
  options: HitDetectionOptions = {},
): HitIndex<T> {
  const cellSize = options.cellSize ?? 20;
  const hitRadius = options.hitRadius ?? 10;
  const grid = new Map<string, IndexedHit<T>[]>();
  if (width > 0 && height > 0 && cellSize > 0 && hitRadius > 0) {
    for (const hit of hits) {
      const indexed = { hit, x: hit.x_normalized * width, y: hit.y_normalized * height };
      const key = `${Math.floor(indexed.x / cellSize)},${Math.floor(indexed.y / cellSize)}`;
      const cell = grid.get(key);
      if (cell) cell.push(indexed);
      else grid.set(key, [indexed]);
    }
  }
  return {
    findHit(mouseX, mouseY) {
      if (width <= 0 || height <= 0 || !Number.isFinite(mouseX) || !Number.isFinite(mouseY)) return null;
      const gridX = Math.floor(mouseX / cellSize);
      const gridY = Math.floor(mouseY / cellSize);
      const candidates: IndexedHit<T>[] = [];
      for (let dx = -1; dx <= 1; dx += 1) {
        for (let dy = -1; dy <= 1; dy += 1) {
          candidates.push(...(grid.get(`${gridX + dx},${gridY + dy}`) ?? []));
        }
      }
      let best: IndexedHit<T> | null = null;
      let bestSquared = hitRadius * hitRadius;
      for (const candidate of candidates) {
        const squared = (mouseX - candidate.x) ** 2 + (mouseY - candidate.y) ** 2;
        if (
          squared < bestSquared
          || (squared === bestSquared && best !== null && candidate.hit.key < best.hit.key)
        ) {
          best = candidate;
          bestSquared = squared;
        }
      }
      return best?.hit ?? null;
    },
  };
}

export function useHitDetection<T extends PositionedHit>(
  hits: readonly T[],
  width: number,
  height: number,
  options: HitDetectionOptions = {},
): HitIndex<T> {
  const { cellSize = 20, hitRadius = 10 } = options;
  const index = useMemo(
    () => createHitIndex(hits, width, height, { cellSize, hitRadius }),
    [hits, width, height, cellSize, hitRadius],
  );
  return { findHit: useCallback((x, y) => index.findHit(x, y), [index]) };
}
