import { useMemo } from 'react';
import type { GeneAnnotationsV1, GeneFeatureV1, RegionLayoutV1 } from '../contracts/types.js';
import { xForRegionPosition } from './layout.js';

export interface GeneTrackProps {
  annotations: GeneAnnotationsV1;
  layout: RegionLayoutV1;
  className?: string;
}

interface PositionedGene {
  feature: GeneFeatureV1;
  x1: number;
  x2: number;
  lane: number;
}

const MAX_GENE_LANES = 6;
const LANE_HEIGHT = 22;
const TRACK_TOP = 18;

function positionGenes(annotations: GeneAnnotationsV1, layout: RegionLayoutV1): PositionedGene[] {
  const laneEnds = Array<number>(MAX_GENE_LANES).fill(Number.NEGATIVE_INFINITY);
  return annotations.features.flatMap((feature) => {
    const x1 = xForRegionPosition(layout, feature.start);
    const x2 = xForRegionPosition(layout, feature.end);
    if (feature.contig !== layout.contig || x1 === null || x2 === null) return [];
    const firstOpenLane = laneEnds.findIndex((end) => x1 >= end + 8);
    const lane = firstOpenLane < 0 ? MAX_GENE_LANES - 1 : firstOpenLane;
    laneEnds[lane] = Math.max(laneEnds[lane], x2);
    return [{ feature, x1, x2, lane }];
  });
}

function geneLabel(feature: GeneFeatureV1): string {
  const name = feature.symbol || feature.id;
  return `${name}, ${feature.contig}:${feature.start.toLocaleString()}–${feature.end.toLocaleString()}, ${feature.strand} strand`;
}

export function GeneTrack({ annotations, layout, className }: GeneTrackProps) {
  const genes = useMemo(() => positionGenes(annotations, layout), [annotations, layout]);
  const classes = ['association-gene-track', className].filter(Boolean).join(' ');

  if (annotations.state === 'unavailable') {
    return (
      <div className={`${classes} association-gene-state`} role="status">
        <strong>Gene annotations unavailable.</strong>
        {annotations.message && <span>{annotations.message}</span>}
      </div>
    );
  }
  if (annotations.state === 'error') {
    return (
      <div className={`${classes} association-gene-state is-error`} role="alert">
        <strong>Gene annotations could not be loaded.</strong>
        {annotations.message && <span>{annotations.message}</span>}
      </div>
    );
  }
  if (genes.length === 0) {
    return (
      <div className={`${classes} association-gene-state`} role="status">
        <strong>No gene annotations in this region.</strong>
        {annotations.message && <span>{annotations.message}</span>}
      </div>
    );
  }

  const usedLanes = Math.max(...genes.map((gene) => gene.lane)) + 1;
  const height = TRACK_TOP + usedLanes * LANE_HEIGHT + 8;
  return (
    <div className={classes}>
      <svg
        role="list"
        aria-label={`Gene annotations for chromosome ${layout.contig}`}
        viewBox={`0 0 ${layout.width} ${height}`}
        preserveAspectRatio="none"
      >
        <text className="association-gene-track-title" x={0} y={11}>Genes</text>
        {genes.map(({ feature, x1, x2, lane }) => {
          const y = TRACK_TOP + lane * LANE_HEIGHT + 8;
          const key = `${feature.id}:${feature.start}:${feature.end}:${feature.strand}`;
          const direction = feature.strand === 'minus' ? -1 : 1;
          const arrowX = direction > 0 ? x2 : x1;
          const lineStart = Math.min(x1, x2);
          const lineEnd = Math.max(x1, x2);
          return (
            <g
              key={key}
              role="listitem"
              aria-label={geneLabel(feature)}
              className={`association-gene is-${feature.strand}`}
              data-gene-id={feature.id}
            >
              <line className="association-gene-span" x1={x1} x2={x2} y1={y} y2={y} />
              <path
                className="association-gene-direction"
                d={`M ${arrowX} ${y} l ${-direction * 5} -4 l 0 8 z`}
              />
              <text
                className="association-gene-label"
                x={Math.min(Math.max((lineStart + lineEnd) / 2, 2), layout.width - 2)}
                y={y - 5}
                textAnchor={lineStart < 12 ? 'start' : lineEnd > layout.width - 12 ? 'end' : 'middle'}
              >
                {feature.symbol || feature.id}
              </text>
            </g>
          );
        })}
      </svg>
    </div>
  );
}
