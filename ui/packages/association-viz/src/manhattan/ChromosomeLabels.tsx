import type { GenomeLayoutV1 } from '../contracts/types.js';
import { canonicalContig } from './layout.js';

export interface ChromosomeLabelsProps {
  layout: GenomeLayoutV1;
  width: number;
  height?: number;
  xRange?: { min: number; max: number };
  selectedContig?: string | null;
  onChromosomeClick?: (contig: string) => void;
}

export function ChromosomeLabels({
  layout,
  width,
  height = 26,
  xRange = { min: 0, max: 1 },
  selectedContig,
  onChromosomeClick,
}: ChromosomeLabelsProps) {
  if (width <= 0 || xRange.max <= xRange.min) return null;
  const span = xRange.max - xRange.min;
  return (
    <div className="association-chromosome-labels" style={{ width, height }} aria-label="Chromosomes">
      {layout.contigs.map((contig) => {
        const start = contig.x_start / layout.width;
        const end = contig.x_end / layout.width;
        const centerPx = (((start + end) / 2 - xRange.min) / span) * width;
        const visibleWidth = ((end - start) / span) * width;
        if (centerPx < -20 || centerPx > width + 20 || visibleWidth < 14) return null;
        const selected = selectedContig != null && canonicalContig(selectedContig) === contig.name;
        return (
          <button
            type="button"
            key={contig.name}
            className={`association-chromosome-label${selected ? ' is-selected' : ''}`}
            style={{ left: centerPx }}
            onClick={() => onChromosomeClick?.(contig.name)}
            disabled={!onChromosomeClick}
            aria-pressed={selected}
            aria-label={`Chromosome ${contig.name}`}
          >
            {contig.name}
          </button>
        );
      })}
    </div>
  );
}
