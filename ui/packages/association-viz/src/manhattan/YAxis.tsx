import type { YScaleV1 } from '../contracts/types.js';
import { yForNegLog10P } from './layout.js';

export interface YAxisProps {
  scale: YScaleV1;
  renderedHeight: number;
  width?: number;
}

export function generateYAxisTicks(scale: YScaleV1): number[] {
  const ticks = new Set<number>([0, scale.log_threshold, scale.max_neg_log_p]);
  const step = scale.log_threshold <= 10 ? 2 : scale.log_threshold / 5;
  for (let value = step; value < scale.log_threshold; value += step) ticks.add(value);
  for (let power = Math.ceil(Math.log10(scale.log_threshold)); power <= Math.floor(Math.log10(scale.max_neg_log_p)); power += 1) {
    for (const multiplier of [1, 3]) {
      const value = multiplier * 10 ** power;
      if (value > scale.log_threshold && value < scale.max_neg_log_p) ticks.add(value);
    }
  }
  return [...ticks].sort((a, b) => a - b);
}

export function YAxis({ scale, renderedHeight, width = 52 }: YAxisProps) {
  return (
    <svg
      className="association-y-axis"
      width={width}
      height={renderedHeight}
      aria-label="Manhattan plot y axis, negative log 10 p value"
    >
      <text className="association-y-axis-title" transform={`translate(11 ${renderedHeight / 2}) rotate(-90)`} textAnchor="middle">
        −log₁₀(p)
      </text>
      {generateYAxisTicks(scale).map((value) => {
        const y = yForNegLog10P(scale, value) * renderedHeight;
        if (y < 0 || y > renderedHeight) return null;
        const labelY = Math.min(Math.max(y, 7), Math.max(7, renderedHeight - 3));
        return (
          <g key={value} data-tick-value={value}>
            <text x={width - 8} y={labelY} textAnchor="end" dominantBaseline="middle">{value}</text>
            <line x1={width - 6} x2={width} y1={y} y2={y} />
          </g>
        );
      })}
    </svg>
  );
}
