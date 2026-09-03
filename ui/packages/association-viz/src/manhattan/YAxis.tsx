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

const MIN_LABEL_CENTER_SPACING = 12;

export function YAxis({ scale, renderedHeight, width = 52 }: YAxisProps) {
  const ticks = generateYAxisTicks(scale).map((value) => {
    const y = yForNegLog10P(scale, value) * renderedHeight;
    const labelY = Math.min(Math.max(y, 7), Math.max(7, renderedHeight - 3));
    return { value, y, labelY };
  }).filter(({ y }) => y >= 0 && y <= renderedHeight);

  // Keep endpoint labels, then greedily admit interior labels from top to bottom.
  // Tick-line positions remain untouched even when a crowded label is omitted.
  const endpointValues = new Set([0, scale.max_neg_log_p]);
  const labelOrder = [
    ...ticks.filter(({ value }) => endpointValues.has(value)),
    ...ticks.filter(({ value }) => !endpointValues.has(value)).sort((a, b) => a.y - b.y),
  ];
  const visibleLabels = new Set<number>();
  const occupiedLabelCenters: number[] = [];
  for (const tick of labelOrder) {
    if (occupiedLabelCenters.every((center) => Math.abs(center - tick.labelY) >= MIN_LABEL_CENTER_SPACING)) {
      visibleLabels.add(tick.value);
      occupiedLabelCenters.push(tick.labelY);
    }
  }

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
      {ticks.map(({ value, y, labelY }) => (
        <g key={value} data-tick-value={value} data-label-visible={visibleLabels.has(value)}>
          {visibleLabels.has(value) && (
            <text x={width - 8} y={labelY} textAnchor="end" dominantBaseline="middle">{value}</text>
          )}
          <line x1={width - 6} x2={width} y1={y} y2={y} />
        </g>
      ))}
    </svg>
  );
}
