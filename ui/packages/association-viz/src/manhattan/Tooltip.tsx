import type { DisplayHit } from './layout.js';

export interface TooltipProps {
  hit: DisplayHit;
  x: number;
  y: number;
  containerWidth: number;
}

export function formatAssociationPValue(hit: Pick<DisplayHit, 'pvalue' | 'neg_log10_p'>): string {
  if (hit.pvalue === 0) return `underflow (−log₁₀ p = ${hit.neg_log10_p.toLocaleString()})`;
  if (hit.neg_log10_p > 100) return `≈ 1e−${Math.round(hit.neg_log10_p)}`;
  return hit.pvalue.toExponential(2);
}

export function Tooltip({ hit, x, y, containerWidth }: TooltipProps) {
  const flip = x > containerWidth * 0.8;
  const variant = hit.ref_allele && hit.alt_allele
    ? `${hit.contig}-${hit.position.toLocaleString()}-${hit.ref_allele}-${hit.alt_allele}`
    : `${hit.contig}:${hit.position.toLocaleString()}`;
  return (
    <div
      role="tooltip"
      className="association-tooltip"
      style={{ top: Math.max(0, y - 10), left: flip ? undefined : x + 14, right: flip ? containerWidth - x + 14 : undefined }}
    >
      <strong>{variant}</strong>
      {hit.gene_symbol && <span>Gene: {hit.gene_symbol}</span>}
      {hit.consequence && <span>{hit.consequence.replace(/_/g, ' ')}</span>}
      <span>P = {formatAssociationPValue(hit)}</span>
      {hit.beta != null && <span>Beta = {hit.beta.toPrecision(3)}</span>}
      {hit.allele_frequency != null && <span>AF = {hit.allele_frequency.toPrecision(3)}</span>}
    </div>
  );
}
