import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import { validateAssociationRegionPair } from '../contracts/parse.js';
import type { GeneAnnotationsV1 } from '../contracts/types.js';
import { rawGoldenRegionManifest, rawGoldenRegionOverlay } from '../test/fixture.js';
import { GeneTrack } from './GeneTrack.js';

const { manifest, overlay } = validateAssociationRegionPair(
  rawGoldenRegionManifest(),
  rawGoldenRegionOverlay(),
);

function annotations(state: GeneAnnotationsV1['state'], message?: string): GeneAnnotationsV1 {
  return { state, message, features: [] };
}

describe('GeneTrack', () => {
  it('shows unavailable, provider-error, and available-empty states distinctly', () => {
    const { rerender } = render(<GeneTrack annotations={annotations('unavailable')} layout={manifest.layout} />);
    expect(screen.getByRole('status')).toHaveTextContent('Gene annotations unavailable');

    rerender(<GeneTrack annotations={annotations('error', 'Reference timed out')} layout={manifest.layout} />);
    expect(screen.getByRole('alert')).toHaveTextContent('Gene annotations could not be loaded');
    expect(screen.getByRole('alert')).toHaveTextContent('Reference timed out');

    rerender(<GeneTrack annotations={annotations('available')} layout={manifest.layout} />);
    expect(screen.getByRole('status')).toHaveTextContent('No gene annotations in this region');
  });

  it('renders bounded lanes and aligns genes that overlap both locus edges', () => {
    const edgeAnnotations: GeneAnnotationsV1 = {
      state: 'available',
      features: [{
        id: 'edge-gene',
        symbol: 'EDGE',
        contig: manifest.layout.contig,
        start: manifest.layout.effective_start,
        end: manifest.layout.effective_end,
        strand: 'minus',
      }],
    };
    const { container } = render(<GeneTrack annotations={edgeAnnotations} layout={manifest.layout} />);
    expect(screen.getByRole('list', { name: /Gene annotations/ })).toBeInTheDocument();
    expect(screen.getByRole('listitem', { name: /EDGE/ })).toBeInTheDocument();
    const span = container.querySelector('.association-gene-span');
    expect(span).toHaveAttribute('x1', '0');
    expect(Number(span?.getAttribute('x2'))).toBeCloseTo(799.5);
    expect(container.querySelector('svg')).toHaveAttribute('viewBox', expect.stringMatching(/^0 0 800 /));
  });

  it('renders available contract annotations', () => {
    render(<GeneTrack annotations={overlay.gene_annotations} layout={manifest.layout} />);
    expect(screen.getByRole('listitem', { name: /GENE1/ })).toHaveAttribute('data-gene-id', 'ENSG000001');
  });
});
