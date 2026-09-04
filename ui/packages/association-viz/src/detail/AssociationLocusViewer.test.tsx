import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { validateAssociationRegionPair } from '../contracts/parse.js';
import type { AssociationPointV1 } from '../contracts/types.js';
import { rawGoldenRegionManifest, rawGoldenRegionOverlay } from '../test/fixture.js';
import { AssociationLocusViewer } from './AssociationLocusViewer.js';

let resizeCallback: ResizeObserverCallback | undefined;
class ResizeObserverMock {
  constructor(callback: ResizeObserverCallback) { resizeCallback = callback; }
  observe = vi.fn();
  disconnect = vi.fn();
  unobserve = vi.fn();
}

function rect(width: number, height: number): DOMRect {
  return { x: 0, y: 0, top: 0, left: 0, right: width, bottom: height, width, height, toJSON: () => ({}) };
}

function pair() {
  return validateAssociationRegionPair(rawGoldenRegionManifest(), rawGoldenRegionOverlay());
}

function selectedAbsentPoint(): AssociationPointV1 {
  return {
    contig: '4',
    position: 69_000_000,
    ref_allele: 'C',
    alt_allele: 'T',
    pvalue: 0.25,
    neg_log10_p: 0.6020599913,
    source_ordinal: 99,
  };
}

describe('AssociationLocusViewer', () => {
  beforeEach(() => {
    vi.stubGlobal('ResizeObserver', ResizeObserverMock);
    vi.spyOn(HTMLImageElement.prototype, 'complete', 'get').mockReturnValue(false);
    vi.spyOn(HTMLImageElement.prototype, 'naturalWidth', 'get').mockReturnValue(1_600);
    vi.spyOn(HTMLImageElement.prototype, 'naturalHeight', 'get').mockReturnValue(640);
    resizeCallback = undefined;
  });

  it('renders logical SVG geometry over a DPR raster and resizes responsively', async () => {
    const { manifest, overlay } = pair();
    const { container } = render(
      <AssociationLocusViewer manifest={manifest} overlay={overlay} rasterUrl="locus.png" />,
    );
    const image = screen.getByRole('img', { name: /locus plot/ });
    expect(image).toHaveAttribute('width', '800');
    expect(image).toHaveAttribute('height', '320');
    vi.spyOn(image, 'getBoundingClientRect').mockReturnValue(rect(800, 320));
    fireEvent.load(image);

    const svg = container.querySelector('.association-locus-overlay');
    expect(svg).toHaveAttribute('viewBox', '0 0 800 320');
    expect(svg).toHaveAttribute('width', '800');
    expect(svg).toHaveAttribute('height', '320');
    expect(screen.getAllByRole('button', { name: /Association point/ })).toHaveLength(3);

    resizeCallback?.([{ contentRect: rect(400, 160) } as ResizeObserverEntry], {} as ResizeObserver);
    await waitFor(() => expect(svg).toHaveAttribute('width', '400'));
    expect(svg).toHaveAttribute('height', '160');
    // Logical marker coordinates stay fixed while the SVG's CSS box scales.
    expect(screen.getAllByRole('button', { name: /Association point/ })[0]).toHaveAttribute('cx', '0');
  });

  it('exposes semantic roles, tooltip, click, focus, and keyboard selection', () => {
    const { manifest, overlay } = pair();
    const onPointSelect = vi.fn();
    const onPointHover = vi.fn();
    const { container } = render(
      <AssociationLocusViewer
        manifest={manifest}
        overlay={overlay}
        rasterUrl="locus.png"
        selectedSourceOrdinal={9}
        onPointSelect={onPointSelect}
        onPointHover={onPointHover}
      />,
    );
    const image = screen.getByRole('img');
    vi.spyOn(image, 'getBoundingClientRect').mockReturnValue(rect(800, 320));
    fireEvent.load(image);

    const points = screen.getAllByRole('button', { name: /Association point/ });
    expect(points[0]).toHaveClass('is-significant', 'is-coding');
    expect(points[2]).toHaveClass('is-selected');
    expect(points[2]).toHaveAttribute('aria-pressed', 'true');
    expect(container.querySelector('.association-locus-selection')).toBeInTheDocument();

    fireEvent.mouseEnter(points[0]);
    expect(screen.getByRole('tooltip')).toHaveTextContent('underflow');
    expect(onPointHover).toHaveBeenLastCalledWith(expect.objectContaining({ source_ordinal: 7 }));
    fireEvent.focus(points[0]);
    fireEvent.click(points[1]);
    fireEvent.keyDown(points[2], { key: 'Enter' });
    fireEvent.keyDown(points[2], { key: ' ' });
    expect(onPointSelect).toHaveBeenNthCalledWith(1, expect.objectContaining({ source_ordinal: 8 }));
    expect(onPointSelect).toHaveBeenNthCalledWith(2, expect.objectContaining({ source_ordinal: 9 }));
    expect(onPointSelect).toHaveBeenCalledTimes(3);
    fireEvent.blur(points[0]);
    expect(onPointHover).toHaveBeenLastCalledWith(null);
  });

  it('adds an explicit selected marker when the point is absent from representatives', () => {
    const { manifest, overlay } = pair();
    const onPointSelect = vi.fn();
    const absent = selectedAbsentPoint();
    const { container } = render(
      <AssociationLocusViewer
        manifest={manifest}
        overlay={overlay}
        rasterUrl="locus.png"
        selectedSourceOrdinal={99}
        selectedPoint={absent}
        onPointSelect={onPointSelect}
      />,
    );
    const image = screen.getByRole('img');
    vi.spyOn(image, 'getBoundingClientRect').mockReturnValue(rect(800, 320));
    fireEvent.load(image);

    const marker = screen.getByRole('button', { name: /selected point outside ordinary representatives/ });
    expect(marker).toHaveClass('is-absent-representative', 'is-selected');
    expect(Number(marker.getAttribute('cx'))).toBeCloseTo(799.5);
    expect(container.querySelector('.association-locus-selection.is-absent-representative')).toBeInTheDocument();
    expect(screen.getAllByRole('button', { name: /Association point/ })).toHaveLength(4);
    fireEvent.click(marker);
    expect(onPointSelect).toHaveBeenCalledWith(expect.objectContaining({ source_ordinal: 99 }));
  });

  it('reports raster failures and rejects wrong natural physical dimensions', () => {
    const { manifest, overlay } = pair();
    const onResourceError = vi.fn();
    const { rerender } = render(
      <AssociationLocusViewer manifest={manifest} overlay={overlay} rasterUrl="bad.png" onResourceError={onResourceError} />,
    );
    fireEvent.error(screen.getByRole('img'));
    expect(screen.getByRole('alert')).toHaveTextContent('raster could not be loaded');
    expect(screen.getByRole('listitem', { name: /GENE1/ })).toBeInTheDocument();

    rerender(
      <AssociationLocusViewer manifest={manifest} overlay={overlay} rasterUrl="wrong.png" onResourceError={onResourceError} />,
    );
    vi.spyOn(HTMLImageElement.prototype, 'naturalWidth', 'get').mockReturnValue(800);
    vi.spyOn(HTMLImageElement.prototype, 'naturalHeight', 'get').mockReturnValue(320);
    fireEvent.load(screen.getByRole('img'));
    expect(onResourceError).toHaveBeenLastCalledWith(expect.objectContaining({ message: expect.stringContaining('physical dimensions') }));
  });

  it('rejects manifest-overlay mismatches before rendering a raster', async () => {
    const { manifest, overlay } = pair();
    const onResourceError = vi.fn();
    const mismatched = { ...overlay, render_hash: `sha256:${'0'.repeat(64)}` };
    render(
      <AssociationLocusViewer
        manifest={manifest}
        overlay={mismatched}
        rasterUrl="must-not-render.png"
        onResourceError={onResourceError}
      />,
    );
    expect(screen.getByRole('alert')).toHaveTextContent('data do not match');
    expect(screen.queryByRole('img')).not.toBeInTheDocument();
    await waitFor(() => expect(onResourceError).toHaveBeenCalledWith(
      expect.objectContaining({ message: 'region manifest and overlay do not match' }),
    ));
  });
});
