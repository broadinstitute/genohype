import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { goldenManifest } from '../test/fixture.js';
import { AssociationManhattanViewer } from './AssociationManhattanViewer.js';

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

describe('AssociationManhattanViewer', () => {
  beforeEach(() => {
    vi.stubGlobal('ResizeObserver', ResizeObserverMock);
    vi.spyOn(HTMLImageElement.prototype, 'complete', 'get').mockReturnValue(false);
    vi.spyOn(HTMLImageElement.prototype, 'naturalWidth', 'get').mockReturnValue(0);
    vi.spyOn(HTMLImageElement.prototype, 'naturalHeight', 'get').mockReturnValue(0);
    resizeCallback = undefined;
  });

  it('loads, resizes, hits, opens loci, and reports chromosome interactions', async () => {
    const onHitClick = vi.fn();
    const onHitHover = vi.fn();
    const onChromosomeClick = vi.fn();
    const onOpenLocus = vi.fn();
    const onResourceLoad = vi.fn();
    const { container } = render(
      <AssociationManhattanViewer
        manifest={goldenManifest()}
        rasterUrl="https://example.test/plot.png"
        selectedHitKey="1:3:A:G:1"
        onHitClick={onHitClick}
        onHitHover={onHitHover}
        onChromosomeClick={onChromosomeClick}
        onOpenLocus={onOpenLocus}
        onResourceLoad={onResourceLoad}
      />,
    );
    expect(screen.getByRole('status')).toHaveTextContent('Loading association raster');
    const image = screen.getByRole('img');
    vi.spyOn(image, 'getBoundingClientRect').mockReturnValue(rect(100, 50));
    fireEvent.load(image);
    expect(onResourceLoad).toHaveBeenCalledOnce();
    expect(screen.queryByRole('status')).not.toBeInTheDocument();
    const markers = screen.getAllByRole('button', { name: /Association hit/ });
    expect(markers).toHaveLength(2);
    expect(container.querySelector('.association-hit-selection')).toBeInTheDocument();

    const overlay = container.querySelector('.association-overlay') as SVGSVGElement;
    vi.spyOn(overlay, 'getBoundingClientRect').mockReturnValue(rect(100, 50));
    fireEvent.mouseMove(overlay, { clientX: 6.4, clientY: 1 });
    expect(await screen.findByRole('tooltip')).toHaveTextContent('underflow');
    expect(onHitHover).toHaveBeenLastCalledWith(expect.objectContaining({ contig: '1', position: 3 }));
    fireEvent.click(markers[0]);
    expect(onHitClick).toHaveBeenCalledWith(expect.objectContaining({ key: '1:3:A:G:1' }));
    fireEvent.doubleClick(markers[0]);
    expect(onOpenLocus).toHaveBeenCalledWith(expect.objectContaining({ contig: '1', position: 3 }));

    fireEvent.mouseLeave(overlay);
    fireEvent.click(overlay, { clientX: 50, clientY: 20 });
    expect(onChromosomeClick).toHaveBeenCalledWith('2');
    fireEvent.click(screen.getByRole('button', { name: 'Chromosome 1' }));
    expect(onChromosomeClick).toHaveBeenCalledWith('1');

    resizeCallback?.([{ contentRect: rect(200, 100) } as ResizeObserverEntry], {} as ResizeObserver);
    await waitFor(() => expect(markers[0]).toHaveAttribute('cx', '12.8'));
  });

  it('reports image failures and dimension mismatches', () => {
    const onResourceError = vi.fn();
    const { rerender } = render(
      <AssociationManhattanViewer manifest={goldenManifest()} rasterUrl="bad.png" onResourceError={onResourceError} />,
    );
    fireEvent.error(screen.getByRole('img'));
    expect(screen.getByRole('alert')).toHaveTextContent('could not be loaded');
    expect(onResourceError).toHaveBeenCalledWith(expect.objectContaining({ message: 'failed to load association raster' }));

    rerender(<AssociationManhattanViewer manifest={goldenManifest()} rasterUrl="wrong-size.png" onResourceError={onResourceError} />);
    vi.spyOn(HTMLImageElement.prototype, 'naturalWidth', 'get').mockReturnValue(101);
    vi.spyOn(HTMLImageElement.prototype, 'naturalHeight', 'get').mockReturnValue(50);
    fireEvent.load(screen.getByRole('img'));
    expect(onResourceError).toHaveBeenLastCalledWith(expect.objectContaining({ message: expect.stringContaining('do not match') }));
  });

  it('handles an already cached image and observes the actual image element', async () => {
    vi.spyOn(HTMLImageElement.prototype, 'complete', 'get').mockReturnValue(true);
    vi.spyOn(HTMLImageElement.prototype, 'naturalWidth', 'get').mockReturnValue(100);
    vi.spyOn(HTMLImageElement.prototype, 'naturalHeight', 'get').mockReturnValue(50);
    vi.spyOn(HTMLImageElement.prototype, 'getBoundingClientRect').mockReturnValue(rect(100, 50));
    render(<AssociationManhattanViewer manifest={goldenManifest()} rasterUrl="cached.png" />);
    await waitFor(() => expect(screen.queryByRole('status')).not.toBeInTheDocument());
    expect(screen.getAllByRole('button', { name: /Association hit/ })).toHaveLength(2);
  });
});
