import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import type { AssociationVisualizationManifestV1 } from '../contracts/types.js';
import { ChromosomeLabels } from './ChromosomeLabels.js';
import { computeDisplayHits, contigAtNormalizedX, type DisplayHit } from './layout.js';
import { Tooltip } from './Tooltip.js';
import { useHitDetection } from './useHitDetection.js';
import { YAxis } from './YAxis.js';

export interface AssociationManhattanViewerProps {
  manifest: AssociationVisualizationManifestV1;
  rasterUrl: string;
  selectedHitKey?: string | null;
  selectedContig?: string | null;
  showYAxis?: boolean;
  showStats?: boolean;
  className?: string;
  onHitClick?: (hit: DisplayHit) => void;
  onHitHover?: (hit: DisplayHit | null) => void;
  onChromosomeClick?: (contig: string) => void;
  onOpenLocus?: (hit: DisplayHit) => void;
  onResourceLoad?: () => void;
  onResourceError?: (error: Error) => void;
}

interface Dimensions { width: number; height: number }

export function AssociationManhattanViewer({
  manifest,
  rasterUrl,
  selectedHitKey,
  selectedContig,
  showYAxis = true,
  showStats = true,
  className,
  onHitClick,
  onHitHover,
  onChromosomeClick,
  onOpenLocus,
  onResourceLoad,
  onResourceError,
}: AssociationManhattanViewerProps) {
  const imageRef = useRef<HTMLImageElement>(null);
  const [dimensions, setDimensions] = useState<Dimensions>({ width: 0, height: 0 });
  const [imageState, setImageState] = useState<'loading' | 'loaded' | 'error'>('loading');
  const [hoveredHit, setHoveredHit] = useState<DisplayHit | null>(null);
  const [cursor, setCursor] = useState({ x: 0, y: 0 });
  const displayHits = useMemo(
    () => computeDisplayHits(manifest.top_hits, manifest.layout),
    [manifest.top_hits, manifest.layout],
  );
  const hitIndex = useHitDetection(displayHits, dimensions.width, dimensions.height);

  useEffect(() => {
    setImageState('loading');
    setDimensions({ width: 0, height: 0 });
    setHoveredHit(null);
  }, [rasterUrl]);

  const updateDimensions = useCallback((image: HTMLImageElement) => {
    const rect = image.getBoundingClientRect();
    if (rect.width > 0 && rect.height > 0) setDimensions({ width: rect.width, height: rect.height });
    else if (image.clientWidth > 0) {
      setDimensions({
        width: image.clientWidth,
        height: image.clientWidth * manifest.layout.height / manifest.layout.width,
      });
    }
  }, [manifest.layout.height, manifest.layout.width]);

  const failImage = useCallback((message: string) => {
    const error = new Error(message);
    setImageState('error');
    setDimensions({ width: 0, height: 0 });
    onResourceError?.(error);
  }, [onResourceError]);

  const handleImageLoad = useCallback(() => {
    const image = imageRef.current;
    if (!image) return;
    if (
      image.naturalWidth > 0
      && (image.naturalWidth !== manifest.layout.width || image.naturalHeight !== manifest.layout.height)
    ) {
      failImage(`raster dimensions ${image.naturalWidth}×${image.naturalHeight} do not match manifest layout ${manifest.layout.width}×${manifest.layout.height}`);
      return;
    }
    setImageState('loaded');
    updateDimensions(image);
    onResourceLoad?.();
  }, [failImage, manifest.layout.height, manifest.layout.width, onResourceLoad, updateDimensions]);

  useEffect(() => {
    const image = imageRef.current;
    if (!image) return;
    const observer = typeof ResizeObserver === 'undefined'
      ? null
      : new ResizeObserver((entries) => {
        const entry = entries[0];
        if (entry?.contentRect.width && entry.contentRect.height) {
          setDimensions({ width: entry.contentRect.width, height: entry.contentRect.height });
        }
      });
    observer?.observe(image);
    if (image.complete) {
      if (image.naturalWidth > 0) handleImageLoad();
      else failImage('failed to load association raster');
    }
    return () => observer?.disconnect();
  }, [rasterUrl, handleImageLoad, failImage]);

  const handlePointerMove = useCallback((event: React.MouseEvent<SVGSVGElement>) => {
    const rect = event.currentTarget.getBoundingClientRect();
    const point = { x: event.clientX - rect.left, y: event.clientY - rect.top };
    setCursor(point);
    const next = hitIndex.findHit(point.x, point.y);
    if (next?.key !== hoveredHit?.key) {
      setHoveredHit(next);
      onHitHover?.(next);
    }
  }, [hitIndex, hoveredHit?.key, onHitHover]);

  const clearHover = useCallback(() => {
    setHoveredHit(null);
    onHitHover?.(null);
  }, [onHitHover]);

  const handlePlotClick = useCallback((event: React.MouseEvent<SVGSVGElement>) => {
    if (hoveredHit) {
      onHitClick?.(hoveredHit);
      return;
    }
    if (!onChromosomeClick || dimensions.width <= 0) return;
    const rect = event.currentTarget.getBoundingClientRect();
    const contig = contigAtNormalizedX(manifest.layout, (event.clientX - rect.left) / dimensions.width);
    if (contig) onChromosomeClick(contig);
  }, [dimensions.width, hoveredHit, manifest.layout, onChromosomeClick, onHitClick]);

  const axisWidth = showYAxis ? 52 : 0;
  const classes = ['association-manhattan', className].filter(Boolean).join(' ');
  return (
    <section className={classes} aria-label={manifest.title}>
      <div className="association-plot-row">
        {showYAxis && imageState === 'loaded' && dimensions.height > 0 && (
          <YAxis scale={manifest.layout.y_scale} renderedHeight={dimensions.height} width={axisWidth} />
        )}
        <div className="association-image-wrapper">
          <img
            ref={imageRef}
            className="association-raster"
            src={rasterUrl}
            alt={`${manifest.title} Manhattan plot`}
            draggable={false}
            onLoad={handleImageLoad}
            onError={() => failImage('failed to load association raster')}
            style={{ aspectRatio: `${manifest.layout.width} / ${manifest.layout.height}` }}
          />
          {imageState === 'loaded' && dimensions.width > 0 && dimensions.height > 0 && (
            <svg
              className="association-overlay"
              width={dimensions.width}
              height={dimensions.height}
              viewBox={`0 0 ${dimensions.width} ${dimensions.height}`}
              preserveAspectRatio="none"
              aria-label="Interactive top association hits"
              onMouseMove={handlePointerMove}
              onMouseLeave={clearHover}
              onClick={handlePlotClick}
            >
              {displayHits.map((hit) => {
                const selected = hit.key === selectedHitKey;
                const hovered = hit.key === hoveredHit?.key;
                return (
                  <g key={hit.key}>
                    {selected && (
                      <circle
                        className="association-hit-selection"
                        cx={hit.x_normalized * dimensions.width}
                        cy={hit.y_normalized * dimensions.height}
                        r={8}
                      />
                    )}
                    <circle
                      role="button"
                      tabIndex={0}
                      aria-label={`Association hit ${hit.contig}:${hit.position}`}
                      aria-pressed={selected}
                      className={`association-hit${hovered ? ' is-hovered' : ''}`}
                      cx={hit.x_normalized * dimensions.width}
                      cy={hit.y_normalized * dimensions.height}
                      r={hovered ? 5 : 3}
                      onFocus={() => { setHoveredHit(hit); onHitHover?.(hit); }}
                      onBlur={clearHover}
                      onClick={(event) => { event.stopPropagation(); onHitClick?.(hit); }}
                      onDoubleClick={(event) => { event.stopPropagation(); onOpenLocus?.(hit); }}
                      onKeyDown={(event) => {
                        if (event.key === 'Enter' || event.key === ' ') {
                          event.preventDefault();
                          onHitClick?.(hit);
                        } else if (event.key.toLowerCase() === 'o') onOpenLocus?.(hit);
                      }}
                    />
                  </g>
                );
              })}
            </svg>
          )}
          {hoveredHit && imageState === 'loaded' && (
            <Tooltip hit={hoveredHit} x={cursor.x} y={cursor.y} containerWidth={dimensions.width} />
          )}
        </div>
      </div>
      {imageState === 'loading' && <div className="association-loading" role="status">Loading association raster…</div>}
      {imageState === 'error' && <div className="association-error" role="alert">Association raster could not be loaded.</div>}
      {imageState === 'loaded' && dimensions.width > 0 && (
        <div style={{ marginLeft: axisWidth }}>
          <ChromosomeLabels
            layout={manifest.layout}
            width={dimensions.width}
            selectedContig={selectedContig}
            onChromosomeClick={onChromosomeClick}
          />
        </div>
      )}
      {showStats && imageState === 'loaded' && (
        <div className="association-stats" style={{ marginLeft: axisWidth }}>
          <span><strong>{manifest.rendered_point_count.toLocaleString()}</strong> rasterized points</span>
          <span><strong>{manifest.top_hits.length.toLocaleString()}</strong> interactive top hits</span>
          {manifest.thresholds.map((threshold) => (
            <span key={threshold.name}>{threshold.name.replace(/_/g, ' ')}: P &lt; {threshold.pvalue.toExponential(1)}</span>
          ))}
        </div>
      )}
    </section>
  );
}
