import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import type {
  AssociationPointV1,
  AssociationRegionOverlayV1,
  AssociationRegionRenderManifestV1,
} from '../contracts/types.js';
import { validateAssociationRegionPair } from '../contracts/parse.js';
import { Tooltip } from '../manhattan/Tooltip.js';
import { useHitDetection } from '../manhattan/useHitDetection.js';
import { YAxis } from '../manhattan/YAxis.js';
import { stableHitKey } from '../manhattan/layout.js';
import { GeneTrack } from './GeneTrack.js';
import {
  positionAbsentSelectedPoint,
  positionRegionOverlayPoint,
  validateRegionRasterDimensions,
  type LocusDisplayPoint,
} from './layout.js';

export interface AssociationLocusViewerProps {
  manifest: AssociationRegionRenderManifestV1;
  overlay: AssociationRegionOverlayV1;
  rasterUrl: string;
  /** `undefined` derives selection from selectedPoint; `null` explicitly clears it. */
  selectedSourceOrdinal?: number | null;
  /** Supplies full geometry when a controlled selection is absent from overlay.points. */
  selectedPoint?: AssociationPointV1 | null;
  showYAxis?: boolean;
  showStats?: boolean;
  className?: string;
  onPointSelect?: (point: AssociationPointV1) => void;
  onPointHover?: (point: AssociationPointV1 | null) => void;
  onResourceLoad?: () => void;
  onResourceError?: (error: Error) => void;
}

interface Dimensions { width: number; height: number }

function pointLabel(hit: LocusDisplayPoint): string {
  const alleles = hit.ref_allele && hit.alt_allele ? ` ${hit.ref_allele} to ${hit.alt_allele}` : '';
  const roles = [
    hit.significant ? 'significant' : null,
    hit.coding ? 'coding' : null,
    hit.absent_representative ? 'selected point outside ordinary representatives' : null,
  ].filter(Boolean).join(', ');
  return `Association point ${hit.contig}:${hit.position}${alleles}${roles ? `, ${roles}` : ''}`;
}

function controlledSelectionError(
  selectedOrdinal: number | null,
  selectedPoint: AssociationPointV1 | null | undefined,
  points: readonly LocusDisplayPoint[],
): Error | null {
  if (selectedOrdinal === null || selectedPoint == null) return null;
  if (selectedPoint.source_ordinal !== selectedOrdinal) {
    return new Error('selected point does not match selected source ordinal');
  }
  const retained = points.find((point) => point.source_ordinal === selectedOrdinal);
  if (retained && retained.key !== stableHitKey(selectedPoint)) {
    return new Error('selected point identity does not match the region overlay');
  }
  return null;
}

export function AssociationLocusViewer({
  manifest,
  overlay,
  rasterUrl,
  selectedSourceOrdinal,
  selectedPoint,
  showYAxis = true,
  showStats = true,
  className,
  onPointSelect,
  onPointHover,
  onResourceLoad,
  onResourceError,
}: AssociationLocusViewerProps) {
  const imageRef = useRef<HTMLImageElement>(null);
  const [dimensions, setDimensions] = useState<Dimensions>({ width: 0, height: 0 });
  const [imageState, setImageState] = useState<'loading' | 'loaded' | 'error'>('loading');
  const [hoveredPoint, setHoveredPoint] = useState<LocusDisplayPoint | null>(null);
  const [cursor, setCursor] = useState({ x: 0, y: 0 });

  const pairError = useMemo(() => {
    try {
      validateAssociationRegionPair(manifest, overlay);
      return null;
    } catch {
      return new Error('region manifest and overlay do not match');
    }
  }, [manifest, overlay]);

  const retainedPoints = useMemo(
    () => overlay.points.flatMap((entry) => {
      const point = positionRegionOverlayPoint(entry, manifest.layout);
      return point ? [point] : [];
    }),
    [manifest.layout, overlay.points],
  );
  const effectiveSelectedOrdinal = selectedSourceOrdinal === undefined
    ? selectedPoint?.source_ordinal ?? null
    : selectedSourceOrdinal;
  const selectionError = useMemo(
    () => controlledSelectionError(effectiveSelectedOrdinal, selectedPoint, retainedPoints),
    [effectiveSelectedOrdinal, retainedPoints, selectedPoint],
  );
  const displayedPoints = useMemo(() => {
    if (effectiveSelectedOrdinal === null
        || retainedPoints.some((point) => point.source_ordinal === effectiveSelectedOrdinal)
        || selectedPoint == null
        || selectedPoint.source_ordinal !== effectiveSelectedOrdinal) return retainedPoints;
    const absent = positionAbsentSelectedPoint(selectedPoint, manifest.layout);
    return absent ? [...retainedPoints, absent] : retainedPoints;
  }, [effectiveSelectedOrdinal, manifest.layout, retainedPoints, selectedPoint]);
  const inputError = pairError ?? selectionError;
  const hitIndex = useHitDetection(displayedPoints, dimensions.width, dimensions.height);

  useEffect(() => {
    if (inputError) onResourceError?.(inputError);
  }, [inputError, onResourceError]);

  useEffect(() => {
    setImageState('loading');
    setDimensions({ width: 0, height: 0 });
    setHoveredPoint(null);
  }, [rasterUrl, manifest.render_hash]);

  const updateDimensions = useCallback((image: HTMLImageElement) => {
    const rect = image.getBoundingClientRect();
    if (rect.width > 0 && rect.height > 0) {
      setDimensions({ width: rect.width, height: rect.height });
    } else if (image.clientWidth > 0) {
      setDimensions({
        width: image.clientWidth,
        height: image.clientWidth * manifest.layout.height / manifest.layout.width,
      });
    }
  }, [manifest.layout.height, manifest.layout.width]);

  const failImage = useCallback((error: Error) => {
    setImageState('error');
    setDimensions({ width: 0, height: 0 });
    onResourceError?.(error);
  }, [onResourceError]);

  const handleImageLoad = useCallback(() => {
    const image = imageRef.current;
    if (!image) return;
    try {
      validateRegionRasterDimensions(manifest.layout, image.naturalWidth, image.naturalHeight);
    } catch (error) {
      failImage(error instanceof Error ? error : new Error('invalid association locus raster'));
      return;
    }
    setImageState('loaded');
    updateDimensions(image);
    onResourceLoad?.();
  }, [failImage, manifest.layout, onResourceLoad, updateDimensions]);

  useEffect(() => {
    const image = imageRef.current;
    if (!image || inputError) return;
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
      else failImage(new Error('failed to load association locus raster'));
    }
    return () => observer?.disconnect();
  }, [failImage, handleImageLoad, inputError, rasterUrl]);

  const showPoint = useCallback((point: LocusDisplayPoint, x?: number, y?: number) => {
    setHoveredPoint(point);
    setCursor({
      x: x ?? point.x_normalized * dimensions.width,
      y: y ?? point.y_normalized * dimensions.height,
    });
    onPointHover?.(point);
  }, [dimensions.height, dimensions.width, onPointHover]);

  const clearHover = useCallback(() => {
    setHoveredPoint(null);
    onPointHover?.(null);
  }, [onPointHover]);

  const handlePointerMove = useCallback((event: React.MouseEvent<SVGSVGElement>) => {
    const rect = event.currentTarget.getBoundingClientRect();
    const cursorPoint = { x: event.clientX - rect.left, y: event.clientY - rect.top };
    const next = hitIndex.findHit(cursorPoint.x, cursorPoint.y);
    setCursor(cursorPoint);
    if (next?.key !== hoveredPoint?.key) {
      setHoveredPoint(next);
      onPointHover?.(next);
    }
  }, [hitIndex, hoveredPoint?.key, onPointHover]);

  const axisWidth = showYAxis ? 52 : 0;
  const classes = ['association-locus', className].filter(Boolean).join(' ');
  if (inputError) {
    return (
      <section className={classes} aria-label={manifest.title}>
        <div className="association-error" role="alert">Association locus data do not match.</div>
        <GeneTrack annotations={overlay.gene_annotations} layout={overlay.layout} />
      </section>
    );
  }

  return (
    <section className={classes} aria-label={manifest.title}>
      <div className="association-plot-row">
        {showYAxis && imageState === 'loaded' && dimensions.height > 0 && (
          <YAxis scale={manifest.layout.y_scale} renderedHeight={dimensions.height} width={axisWidth} />
        )}
        <div className="association-image-wrapper">
          <img
            ref={imageRef}
            className="association-raster association-locus-raster"
            src={rasterUrl}
            alt={`${manifest.title} locus plot`}
            width={manifest.layout.width}
            height={manifest.layout.height}
            draggable={false}
            onLoad={handleImageLoad}
            onError={() => failImage(new Error('failed to load association locus raster'))}
            style={{ aspectRatio: `${manifest.layout.width} / ${manifest.layout.height}` }}
          />
          {imageState === 'loaded' && dimensions.width > 0 && dimensions.height > 0 && (
            <svg
              className="association-overlay association-locus-overlay"
              width={dimensions.width}
              height={dimensions.height}
              viewBox={`0 0 ${manifest.layout.width} ${manifest.layout.height}`}
              preserveAspectRatio="none"
              aria-label={`Interactive association points for chromosome ${manifest.layout.contig}`}
              onMouseMove={handlePointerMove}
              onMouseLeave={clearHover}
            >
              {displayedPoints.map((point) => {
                const selected = point.source_ordinal === effectiveSelectedOrdinal;
                const hovered = point.key === hoveredPoint?.key;
                const roleClasses = [
                  'association-locus-point',
                  point.ordinary_representative ? 'is-ordinary' : '',
                  point.significant ? 'is-significant' : '',
                  point.coding ? 'is-coding' : '',
                  selected ? 'is-selected' : '',
                  hovered ? 'is-hovered' : '',
                  point.absent_representative ? 'is-absent-representative' : '',
                ].filter(Boolean).join(' ');
                return (
                  <g key={point.key}>
                    {selected && (
                      <circle
                        className={`association-locus-selection${point.absent_representative ? ' is-absent-representative' : ''}`}
                        cx={point.x}
                        cy={point.y}
                        r={9}
                      />
                    )}
                    <circle
                      role="button"
                      tabIndex={0}
                      aria-label={pointLabel(point)}
                      aria-pressed={selected}
                      className={roleClasses}
                      data-source-ordinal={point.source_ordinal}
                      cx={point.x}
                      cy={point.y}
                      r={hovered || selected ? 5 : 3.5}
                      onMouseEnter={() => showPoint(point)}
                      onFocus={() => showPoint(point)}
                      onBlur={clearHover}
                      onClick={(event) => { event.stopPropagation(); onPointSelect?.(point); }}
                      onKeyDown={(event) => {
                        if (event.key === 'Enter' || event.key === ' ') {
                          event.preventDefault();
                          onPointSelect?.(point);
                        }
                      }}
                    />
                  </g>
                );
              })}
            </svg>
          )}
          {hoveredPoint && imageState === 'loaded' && (
            <Tooltip hit={hoveredPoint} x={cursor.x} y={cursor.y} containerWidth={dimensions.width} />
          )}
        </div>
      </div>
      {imageState === 'loading' && <div className="association-loading" role="status">Loading association locus raster…</div>}
      {imageState === 'error' && <div className="association-error" role="alert">Association locus raster could not be loaded.</div>}
      <div className="association-locus-details" style={{ marginLeft: axisWidth }}>
        <div className="association-locus-range">
          Chromosome {manifest.layout.contig}:{manifest.layout.effective_start.toLocaleString()}–{manifest.layout.effective_end.toLocaleString()}
        </div>
        <GeneTrack annotations={overlay.gene_annotations} layout={manifest.layout} />
        {showStats && (
          <div className="association-stats">
            <span><strong>{manifest.counts.rasterized_point_count.toLocaleString()}</strong> rasterized points</span>
            <span><strong>{overlay.points.length.toLocaleString()}</strong> interactive representatives</span>
            {manifest.aggregation.omitted_ordinary_count > 0 && (
              <span>{manifest.aggregation.omitted_ordinary_count.toLocaleString()} ordinary points omitted from overlay</span>
            )}
          </div>
        )}
      </div>
    </section>
  );
}
