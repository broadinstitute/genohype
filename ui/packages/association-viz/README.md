# `@genohype/association-viz`

Manifest-driven React components for association rasters and bounded semantic top-hit overlays.

```tsx
import {
  AssociationHitTable,
  AssociationManhattanViewer,
  parseAssociationVisualizationManifest,
  resolveAssociationResourceUrl,
} from '@genohype/association-viz';
import '@genohype/association-viz/styles.css';
```

Parse untrusted manifest JSON before rendering. Resolve each resource against the final fetched manifest URL. Selection is controlled with `selectedHitKey` (from `stableHitKey`) and the plot/table callbacks; locus and chromosome actions are callbacks, never navigation. The package renders only `manifest.top_hits` as SVG semantics over the dense raster.

The default theme is standalone. Consumers may override the `--association-*` variables, including the opt-in `--association-raster-filter`.
