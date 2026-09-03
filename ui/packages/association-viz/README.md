# `@genohype/association-viz`

Manifest-driven React components for association rasters and bounded semantic top-hit overlays.

The public contract types in `src/contracts/types.ts` are generated from the checked-in Rust schema at `association/schema/association-visualization-v1.schema.json`. Run `npm run contracts:generate -w @genohype/association-viz` after an intentional schema change; package build, typecheck, and tests run `contracts:check` and fail if the generated binding drifts.

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
