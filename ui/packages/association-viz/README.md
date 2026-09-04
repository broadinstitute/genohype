# `@genohype/association-viz`

Manifest-driven React components for association rasters and bounded semantic top-hit overlays.

The public contract types in `src/contracts/types.ts` are generated from the checked-in Rust schemas at `association/schema/association-visualization-v1.schema.json` and `association/schema/association-page-v1.schema.json`. Run `npm run contracts:generate -w @genohype/association-viz` after an intentional schema change; package build, typecheck, and tests run `contracts:check` and fail if either generated binding drifts.

```tsx
import {
  AssociationHitTable,
  AssociationManhattanViewer,
  parseAssociationVisualizationManifest,
  resolveAssociationResourceUrl,
} from '@genohype/association-viz';
import '@genohype/association-viz/styles.css';
```

Parse untrusted manifest JSON before rendering. Resolve each resource against the final fetched manifest URL. Parse table and region API responses with `parseAssociationPage`; it rejects unsupported major versions and unsafe or out-of-contract values while retaining compatible V1 minor fields. Selection is controlled with `selectedHitKey` (from `stableHitKey`) and the plot/table callbacks; locus and chromosome actions are callbacks, never navigation. The package renders only `manifest.top_hits` as SVG semantics over the dense raster.

`AssociationHitTable` keeps its original local API (`hits`) and client-side search/sort. For a fetched page, use the discriminated controlled mode:

```tsx
<AssociationHitTable
  mode="server"
  rows={page.points}
  total={page.total}
  offset={page.offset}
  limit={page.limit}
  query={query}
  sort={sort}
  direction={direction}
  loading={loading}
  error={error}
  onQueryChange={setQuery}
  onSortChange={(nextSort, nextDirection) => {
    setSort(nextSort);
    setDirection(nextDirection);
  }}
  onOffsetChange={setOffset}
/>
```

Server mode renders the supplied rows in server order and delegates all paging, query, and sort changes. Query and sort actions also request offset zero through `onOffsetChange`.

The default theme is standalone. Consumers may override the `--association-*` variables, including the opt-in `--association-raster-filter`.
