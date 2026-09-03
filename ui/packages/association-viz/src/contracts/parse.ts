import {
  canonicalContig,
  compareCanonicalContigs,
  verifyLayoutHash,
} from '../manhattan/layout.js';
import type {
  AssociationPointV1,
  AssociationThresholdV1,
  AssociationVisualizationManifestV1,
  GenomeLayoutV1,
  ResourceDescriptorV1,
} from './types.js';

export type ManifestValidationCode = 'schema' | 'resource' | 'layout';

export class ManifestValidationError extends Error {
  readonly code: ManifestValidationCode;

  constructor(code: ManifestValidationCode, message: string) {
    super(message);
    this.name = 'ManifestValidationError';
    this.code = code;
  }
}

function fail(message: string, code: ManifestValidationCode = 'schema'): never {
  throw new ManifestValidationError(code, message);
}

function record(value: unknown, path: string): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) fail(`${path} must be an object`);
  return value as Record<string, unknown>;
}

function string(value: unknown, path: string, nonEmpty = false): string {
  if (typeof value !== 'string' || (nonEmpty && !value.trim())) fail(`${path} must be a non-empty string`);
  return value;
}

function finite(value: unknown, path: string): number {
  if (typeof value !== 'number' || !Number.isFinite(value)) fail(`${path} must be a finite number`);
  return value;
}

function integer(
  value: unknown,
  path: string,
  minimum = 0,
  maximum = Number.MAX_SAFE_INTEGER,
): number {
  const number = finite(value, path);
  if (!Number.isSafeInteger(number) || number < minimum || number > maximum) {
    fail(`${path} must be a safe integer in ${minimum}..=${maximum}`);
  }
  return number;
}

function array(value: unknown, path: string): unknown[] {
  if (!Array.isArray(value)) fail(`${path} must be an array`);
  return value;
}

function optionalNullableString(value: unknown, path: string): string | null | undefined {
  if (value === undefined || value === null) return value;
  return string(value, path);
}

function optionalNullableFinite(value: unknown, path: string): number | null | undefined {
  if (value === undefined || value === null) return value;
  return finite(value, path);
}

function parseResourceValue(value: unknown, path: string): ResourceDescriptorV1 {
  const source = record(value, path);
  const url = string(source.url, `${path}.url`);
  const firstComponent = url.split('/')[0] ?? '';
  const invalid = !url
    || url !== url.trim()
    || url.startsWith('/')
    || url.startsWith('\\')
    || /[?:#@%\\]/.test(url)
    || firstComponent.includes(':')
    || url.split('/').some((component) => !component || component === '.' || component === '..')
    || /[\u0000-\u001f\u007f]/.test(url);
  if (invalid) fail(`${path}.url must be safe, credential-free, and relative`, 'resource');
  const mediaType = string(source.media_type, `${path}.media_type`, true);
  const byteSize = source.byte_size === undefined || source.byte_size === null
    ? source.byte_size
    : integer(source.byte_size, `${path}.byte_size`);
  return {
    ...source,
    url,
    media_type: mediaType,
    byte_size: byteSize,
    checksum: optionalNullableString(source.checksum, `${path}.checksum`),
    expires_at: optionalNullableString(source.expires_at, `${path}.expires_at`),
  } as ResourceDescriptorV1;
}

function parseResource(value: unknown, path: string): ResourceDescriptorV1 {
  try {
    return parseResourceValue(value, path);
  } catch (error) {
    if (error instanceof ManifestValidationError) throw new ManifestValidationError('resource', error.message);
    throw error;
  }
}

function parseLayout(value: unknown): GenomeLayoutV1 {
  const source = record(value, 'layout');
  const width = integer(source.width, 'layout.width', 1, 0xffff_ffff);
  const height = integer(source.height, 'layout.height', 1, 0xffff_ffff);
  const gap = integer(source.contig_gap, 'layout.contig_gap', 0, 0xffff_ffff);
  const contigs = array(source.contigs, 'layout.contigs').map((value, index) => {
    const contig = record(value, `layout.contigs[${index}]`);
    const xStart = finite(contig.x_start, `layout.contigs[${index}].x_start`);
    const xEnd = finite(contig.x_end, `layout.contigs[${index}].x_end`);
    if (xStart < 0 || xEnd < 0) fail(`layout.contigs[${index}] extents must be non-negative`);
    const color = string(contig.color, `layout.contigs[${index}].color`);
    if (!/^#[0-9a-f]{6}$/i.test(color)) fail(`layout.contigs[${index}].color must be #RRGGBB`);
    return {
      ...contig,
      name: string(contig.name, `layout.contigs[${index}].name`, true),
      length: integer(contig.length, `layout.contigs[${index}].length`, 1),
      x_start: xStart,
      x_end: xEnd,
      color,
    };
  });
  const scale = record(source.y_scale, 'layout.y_scale');
  return {
    ...source,
    width,
    height,
    contig_gap: gap,
    contigs,
    y_scale: {
      ...scale,
      height: integer(scale.height, 'layout.y_scale.height', 1, 0xffff_ffff),
      log_threshold: finite(scale.log_threshold, 'layout.y_scale.log_threshold'),
      linear_fraction: finite(scale.linear_fraction, 'layout.y_scale.linear_fraction'),
      max_neg_log_p: finite(scale.max_neg_log_p, 'layout.y_scale.max_neg_log_p'),
    },
  } as GenomeLayoutV1;
}

function parsePoint(value: unknown, index: number): AssociationPointV1 {
  const path = `top_hits[${index}]`;
  const source = record(value, path);
  const contig = string(source.contig, `${path}.contig`, true);
  if (canonicalContig(contig) !== contig) fail(`${path}.contig must use canonical spelling`);
  const pvalue = finite(source.pvalue, `${path}.pvalue`);
  const significance = finite(source.neg_log10_p, `${path}.neg_log10_p`);
  if (pvalue < 0 || pvalue > 1) fail(`${path}.pvalue must be in 0..=1`);
  if (significance < 0) fail(`${path}.neg_log10_p must be non-negative`);
  const alleleFrequency = optionalNullableFinite(source.allele_frequency, `${path}.allele_frequency`);
  if (alleleFrequency != null && (alleleFrequency < 0 || alleleFrequency > 1)) {
    fail(`${path}.allele_frequency must be in 0..=1`);
  }
  const alleleCount = optionalNullableFinite(source.allele_count, `${path}.allele_count`);
  if (alleleCount != null && alleleCount < 0) fail(`${path}.allele_count must be non-negative`);
  return {
    ...source,
    contig,
    position: integer(source.position, `${path}.position`, 1),
    pvalue,
    neg_log10_p: significance,
    source_ordinal: integer(source.source_ordinal, `${path}.source_ordinal`),
    ref_allele: optionalNullableString(source.ref_allele, `${path}.ref_allele`),
    alt_allele: optionalNullableString(source.alt_allele, `${path}.alt_allele`),
    beta: optionalNullableFinite(source.beta, `${path}.beta`),
    standard_error: optionalNullableFinite(source.standard_error, `${path}.standard_error`),
    allele_frequency: alleleFrequency,
    allele_count: alleleCount,
    gene_symbol: optionalNullableString(source.gene_symbol, `${path}.gene_symbol`),
    consequence: optionalNullableString(source.consequence, `${path}.consequence`),
    series: optionalNullableString(source.series, `${path}.series`),
  } as AssociationPointV1;
}

function compareOptionalString(left: string | null | undefined, right: string | null | undefined): number {
  if (left == null) return right == null ? 0 : -1;
  if (right == null) return 1;
  return left < right ? -1 : left > right ? 1 : 0;
}

function comparePoints(left: AssociationPointV1, right: AssociationPointV1): number {
  return right.neg_log10_p - left.neg_log10_p
    || compareCanonicalContigs(left.contig, right.contig)
    || left.position - right.position
    || compareOptionalString(left.ref_allele, right.ref_allele)
    || compareOptionalString(left.alt_allele, right.alt_allele)
    || left.source_ordinal - right.source_ordinal;
}

function parseThreshold(value: unknown, index: number): AssociationThresholdV1 {
  const source = record(value, `thresholds[${index}]`);
  const pvalue = finite(source.pvalue, `thresholds[${index}].pvalue`);
  if (pvalue <= 0 || pvalue > 1) fail(`thresholds[${index}].pvalue must be in (0, 1]`);
  return { ...source, name: string(source.name, `thresholds[${index}].name`, true), pvalue } as AssociationThresholdV1;
}

/** Parse and fully validate a V1 manifest while tolerating unknown V1 minor fields. */
export function parseAssociationVisualizationManifest(input: unknown): AssociationVisualizationManifestV1 {
  const source = record(input, 'manifest');
  const schemaVersion = string(source.schema_version, 'schema_version');
  const versionMatch = /^(\d+)\.(\d+)$/.exec(schemaVersion);
  if (!versionMatch || versionMatch[1] !== String(Number(versionMatch[1]))) {
    fail(`malformed schema version ${JSON.stringify(schemaVersion)}`);
  }
  if (Number(versionMatch[1]) !== 1) fail(`unsupported association manifest schema major version ${versionMatch[1]}`);

  const kind = string(source.kind, 'kind');
  if (kind !== 'manhattan' && kind !== 'locus') fail('kind must be manhattan or locus');
  const coordinateConvention = string(source.coordinate_convention, 'coordinate_convention');
  if (coordinateConvention !== '1-based-inclusive') fail(`unsupported coordinate convention ${coordinateConvention}`);
  const assembly = string(source.assembly, 'assembly', true);
  const layout = parseLayout(source.layout);
  const layoutHash = string(source.layout_hash, 'layout_hash', true);
  try {
    verifyLayoutHash(layout, assembly, layoutHash);
  } catch (error) {
    fail(error instanceof Error ? error.message : 'invalid layout', 'layout');
  }

  const rawTopHits = array(source.top_hits, 'top_hits');
  if (rawTopHits.length > 1_000) fail('top_hits cannot contain more than 1000 points');
  const topHits = rawTopHits.map(parsePoint);
  if (!topHits.slice(1).every((point, index) => comparePoints(topHits[index], point) <= 0)) {
    fail('top_hits are not in canonical order');
  }
  const counts = {
    row_count: integer(source.row_count, 'row_count'),
    valid_row_count: integer(source.valid_row_count, 'valid_row_count'),
    skipped_row_count: integer(source.skipped_row_count, 'skipped_row_count'),
    rendered_point_count: integer(source.rendered_point_count, 'rendered_point_count'),
    significant_hit_count: integer(source.significant_hit_count, 'significant_hit_count'),
  };
  if (
    counts.valid_row_count + counts.skipped_row_count !== counts.row_count
    || counts.rendered_point_count > counts.valid_row_count
    || counts.significant_hit_count > counts.valid_row_count
    || topHits.length > counts.valid_row_count
  ) fail('manifest row counts are inconsistent');

  const resources = record(source.resources, 'resources');
  const warnings = array(source.warnings, 'warnings').map((value, index) => string(value, `warnings[${index}]`));
  return {
    ...source,
    schema_version: schemaVersion,
    plot_id: string(source.plot_id, 'plot_id', true),
    kind,
    title: string(source.title, 'title', true),
    assembly,
    coordinate_convention: coordinateConvention,
    source_fingerprint: string(source.source_fingerprint, 'source_fingerprint', true),
    genohype_version: string(source.genohype_version, 'genohype_version', true),
    created_at: string(source.created_at, 'created_at', true),
    layout,
    layout_hash: layoutHash,
    thresholds: array(source.thresholds, 'thresholds').map(parseThreshold),
    ...counts,
    top_hits: topHits,
    warnings,
    resources: {
      ...resources,
      raster: parseResource(resources.raster, 'resources.raster'),
      region_api: resources.region_api == null ? resources.region_api as null | undefined : parseResource(resources.region_api, 'resources.region_api'),
      table_api: resources.table_api == null ? resources.table_api as null | undefined : parseResource(resources.table_api, 'resources.table_api'),
    },
  } as AssociationVisualizationManifestV1;
}

/** Resolve a previously validated relative resource against the fetched manifest URL. */
export function resolveAssociationResourceUrl(
  resource: ResourceDescriptorV1,
  manifestUrl: string | URL,
): URL {
  // Re-validate at the trust boundary in case a caller did not obtain the descriptor from the parser.
  const validated = parseResource(resource, 'resource');
  let base: URL;
  try {
    base = new URL(manifestUrl);
  } catch {
    fail('manifest URL must be absolute before resources can be resolved', 'resource');
  }
  const resolved = new URL(validated.url, base);
  if (resolved.origin !== base.origin) fail('resource escaped the manifest origin', 'resource');
  return resolved;
}
