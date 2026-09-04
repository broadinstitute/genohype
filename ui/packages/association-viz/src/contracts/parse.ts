import {
  canonicalContig,
  compareCanonicalContigs,
  verifyLayoutHash,
} from '../manhattan/layout.js';
import type {
  AssociationPageV1,
  AssociationPointV1,
  AssociationSortDirectionV1,
  AssociationTableSortV1,
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

function string(value: unknown, path: string, nonEmpty = false, maximumLength?: number): string {
  if (typeof value !== 'string' || (nonEmpty && !value.trim())) fail(`${path} must be a non-empty string`);
  if (maximumLength !== undefined && Array.from(value).length > maximumLength) {
    fail(`${path} cannot exceed ${maximumLength} characters`);
  }
  return value;
}

function schemaVersion(value: unknown, contract: string): string {
  const version = string(value, 'schema_version');
  const match = /^(\d+)\.(\d+)$/.exec(version);
  if (!match || match[1] !== String(Number(match[1]))) {
    fail(`malformed schema version ${JSON.stringify(version)}`);
  }
  if (Number(match[1]) !== 1) fail(`unsupported ${contract} schema major version ${match[1]}`);
  return version;
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

function optionalNullableString(
  value: unknown,
  path: string,
  maximumLength?: number,
): string | null | undefined {
  if (value === undefined || value === null) return value;
  return string(value, path, false, maximumLength);
}

function optionalNullableFinite(value: unknown, path: string): number | null | undefined {
  if (value === undefined || value === null) return value;
  return finite(value, path);
}

const MAX_POINT_FIELD_BYTES = 4_096;
const MAX_POINT_TEXT_BYTES = 16_384;
const MAX_PAGE_POINT_TEXT_BYTES = 1_048_576;
const utf8Encoder = new TextEncoder();

function utf8ByteLength(value: string): number {
  return utf8Encoder.encode(value).length;
}

function pointTextField(value: unknown, path: string, nonEmpty = false): string {
  const parsed = string(value, path, nonEmpty);
  if (utf8ByteLength(parsed) > MAX_POINT_FIELD_BYTES) {
    fail(`${path} cannot exceed ${MAX_POINT_FIELD_BYTES} UTF-8 bytes`);
  }
  return parsed;
}

function optionalNullablePointText(
  value: unknown,
  path: string,
): string | null | undefined {
  if (value === undefined || value === null) return value;
  return pointTextField(value, path);
}

function pointTextBytes(point: AssociationPointV1): number {
  return [
    point.contig,
    point.ref_allele,
    point.alt_allele,
    point.gene_symbol,
    point.consequence,
    point.series,
  ].reduce((total, value) => total + (value == null ? 0 : utf8ByteLength(value)), 0);
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

function parsePoint(value: unknown, path: string): AssociationPointV1 {
  const source = record(value, path);
  const contig = pointTextField(source.contig, `${path}.contig`, true);
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
  const point = {
    ...source,
    contig,
    position: integer(source.position, `${path}.position`, 1),
    pvalue,
    neg_log10_p: significance,
    source_ordinal: integer(source.source_ordinal, `${path}.source_ordinal`),
    ref_allele: optionalNullablePointText(source.ref_allele, `${path}.ref_allele`),
    alt_allele: optionalNullablePointText(source.alt_allele, `${path}.alt_allele`),
    beta: optionalNullableFinite(source.beta, `${path}.beta`),
    standard_error: optionalNullableFinite(source.standard_error, `${path}.standard_error`),
    allele_frequency: alleleFrequency,
    allele_count: alleleCount,
    gene_symbol: optionalNullablePointText(source.gene_symbol, `${path}.gene_symbol`),
    consequence: optionalNullablePointText(source.consequence, `${path}.consequence`),
    series: optionalNullablePointText(source.series, `${path}.series`),
  } as AssociationPointV1;
  if (pointTextBytes(point) > MAX_POINT_TEXT_BYTES) {
    fail(`${path} text cannot exceed ${MAX_POINT_TEXT_BYTES} UTF-8 bytes`);
  }
  return point;
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
  const parsedSchemaVersion = schemaVersion(source.schema_version, 'association manifest');

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
  const topHits = rawTopHits.map((point, index) => parsePoint(point, `top_hits[${index}]`));
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
    schema_version: parsedSchemaVersion,
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

/** Parse and fully validate a bounded V1 table or region page. */
export function parseAssociationPage(input: unknown): AssociationPageV1 {
  const source = record(input, 'page');
  const parsedSchemaVersion = schemaVersion(source.schema_version, 'association page');
  const datasetId = string(source.dataset_id, 'dataset_id');
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(datasetId)) {
    fail('dataset_id must be a canonical lowercase UUID-v4');
  }

  const rawPoints = array(source.points, 'points');
  if (rawPoints.length > 1_000) fail('points cannot contain more than 1000 rows');
  const offset = integer(source.offset, 'offset');
  const limit = integer(source.limit, 'limit', 1, 1_000);
  const total = integer(source.total, 'total');
  if (rawPoints.length > limit) fail('points cannot contain more rows than limit');
  if (rawPoints.length > 0) {
    const endOffset = offset + rawPoints.length;
    if (offset >= total || !Number.isSafeInteger(endOffset) || endOffset > total) {
      fail('nonempty page range must be within total');
    }
  }

  let pagePointTextBytes = 0;
  const points = rawPoints.map((point, index) => {
    const parsed = parsePoint(point, `points[${index}]`);
    pagePointTextBytes += pointTextBytes(parsed);
    if (pagePointTextBytes > MAX_PAGE_POINT_TEXT_BYTES) {
      fail(`page point text cannot exceed ${MAX_PAGE_POINT_TEXT_BYTES} UTF-8 bytes`);
    }
    return parsed;
  });

  let sort: AssociationTableSortV1 | null | undefined;
  if (source.sort === undefined || source.sort === null) sort = source.sort;
  else if (source.sort === 'pvalue' || source.sort === 'locus') sort = source.sort;
  else fail('sort must be pvalue, locus, null, or omitted');

  let direction: AssociationSortDirectionV1 | null | undefined;
  if (source.direction === undefined || source.direction === null) direction = source.direction;
  else if (source.direction === 'ascending' || source.direction === 'descending') direction = source.direction;
  else fail('direction must be ascending, descending, null, or omitted');

  let search: string | null | undefined;
  if (source.search === undefined || source.search === null) search = source.search;
  else {
    search = string(source.search, 'search', false, 256);
    if (utf8ByteLength(search) > 256) fail('search cannot exceed 256 UTF-8 bytes');
  }

  if ((sort != null) !== (direction != null)) {
    fail('sort and direction must either both be present or both be absent');
  }
  if (sort == null && search != null) fail('region pages cannot carry search');

  return {
    ...source,
    schema_version: parsedSchemaVersion,
    dataset_id: datasetId,
    points,
    offset,
    limit,
    total,
    sort,
    direction,
    search,
  } as AssociationPageV1;
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
