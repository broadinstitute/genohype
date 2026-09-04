import {
  canonicalAssembly,
  canonicalContig,
  compareCanonicalContigs,
  sha256,
  verifyLayoutHash,
} from '../manhattan/layout.js';
import type {
  AssociationPageV1,
  AssociationPointV1,
  AssociationRegionOverlayPointV1,
  AssociationRegionOverlayV1,
  AssociationRegionRenderManifestV1,
  GeneAnnotationsV1,
  GeneFeatureV1,
  RegionAggregationDisclosureV1,
  RegionLayoutV1,
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

function utf8String(value: unknown, path: string, nonEmpty = false, maximumBytes?: number): string {
  const parsed = string(value, path, nonEmpty);
  if (maximumBytes !== undefined && utf8ByteLength(parsed) > maximumBytes) {
    fail(`${path} cannot exceed ${maximumBytes} UTF-8 bytes`);
  }
  return parsed;
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

const REGION_DENSE_RULE_V1 = 'pixel_strata_v1';
const LOCUS_RENDERER_VERSION_V1 = 'locus_rasterizer_v1';
const LOCUS_STYLE_VERSION_V1 = 'locus_style_v1';
const MAX_REGION_OVERLAY_POINTS = 1_000;
const MAX_REGION_TEXT_BYTES = 1_048_576;
const MAX_GENE_TEXT_BYTES = 1_048_576;
const codingConsequences = new Set([
  'coding_sequence_variant', 'frameshift_variant', 'inframe_deletion', 'inframe_insertion',
  'incomplete_terminal_codon_variant', 'missense_variant', 'protein_altering_variant',
  'splice_acceptor_variant', 'splice_donor_variant', 'start_lost', 'start_retained_variant',
  'stop_gained', 'stop_lost', 'stop_retained_variant', 'synonymous_variant',
  'transcript_ablation', 'transcript_amplification',
]);

class IdentityBytes {
  readonly bytes: number[] = [];
  raw(value: string): void { this.bytes.push(...utf8Encoder.encode(value)); }
  u8(value: number): void { this.bytes.push(value); }
  u32(value: number): void {
    const data = new Uint8Array(4);
    new DataView(data.buffer).setUint32(0, value, false);
    this.bytes.push(...data);
  }
  u64(value: number): void {
    const data = new Uint8Array(8);
    new DataView(data.buffer).setBigUint64(0, BigInt(value), false);
    this.bytes.push(...data);
  }
  i64(value: number): void {
    const data = new Uint8Array(8);
    new DataView(data.buffer).setBigInt64(0, BigInt(value), false);
    this.bytes.push(...data);
  }
  f64(value: number): void {
    const data = new Uint8Array(8);
    new DataView(data.buffer).setFloat64(0, value, false);
    this.bytes.push(...data);
  }
  text(value: string): void {
    const encoded = utf8Encoder.encode(value);
    this.u64(encoded.length);
    this.bytes.push(...encoded);
  }
  hash(): string { return `sha256:${sha256(Uint8Array.from(this.bytes))}`; }
}

function parseRegionResource(value: unknown, path: string): ResourceDescriptorV1 {
  const parsed = parseResource(value, path);
  for (const [text, maximum, field] of [
    [parsed.url, 2_048, 'url'],
    [parsed.media_type, 255, 'media_type'],
    [parsed.checksum, 256, 'checksum'],
    [parsed.expires_at, 64, 'expires_at'],
  ] as const) {
    if (text != null && utf8ByteLength(text) > maximum) {
      fail(`${path}.${field} cannot exceed ${maximum} UTF-8 bytes`, 'resource');
    }
  }
  return parsed;
}

function digest(value: unknown, path: string): string {
  const parsed = string(value, path);
  if (!/^sha256:[0-9a-f]{64}$/.test(parsed)) fail(`${path} must be a lowercase SHA-256 digest`);
  return parsed;
}

function datasetId(value: unknown, path: string): string {
  const parsed = string(value, path);
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(parsed)) {
    fail(`${path} must be a canonical lowercase UUID-v4`);
  }
  return parsed;
}

function threshold(value: unknown, path: string): number {
  const parsed = finite(value, path);
  if (parsed <= 0 || parsed > 1) fail(`${path} must be in (0, 1]`);
  return parsed;
}

function bool(value: unknown, path: string): boolean {
  if (typeof value !== 'boolean') fail(`${path} must be boolean`);
  return value;
}

function parseRegionLayout(value: unknown, path: string): RegionLayoutV1 {
  const source = record(value, path);
  const contig = utf8String(source.contig, `${path}.contig`, true, 4_096);
  if (canonicalContig(contig) !== contig) fail(`${path}.contig must use canonical spelling`);
  const layout = {
    ...source,
    contig,
    contig_length: integer(source.contig_length, `${path}.contig_length`, 1),
    requested_start: integer(source.requested_start, `${path}.requested_start`, 1),
    requested_end: integer(source.requested_end, `${path}.requested_end`, 1),
    effective_start: integer(source.effective_start, `${path}.effective_start`, 1),
    effective_end: integer(source.effective_end, `${path}.effective_end`, 1),
    requested_width: integer(source.requested_width, `${path}.requested_width`, 50, 4_050),
    width: integer(source.width, `${path}.width`, 50, 4_050),
    height: integer(source.height, `${path}.height`, 64, 2_048),
    dpr: integer(source.dpr, `${path}.dpr`, 1, 4),
    physical_width: integer(source.physical_width, `${path}.physical_width`, 1, 16_384),
    physical_height: integer(source.physical_height, `${path}.physical_height`, 1, 16_384),
    y_scale: record(source.y_scale, `${path}.y_scale`),
  } as RegionLayoutV1;
  const scale = layout.y_scale as unknown as Record<string, unknown>;
  layout.y_scale = {
    ...scale,
    height: integer(scale.height, `${path}.y_scale.height`, 1, 16_384),
    log_threshold: finite(scale.log_threshold, `${path}.y_scale.log_threshold`),
    linear_fraction: finite(scale.linear_fraction, `${path}.y_scale.linear_fraction`),
    max_neg_log_p: finite(scale.max_neg_log_p, `${path}.y_scale.max_neg_log_p`),
  };
  const expectedStart = Number(((BigInt(layout.requested_start) - 1n) / 1_000n) * 1_000n + 1n);
  const roundedEnd = ((BigInt(layout.requested_end) + 999n) / 1_000n) * 1_000n;
  const expectedEnd = Number(roundedEnd < BigInt(layout.contig_length)
    ? roundedEnd : BigInt(layout.contig_length));
  const expectedWidth = Math.ceil(layout.requested_width / 50) * 50;
  if (layout.requested_end < layout.requested_start
      || layout.requested_end > layout.contig_length
      || layout.effective_start !== expectedStart
      || layout.effective_end !== expectedEnd
      || layout.width !== expectedWidth
      || layout.physical_width !== layout.width * layout.dpr
      || layout.physical_height !== layout.height * layout.dpr
      || layout.physical_width * layout.physical_height > 16_777_216
      || layout.y_scale.height !== layout.physical_height
      || layout.y_scale.log_threshold !== 10
      || layout.y_scale.linear_fraction !== 0.6
      || layout.y_scale.max_neg_log_p !== 350) {
    fail(`${path} is not canonical V1 region geometry`, 'layout');
  }
  return layout;
}

function regionLayoutHash(layout: RegionLayoutV1, assembly: string): string {
  const bytes = new IdentityBytes();
  bytes.raw('genohype.association.region-layout.v1\0');
  bytes.text(canonicalAssembly(assembly));
  bytes.text(layout.contig);
  for (const value of [layout.contig_length, layout.requested_start, layout.requested_end,
    layout.effective_start, layout.effective_end]) bytes.i64(value);
  bytes.u32(layout.requested_width); bytes.u32(layout.width); bytes.u32(layout.height);
  bytes.u8(layout.dpr); bytes.u32(layout.physical_width); bytes.u32(layout.physical_height);
  bytes.f64(layout.y_scale.log_threshold); bytes.f64(layout.y_scale.linear_fraction);
  bytes.f64(layout.y_scale.max_neg_log_p);
  return bytes.hash();
}

function regionRasterHash(manifest: AssociationRegionRenderManifestV1): string {
  const { layout } = manifest;
  const bytes = new IdentityBytes();
  bytes.raw('genohype.association.region-raster.v1\0');
  bytes.text(manifest.dataset_id); bytes.text(manifest.source_fingerprint); bytes.text(manifest.assembly);
  bytes.text(layout.contig);
  for (const value of [layout.contig_length, layout.effective_start, layout.effective_end]) bytes.i64(value);
  for (const value of [layout.width, layout.height, layout.physical_width, layout.physical_height]) bytes.u32(value);
  bytes.u8(layout.dpr); bytes.f64(layout.y_scale.log_threshold);
  bytes.f64(layout.y_scale.linear_fraction); bytes.f64(layout.y_scale.max_neg_log_p);
  bytes.f64(manifest.significance_threshold); bytes.text(LOCUS_RENDERER_VERSION_V1);
  bytes.text(LOCUS_STYLE_VERSION_V1); bytes.text(manifest.background_id);
  return bytes.hash();
}

function compareRegionPoints(left: AssociationPointV1, right: AssociationPointV1): number {
  const optional = (a: string | null | undefined, b: string | null | undefined): number => {
    if (a == null) return b == null ? 0 : -1;
    if (b == null) return 1;
    return compareUtf8Text(a, b);
  };
  return right.neg_log10_p - left.neg_log10_p
    || compareCanonicalContigs(left.contig, right.contig)
    || left.position - right.position
    || optional(left.ref_allele, right.ref_allele)
    || optional(left.alt_allele, right.alt_allele)
    || left.source_ordinal - right.source_ordinal;
}

function regionRenderHash(layoutHash: string, rasterHash: string): string {
  const bytes = new IdentityBytes();
  bytes.raw('genohype.association.region-render.v1\0');
  bytes.text(layoutHash); bytes.text(rasterHash);
  return bytes.hash();
}

function compareUtf8Text(left: string, right: string): number {
  const a = utf8Encoder.encode(left); const b = utf8Encoder.encode(right);
  for (let index = 0; index < Math.min(a.length, b.length); index += 1) {
    if (a[index] !== b[index]) return a[index] - b[index];
  }
  return a.length - b.length;
}

function parseGeneAnnotations(value: unknown, layout: RegionLayoutV1): GeneAnnotationsV1 {
  const source = record(value, 'gene_annotations');
  if (!['unavailable', 'available', 'error'].includes(source.state as string)) {
    fail('gene_annotations.state is invalid');
  }
  const state = source.state as GeneAnnotationsV1['state'];
  const message = source.message == null ? source.message as null | undefined
    : utf8String(source.message, 'gene_annotations.message', false, 1_024);
  const rawFeatures = array(source.features, 'gene_annotations.features');
  if (rawFeatures.length > 1_000 || (state !== 'available' && rawFeatures.length !== 0)) {
    fail('gene_annotations features are invalid');
  }
  let textBytes = message == null ? 0 : utf8ByteLength(message);
  const features = rawFeatures.map((value, index) => {
    const path = `gene_annotations.features[${index}]`;
    const feature = record(value, path);
    const contig = utf8String(feature.contig, `${path}.contig`, true, 4_096);
    const parsed = {
      ...feature,
      id: utf8String(feature.id, `${path}.id`, true, 4_096),
      symbol: feature.symbol == null ? feature.symbol as null | undefined
        : utf8String(feature.symbol, `${path}.symbol`, false, 4_096),
      contig,
      start: integer(feature.start, `${path}.start`, 1),
      end: integer(feature.end, `${path}.end`, 1),
      strand: feature.strand,
    } as GeneFeatureV1;
    if (!['plus', 'minus', 'unknown'].includes(parsed.strand)
        || canonicalContig(contig) !== contig || contig !== layout.contig
        || parsed.end < parsed.start || parsed.start < layout.effective_start
        || parsed.end > layout.effective_end) fail(`${path} is invalid`);
    textBytes += utf8ByteLength(parsed.id) + utf8ByteLength(parsed.symbol ?? '') + utf8ByteLength(contig);
    return parsed;
  });
  const strandRank = { plus: 0, minus: 1, unknown: 2 } as const;
  const compareGeneSymbol = (left: string | null | undefined, right: string | null | undefined): number => {
    if (left == null) return right == null ? 0 : -1;
    if (right == null) return 1;
    return compareUtf8Text(left, right);
  };
  const compareGenes = (left: GeneFeatureV1, right: GeneFeatureV1): number => left.start - right.start
    || left.end - right.end || compareUtf8Text(left.id, right.id)
    || compareGeneSymbol(left.symbol, right.symbol)
    || compareUtf8Text(left.contig, right.contig)
    || strandRank[left.strand] - strandRank[right.strand];
  if (textBytes > MAX_GENE_TEXT_BYTES
      || features.slice(1).some((feature, index) => compareGenes(features[index], feature) >= 0)) {
    fail('gene_annotations are not bounded and canonically ordered');
  }
  return { ...source, state, features, message } as GeneAnnotationsV1;
}

function regionOverlayHash(rasterHash: string, selected: number[], genes: GeneAnnotationsV1): string {
  const bytes = new IdentityBytes();
  bytes.raw('genohype.association.region-overlay.v1\0');
  bytes.text(rasterHash); bytes.text(REGION_DENSE_RULE_V1); bytes.u64(selected.length);
  selected.forEach((ordinal) => bytes.u64(ordinal));
  bytes.text(genes.state);
  if (genes.message == null) bytes.u8(0); else { bytes.u8(1); bytes.text(genes.message); }
  bytes.u64(genes.features.length);
  genes.features.forEach((feature) => {
    bytes.text(feature.id);
    if (feature.symbol == null) bytes.u8(0); else { bytes.u8(1); bytes.text(feature.symbol); }
    bytes.text(feature.contig); bytes.i64(feature.start); bytes.i64(feature.end); bytes.text(feature.strand);
  });
  return bytes.hash();
}

const disclosureFields = [
  'input_point_count', 'protected_selected_count', 'protected_significant_count',
  'protected_coding_count', 'protected_union_count', 'ordinary_input_count', 'ordinary_budget',
  'rendered_ordinary_count', 'omitted_ordinary_count', 'rasterized_point_count', 'overlay_point_count',
] as const;

function safeCountSum(path: string, ...values: number[]): number {
  const total = values.reduce((sum, value) => sum + value, 0);
  if (!Number.isSafeInteger(total)) fail(`${path} exceeds the safe integer range`);
  return total;
}

function parseDisclosure(value: unknown, path: string): RegionAggregationDisclosureV1 {
  const source = record(value, path);
  const parsed = { ...source, rule_id: string(source.rule_id, `${path}.rule_id`) } as unknown as RegionAggregationDisclosureV1;
  disclosureFields.forEach((field) => { parsed[field] = integer(source[field], `${path}.${field}`); });
  const categorySum = safeCountSum(
    `${path} protected category sum`,
    parsed.protected_selected_count,
    parsed.protected_significant_count,
    parsed.protected_coding_count,
  );
  const inputSum = safeCountSum(`${path} input accounting`, parsed.protected_union_count, parsed.ordinary_input_count);
  const ordinarySum = safeCountSum(`${path} ordinary accounting`, parsed.rendered_ordinary_count, parsed.omitted_ordinary_count);
  const overlaySum = safeCountSum(`${path} overlay accounting`, parsed.protected_union_count, parsed.rendered_ordinary_count);
  if (parsed.rule_id !== REGION_DENSE_RULE_V1
      || parsed.protected_selected_count > parsed.protected_union_count
      || parsed.protected_significant_count > parsed.protected_union_count
      || parsed.protected_coding_count > parsed.protected_union_count
      || parsed.protected_union_count > categorySum
      || parsed.protected_union_count > MAX_REGION_OVERLAY_POINTS
      || inputSum !== parsed.input_point_count
      || parsed.ordinary_budget !== MAX_REGION_OVERLAY_POINTS - parsed.protected_union_count
      || ordinarySum !== parsed.ordinary_input_count
      || parsed.rendered_ordinary_count > parsed.ordinary_budget
      || parsed.rasterized_point_count !== parsed.input_point_count
      || parsed.overlay_point_count !== overlaySum) {
    fail(`${path} counts are inconsistent`);
  }
  return parsed;
}

/** Parse and fully validate a V1 region render manifest. */
export function parseAssociationRegionRenderManifest(input: unknown): AssociationRegionRenderManifestV1 {
  const source = record(input, 'region manifest');
  const layout = parseRegionLayout(source.layout, 'layout');
  const assembly = utf8String(source.assembly, 'assembly', true, 64);
  if (canonicalAssembly(assembly) !== assembly) fail('assembly must use canonical spelling');
  const backgroundId = utf8String(source.background_id, 'background_id', true, 64);
  if (!['white', 'transparent'].includes(backgroundId) && !/^color:#[0-9A-F]{6}$/.test(backgroundId)) {
    fail('background_id is invalid');
  }
  const aggregation = parseDisclosure(source.aggregation, 'aggregation');
  const countsSource = record(source.counts, 'counts');
  const counts = {
    ...countsSource,
    source_point_count: integer(countsSource.source_point_count, 'counts.source_point_count'),
    rasterized_point_count: integer(countsSource.rasterized_point_count, 'counts.rasterized_point_count'),
    significant_point_count: integer(countsSource.significant_point_count, 'counts.significant_point_count'),
    coding_point_count: integer(countsSource.coding_point_count, 'counts.coding_point_count'),
    selected_point_count: integer(countsSource.selected_point_count, 'counts.selected_point_count'),
    overlay_point_count: integer(countsSource.overlay_point_count, 'counts.overlay_point_count'),
  };
  const resources = record(source.resources, 'resources');
  const parsed = {
    ...source,
    schema_version: schemaVersion(source.schema_version, 'association region manifest'),
    dataset_id: datasetId(source.dataset_id, 'dataset_id'),
    title: utf8String(source.title, 'title', true, 512),
    assembly,
    coordinate_convention: string(source.coordinate_convention, 'coordinate_convention'),
    source_fingerprint: utf8String(source.source_fingerprint, 'source_fingerprint', true, 128),
    layout,
    layout_hash: digest(source.layout_hash, 'layout_hash'),
    background_id: backgroundId,
    raster_hash: digest(source.raster_hash, 'raster_hash'),
    render_hash: digest(source.render_hash, 'render_hash'),
    overlay_hash: digest(source.overlay_hash, 'overlay_hash'),
    significance_threshold: threshold(source.significance_threshold, 'significance_threshold'),
    counts,
    aggregation,
    resources: {
      ...resources,
      raster: parseRegionResource(resources.raster, 'resources.raster'),
      overlay: parseRegionResource(resources.overlay, 'resources.overlay'),
    },
  } as AssociationRegionRenderManifestV1;
  if (parsed.coordinate_convention !== '1-based-inclusive') fail('unsupported coordinate convention');
  if (parsed.resources.raster.media_type !== 'image/png'
      || parsed.resources.overlay.media_type !== 'application/json') fail('invalid region resource media type', 'resource');
  if (parsed.layout_hash !== regionLayoutHash(layout, assembly)) fail('region layout hash mismatch', 'layout');
  if (parsed.raster_hash !== regionRasterHash(parsed)) fail('region raster hash mismatch', 'layout');
  if (parsed.render_hash !== regionRenderHash(parsed.layout_hash, parsed.raster_hash)) fail('region render hash mismatch', 'layout');
  if (counts.source_point_count !== aggregation.input_point_count
      || counts.rasterized_point_count !== aggregation.rasterized_point_count
      || counts.significant_point_count !== aggregation.protected_significant_count
      || counts.coding_point_count !== aggregation.protected_coding_count
      || counts.selected_point_count !== aggregation.protected_selected_count
      || counts.overlay_point_count !== aggregation.overlay_point_count) fail('manifest region counts are inconsistent');
  return parsed;
}

/** Parse and fully validate a V1 semantic region overlay. */
export function parseAssociationRegionOverlay(input: unknown): AssociationRegionOverlayV1 {
  const source = record(input, 'region overlay');
  const layout = parseRegionLayout(source.layout, 'layout');
  const significanceThreshold = threshold(source.significance_threshold, 'significance_threshold');
  const aggregation = parseDisclosure(source.aggregation, 'aggregation');
  const rawSelected = array(source.selected_ordinals, 'selected_ordinals');
  if (rawSelected.length > MAX_REGION_OVERLAY_POINTS) fail('selected_ordinals cannot exceed 1000');
  const selectedOrdinals = rawSelected.map((value, index) => integer(value, `selected_ordinals[${index}]`));
  if (selectedOrdinals.slice(1).some((value, index) => selectedOrdinals[index] >= value)) {
    fail('selected_ordinals must be sorted and unique');
  }
  const selectedSet = new Set(selectedOrdinals);
  const rawPoints = array(source.points, 'points');
  if (rawPoints.length > MAX_REGION_OVERLAY_POINTS) fail('points cannot exceed 1000');
  let overlayTextBytes = 0;
  const seenOrdinals = new Set<number>();
  let priorPoint: AssociationPointV1 | undefined;
  const tallies = { selected: 0, significant: 0, coding: 0, union: 0, ordinary: 0 };
  const points = rawPoints.map((value, index) => {
    const path = `points[${index}]`; const entry = record(value, path);
    const point = parsePoint(entry.point, `${path}.point`);
    const selected = bool(entry.selected, `${path}.selected`);
    const significant = bool(entry.significant, `${path}.significant`);
    const coding = bool(entry.coding, `${path}.coding`);
    const ordinary = bool(entry.ordinary_representative, `${path}.ordinary_representative`);
    const protectedPoint = selected || significant || coding;
    if (seenOrdinals.has(point.source_ordinal)
        || (priorPoint !== undefined && compareRegionPoints(priorPoint, point) > 0)
        || point.contig !== layout.contig || point.position < layout.effective_start
        || point.position > layout.effective_end || selected !== selectedSet.has(point.source_ordinal)
        || significant !== (point.pvalue === 0 || point.pvalue < significanceThreshold)
        || coding !== codingConsequences.has((point.consequence ?? '').trim())
        || ordinary === protectedPoint) fail(`${path} is not a valid canonical overlay point`);
    seenOrdinals.add(point.source_ordinal); priorPoint = point; overlayTextBytes += pointTextBytes(point);
    tallies.selected += Number(selected); tallies.significant += Number(significant);
    tallies.coding += Number(coding); tallies.union += Number(protectedPoint); tallies.ordinary += Number(ordinary);
    return { ...entry, point, selected, significant, coding, ordinary_representative: ordinary } as AssociationRegionOverlayPointV1;
  });
  const genes = parseGeneAnnotations(source.gene_annotations, layout);
  const parsed = {
    ...source,
    schema_version: schemaVersion(source.schema_version, 'association region overlay'),
    layout_hash: digest(source.layout_hash, 'layout_hash'),
    raster_hash: digest(source.raster_hash, 'raster_hash'),
    render_hash: digest(source.render_hash, 'render_hash'),
    overlay_hash: digest(source.overlay_hash, 'overlay_hash'),
    selected_ordinals: selectedOrdinals,
    layout,
    significance_threshold: significanceThreshold,
    points,
    gene_annotations: genes,
    aggregation,
  } as AssociationRegionOverlayV1;
  const ordinaryInput = aggregation.input_point_count - tallies.union;
  const expected = [aggregation.protected_selected_count === tallies.selected,
    aggregation.protected_significant_count === tallies.significant,
    aggregation.protected_coding_count === tallies.coding,
    aggregation.protected_union_count === tallies.union,
    aggregation.ordinary_input_count === ordinaryInput,
    aggregation.ordinary_budget === MAX_REGION_OVERLAY_POINTS - tallies.union,
    aggregation.rendered_ordinary_count === tallies.ordinary,
    aggregation.omitted_ordinary_count === ordinaryInput - tallies.ordinary,
    aggregation.rasterized_point_count === aggregation.input_point_count,
    aggregation.overlay_point_count === points.length];
  if (overlayTextBytes > MAX_REGION_TEXT_BYTES || expected.some((valid) => !valid)) {
    fail('overlay disclosure does not match payload flags and points');
  }
  if (parsed.render_hash !== regionRenderHash(parsed.layout_hash, parsed.raster_hash)) fail('region render hash mismatch', 'layout');
  if (parsed.overlay_hash !== regionOverlayHash(parsed.raster_hash, selectedOrdinals, genes)) fail('region overlay hash mismatch', 'layout');
  return parsed;
}

/** Parse both contracts and require their exact V1 render/overlay agreement. */
export function validateAssociationRegionPair(
  manifestInput: unknown,
  overlayInput: unknown,
): { manifest: AssociationRegionRenderManifestV1; overlay: AssociationRegionOverlayV1 } {
  const manifest = parseAssociationRegionRenderManifest(manifestInput);
  const overlay = parseAssociationRegionOverlay(overlayInput);
  const layoutFields = ['contig', 'contig_length', 'requested_start', 'requested_end', 'effective_start',
    'effective_end', 'requested_width', 'width', 'height', 'dpr', 'physical_width', 'physical_height'] as const;
  const sameLayout = layoutFields.every((field) => manifest.layout[field] === overlay.layout[field])
    && manifest.layout.y_scale.height === overlay.layout.y_scale.height
    && manifest.layout.y_scale.log_threshold === overlay.layout.y_scale.log_threshold
    && manifest.layout.y_scale.linear_fraction === overlay.layout.y_scale.linear_fraction
    && manifest.layout.y_scale.max_neg_log_p === overlay.layout.y_scale.max_neg_log_p;
  const sameDisclosure = disclosureFields.every((field) => manifest.aggregation[field] === overlay.aggregation[field])
    && manifest.aggregation.rule_id === overlay.aggregation.rule_id;
  if (!sameLayout || manifest.significance_threshold !== overlay.significance_threshold
      || manifest.layout_hash !== overlay.layout_hash || manifest.raster_hash !== overlay.raster_hash
      || manifest.render_hash !== overlay.render_hash || manifest.overlay_hash !== overlay.overlay_hash
      || !sameDisclosure) fail('region manifest and overlay do not agree', 'layout');
  return { manifest, overlay };
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
