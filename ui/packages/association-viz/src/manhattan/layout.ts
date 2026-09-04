import type {
  AssociationPointV1,
  ContigLayoutV1,
  GenomeLayoutV1,
  YScaleV1,
} from '../contracts/types.js';

export interface DisplayHit extends AssociationPointV1 {
  key: string;
  x_normalized: number;
  y_normalized: number;
}

export function canonicalAssembly(value: string): string {
  const trimmed = value.trim();
  switch (trimmed.toLowerCase()) {
    case 'grch37':
    case 'hg19':
    case 'b37':
      return 'GRCh37';
    case 'grch38':
    case 'hg38':
    case 'b38':
      return 'GRCh38';
    default:
      return trimmed;
  }
}

export function canonicalContig(value: string): string | null {
  const trimmed = value.trim();
  if (!trimmed) return null;
  const bare = trimmed.slice(0, 3).toLowerCase() === 'chr' ? trimmed.slice(3) : trimmed;
  if (!bare) return null;
  if (/^\d+$/.test(bare)) {
    const number = Number(bare);
    if (Number.isInteger(number) && number >= 1 && number <= 22) return String(number);
  }
  switch (bare.toUpperCase()) {
    case 'X': return 'X';
    case 'Y': return 'Y';
    case 'M':
    case 'MT': return 'MT';
    default: return bare;
  }
}

function canonicalRank(name: string): number {
  const number = Number(name);
  if (Number.isInteger(number) && number >= 1 && number <= 22) return number;
  if (name === 'X') return 23;
  if (name === 'Y') return 24;
  if (name === 'MT') return 25;
  return Number.MAX_SAFE_INTEGER;
}

function compareUtf8(left: string, right: string): number {
  const encoder = new TextEncoder();
  const a = encoder.encode(left);
  const b = encoder.encode(right);
  for (let i = 0; i < Math.min(a.length, b.length); i += 1) {
    if (a[i] !== b[i]) return a[i] - b[i];
  }
  return a.length - b.length;
}

export function compareCanonicalContigs(left: string, right: string): number {
  const a = canonicalContig(left) ?? '';
  const b = canonicalContig(right) ?? '';
  return canonicalRank(a) - canonicalRank(b) || compareUtf8(a, b);
}

export function stableHitKey(hit: AssociationPointV1): string {
  return [
    canonicalContig(hit.contig) ?? hit.contig,
    hit.position,
    hit.ref_allele ?? '',
    hit.alt_allele ?? '',
    hit.source_ordinal,
  ].join(':');
}

export function xForPosition(
  layout: GenomeLayoutV1,
  contig: string,
  position: number,
): number | null {
  const name = canonicalContig(contig);
  const entry = name && layout.contigs.find((candidate) => candidate.name === name);
  if (!entry || !Number.isSafeInteger(position) || position < 1 || position > entry.length) return null;
  const rasterX = entry.x_start + ((position - 1) / entry.length) * (entry.x_end - entry.x_start);
  return rasterX / layout.width;
}

export function contigAtNormalizedX(layout: GenomeLayoutV1, normalizedX: number): string | null {
  if (!Number.isFinite(normalizedX)) return null;
  const rasterX = normalizedX * layout.width;
  return layout.contigs.find((contig) => rasterX >= contig.x_start && rasterX <= contig.x_end)?.name ?? null;
}

export function yForNegLog10P(scale: YScaleV1, value: number): number {
  const significance = Number.isNaN(value)
    ? 0
    : Math.min(Math.max(value, 0), scale.max_neg_log_p);
  const linearHeight = scale.height * scale.linear_fraction;
  let rasterY: number;
  if (significance <= scale.log_threshold) {
    rasterY = scale.height - (significance / scale.log_threshold) * linearHeight;
  } else {
    const logHeight = scale.height * (1 - scale.linear_fraction);
    const normalized = Math.log(significance / scale.log_threshold)
      / Math.log(scale.max_neg_log_p / scale.log_threshold);
    rasterY = Math.max(scale.height - linearHeight - normalized * logHeight, 0);
  }
  return rasterY / scale.height;
}

export function computeDisplayHits(
  hits: readonly AssociationPointV1[],
  layout: GenomeLayoutV1,
): DisplayHit[] {
  const result: DisplayHit[] = [];
  for (const hit of hits) {
    const x = xForPosition(layout, hit.contig, hit.position);
    if (x === null) continue;
    result.push({
      ...hit,
      key: stableHitKey(hit),
      x_normalized: x,
      y_normalized: yForNegLog10P(layout.y_scale, hit.neg_log10_p),
    });
  }
  return result;
}

export function deriveCanonicalContigs(layout: GenomeLayoutV1): ContigLayoutV1[] {
  const names = new Set<string>();
  const contigs = layout.contigs.map((contig) => {
    const name = canonicalContig(contig.name);
    if (!name || names.has(name)) throw new Error(`invalid or duplicate canonical contig ${contig.name}`);
    names.add(name);
    return { name, length: contig.length };
  }).sort((a, b) => compareCanonicalContigs(a.name, b.name));

  const totalGap = layout.contig_gap * Math.max(0, contigs.length - 1);
  if (!contigs.length || totalGap >= layout.width) throw new Error('contig gaps consume the layout width');
  const totalBases = contigs.reduce((sum, contig) => sum + contig.length, 0);
  const pixelsPerBase = (layout.width - totalGap) / totalBases;
  let cursor = 0;
  return contigs.map((contig, index) => {
    const x_start = cursor;
    const x_end = x_start + contig.length * pixelsPerBase;
    cursor = x_end + layout.contig_gap;
    return {
      ...contig,
      x_start,
      x_end,
      color: index % 2 === 0 ? '#404040' : '#4682B4',
    };
  });
}

export function verifyCanonicalGeometry(layout: GenomeLayoutV1): void {
  if (
    layout.y_scale.height !== layout.height
    || layout.y_scale.log_threshold !== 10
    || layout.y_scale.linear_fraction !== 0.6
    || layout.y_scale.max_neg_log_p !== 350
  ) {
    throw new Error('layout must use the canonical V1 y scale');
  }
  const expected = deriveCanonicalContigs(layout);
  if (expected.length !== layout.contigs.length) throw new Error('non-canonical layout geometry');
  // A shortest-round-trip JSON coordinate can land one ULP from the value
  // independently re-derived by JavaScript. Match Rust's narrow, width-scaled
  // allowance; all hashed geometry inputs and non-coordinate fields stay exact.
  const tolerance = Number.EPSILON * Math.max(layout.width, 1) * 8;
  expected.forEach((contig, index) => {
    const actual = layout.contigs[index];
    if (
      actual.name !== contig.name
      || actual.length !== contig.length
      || !(Math.abs(actual.x_start - contig.x_start) <= tolerance)
      || !(Math.abs(actual.x_end - contig.x_end) <= tolerance)
      || actual.color !== contig.color
    ) throw new Error('non-canonical layout geometry');
  });
}

function rotateRight(value: number, shift: number): number {
  return (value >>> shift) | (value << (32 - shift));
}

/** Small synchronous SHA-256 used so manifest validation is deterministic in browsers and Node. */
export function sha256(bytes: Uint8Array): string {
  const constants = new Uint32Array([
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
  ]);
  const hash = new Uint32Array([0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19]);
  const bitLength = BigInt(bytes.length) * 8n;
  const paddedLength = Math.ceil((bytes.length + 9) / 64) * 64;
  const padded = new Uint8Array(paddedLength);
  padded.set(bytes);
  padded[bytes.length] = 0x80;
  new DataView(padded.buffer).setBigUint64(paddedLength - 8, bitLength, false);
  const words = new Uint32Array(64);
  const view = new DataView(padded.buffer);
  for (let offset = 0; offset < paddedLength; offset += 64) {
    for (let index = 0; index < 16; index += 1) words[index] = view.getUint32(offset + index * 4, false);
    for (let index = 16; index < 64; index += 1) {
      const x = words[index - 15];
      const y = words[index - 2];
      const s0 = rotateRight(x, 7) ^ rotateRight(x, 18) ^ (x >>> 3);
      const s1 = rotateRight(y, 17) ^ rotateRight(y, 19) ^ (y >>> 10);
      words[index] = (words[index - 16] + s0 + words[index - 7] + s1) >>> 0;
    }
    let [a, b, c, d, e, f, g, h] = hash;
    for (let index = 0; index < 64; index += 1) {
      const s1 = rotateRight(e, 6) ^ rotateRight(e, 11) ^ rotateRight(e, 25);
      const choice = (e & f) ^ (~e & g);
      const t1 = (h + s1 + choice + constants[index] + words[index]) >>> 0;
      const s0 = rotateRight(a, 2) ^ rotateRight(a, 13) ^ rotateRight(a, 22);
      const majority = (a & b) ^ (a & c) ^ (b & c);
      const t2 = (s0 + majority) >>> 0;
      h = g; g = f; f = e; e = (d + t1) >>> 0; d = c; c = b; b = a; a = (t1 + t2) >>> 0;
    }
    hash[0] = (hash[0] + a) >>> 0; hash[1] = (hash[1] + b) >>> 0;
    hash[2] = (hash[2] + c) >>> 0; hash[3] = (hash[3] + d) >>> 0;
    hash[4] = (hash[4] + e) >>> 0; hash[5] = (hash[5] + f) >>> 0;
    hash[6] = (hash[6] + g) >>> 0; hash[7] = (hash[7] + h) >>> 0;
  }
  return Array.from(hash, (value) => value.toString(16).padStart(8, '0')).join('');
}

function pushU32(target: number[], value: number): void {
  target.push((value >>> 24) & 255, (value >>> 16) & 255, (value >>> 8) & 255, value & 255);
}

function pushU64(target: number[], value: number): void {
  const big = BigInt(value);
  for (let shift = 56n; shift >= 0n; shift -= 8n) target.push(Number((big >> shift) & 255n));
}

function pushString(target: number[], value: string): void {
  const bytes = new TextEncoder().encode(value);
  pushU64(target, bytes.length);
  target.push(...bytes);
}

function pushFloat64(target: number[], value: number): void {
  const buffer = new ArrayBuffer(8);
  new DataView(buffer).setFloat64(0, value, false);
  target.push(...new Uint8Array(buffer));
}

export function computeLayoutHash(layout: GenomeLayoutV1, assembly: string): string {
  const bytes: number[] = [...new TextEncoder().encode('genohype.association.layout.v1\0')];
  pushString(bytes, canonicalAssembly(assembly));
  pushU32(bytes, layout.width);
  pushU32(bytes, layout.height);
  pushU32(bytes, layout.contig_gap);
  pushU64(bytes, layout.contigs.length);
  for (const contig of layout.contigs) {
    pushString(bytes, contig.name);
    pushU64(bytes, contig.length);
  }
  pushFloat64(bytes, layout.y_scale.log_threshold);
  pushFloat64(bytes, layout.y_scale.linear_fraction);
  pushFloat64(bytes, layout.y_scale.max_neg_log_p);
  return `sha256:${sha256(Uint8Array.from(bytes))}`;
}

export function verifyLayoutHash(layout: GenomeLayoutV1, assembly: string, actual: string): void {
  verifyCanonicalGeometry(layout);
  const expected = computeLayoutHash(layout, assembly);
  if (actual !== expected) throw new Error(`layout hash mismatch: expected ${expected}, got ${actual}`);
}
