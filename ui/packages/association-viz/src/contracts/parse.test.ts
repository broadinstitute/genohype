import { describe, expect, it } from 'vitest';
import { rawGoldenManifest } from '../test/fixture.js';
import {
  ManifestValidationError,
  parseAssociationVisualizationManifest,
  resolveAssociationResourceUrl,
} from './parse.js';

type Json = Record<string, any>;
const clone = () => structuredClone(rawGoldenManifest()) as Json;

describe('association manifest V1 parser', () => {
  it('parses the Rust golden and retains compatible minor fields', () => {
    const input = clone();
    input.schema_version = '1.17';
    input.future_minor_field = { supported: true };
    input.layout.future_scale_metadata = 'ignored';
    const manifest = parseAssociationVisualizationManifest(input);
    expect(manifest.schema_version).toBe('1.17');
    expect(manifest.future_minor_field).toEqual({ supported: true });
  });

  it.each(['2.0', '0.9'])('rejects unsupported major %s', (version) => {
    const input = clone();
    input.schema_version = version;
    expect(() => parseAssociationVisualizationManifest(input)).toThrow(/unsupported.*major/i);
  });

  it.each(['1', 'v1', '1.x', '1.2.3', '01.0'])('rejects malformed version %s', (version) => {
    const input = clone();
    input.schema_version = version;
    expect(() => parseAssociationVisualizationManifest(input)).toThrow(/malformed schema version/i);
  });

  it.each([
    '', '/absolute.png', '\\\\server\\plot.png', 'https://example.test/a', 'file:plot.png',
    'a/../secret', 'plot.png?token=secret', 'user@example.test/plot.png', 'a//plot.png', 'a/b:c.png',
  ])('rejects malformed resource %j', (url) => {
    const input = clone();
    input.resources.raster.url = url;
    expect(() => parseAssociationVisualizationManifest(input)).toThrow(ManifestValidationError);
    try {
      parseAssociationVisualizationManifest(input);
    } catch (error) {
      expect((error as ManifestValidationError).code).toBe('resource');
    }
  });

  it('classifies missing or malformed descriptors as resource failures', () => {
    for (const mutate of [
      (input: Json) => { delete input.resources.raster; },
      (input: Json) => { input.resources.raster.media_type = ''; },
      (input: Json) => { input.resources.raster.byte_size = -1; },
    ]) {
      const input = clone();
      mutate(input);
      try {
        parseAssociationVisualizationManifest(input);
        throw new Error('expected parse to fail');
      } catch (error) {
        expect(error).toBeInstanceOf(ManifestValidationError);
        expect((error as ManifestValidationError).code).toBe('resource');
      }
    }
  });

  it('rejects inconsistent domain counts and noncanonical points', () => {
    const counts = clone();
    counts.valid_row_count = 1;
    expect(() => parseAssociationVisualizationManifest(counts)).toThrow(/counts/i);
    const alias = clone();
    alias.top_hits[0].contig = 'chr1';
    expect(() => parseAssociationVisualizationManifest(alias)).toThrow(/canonical spelling/i);
    const frequency = clone();
    frequency.top_hits[0].allele_frequency = 1.1;
    expect(() => parseAssociationVisualizationManifest(frequency)).toThrow(/allele_frequency/i);
    const oversizedWidth = clone();
    oversizedWidth.layout.width = 0x1_0000_0000;
    expect(() => parseAssociationVisualizationManifest(oversizedWidth)).toThrow(/layout.width/i);
  });

  it('safely resolves resources relative to the fetched manifest', () => {
    const manifest = parseAssociationVisualizationManifest(clone());
    expect(resolveAssociationResourceUrl(
      manifest.resources.raster,
      'https://example.test/token/api/manifest',
    ).href).toBe('https://example.test/token/api/height-genome.png');
    expect(() => resolveAssociationResourceUrl(
      { url: '../secret', media_type: 'image/png' },
      'https://example.test/token/api/manifest',
    )).toThrow(/safe/i);
    expect(() => resolveAssociationResourceUrl(
      manifest.resources.raster,
      '/relative/manifest',
    )).toThrow(/absolute/i);
  });
});
