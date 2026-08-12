# Source-manifest inventory

`genohype manifest inventory` creates a bounded, immutable inventory of the objects Genohype needs to read one Hail table. It is a read-only safety primitive: it does not scan row bodies while planning, modify the table, write cloud objects, or publish exported data.

```bash
genohype manifest inventory gs://bucket/path/table.ht \
  --output source-manifest.json \
  --access sequential
```

Options:

- `--output <LOCAL|->` is required. `-` writes only canonical manifest JSON to stdout.
- `--digest-output <LOCAL>` defaults to `<output>.sha256`. With stdout and no explicit digest path, the digest is reported on stderr.
- `--access sequential|indexed` defaults to `sequential`.
- `--max-objects N` bounds each source observation (default: 1,000,000).

Outputs are create-new local files. Existing paths, cloud output URLs, and a manifest/digest path collision are refused. The digest receipt has this form:

```text
genohype.source-manifest.sha256.v1 <64-lowercase-hex-digest>
```

The digest is the existing domain-separated `SourceManifest::digest_sha256()` over compact canonical schema-v1 JSON; it is not a plain `sha256sum` of the newline-terminated output file.

## Exact closure

Both profiles include:

- top-level `metadata.json.gz` and `_SUCCESS`;
- rows and globals metadata at the exact `components.*.rel_path` values;
- every rows and globals part declared by `_partFiles`;
- every regular object recursively beneath `references_rel_path`.

`indexed` additionally includes each per-part index metadata object and the index data object named by its `indexPath`. `sequential` excludes the complete metadata-declared index namespace, matching a full partition scan.

Inventory fails closed on missing declared objects, unexpected non-checksum objects under relevant component/index roots, path traversal, metadata collisions or inconsistent part counts, symlinks, object limits, and changes between two observations. Hadoop `.<target>.crc` sidecars, unrelated root files, and sequential-profile indexes are ignored and reported by count.

## Identity and provider support

Local roots must be absolute directories. Every selected regular file is streamed through SHA-256. Its manifest identity is:

- `size`: streamed byte count;
- `sha256`: lowercase hexadecimal SHA-256;
- `version` and `etag`: `local-sha256:<digest>`;
- `crc32c`: absent.

With the `gcp` feature (enabled by default), GCS roots must be canonical `gs://bucket/nonempty-prefix` locations. Inventory uses Application Default Credentials, including `GOOGLE_APPLICATION_CREDENTIALS`. It paginates the live-object JSON API twice and requires stable generation, size, standard-base64 CRC32C, and JSON listing ETag observations. Each selected generation is then HEADed through the existing `object_store` GCS client; the manifest records that generation and the exact nonempty HEAD ETag used by pinned readers. Planning metadata bodies are read with both generation and ETag preconditions. Row, globals, and index data bodies are not downloaded, and source-body SHA-256 is intentionally absent for GCS objects.

A build without `gcp` fails clearly for GCS input. S3 and HTTP roots remain unsupported by source-manifest inventory v1. Outputs are still local-only: GCS inventory never writes a cloud object.
