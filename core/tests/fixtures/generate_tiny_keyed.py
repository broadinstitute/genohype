#!/usr/bin/env python3
"""Regenerate the tiny keyed Hail table used by Rust tests.

This provenance script is not part of the normal build or test path. To run it
without adding a persistent Python environment to the repository, use:

    uv run --python 3.12 --with hail python core/tests/fixtures/generate_tiny_keyed.py
"""

import gzip
import json
from pathlib import Path

import hail as hl


REPO_ROOT = Path(__file__).resolve().parents[3]
OUTPUT = Path(__file__).resolve().parent / "tiny-keyed.ht"


def main() -> None:
    hl.init(master="local[1]", quiet=True, log="/tmp/genohype-hail-fixture.log")

    rows = [
        {
            "gene_id": "ENSG00000066468",
            "chrom": "10",
            "start": 121_478_332,
            "value": 1,
        },
        {
            "gene_id": "ENSG00000141510",
            "chrom": "17",
            "start": 7_668_402,
            "value": 2,
        },
    ]
    table = hl.Table.parallelize(
        rows,
        key=["gene_id", "chrom", "start"],
        n_partitions=1,
    )
    table.write(str(OUTPUT), overwrite=True)

    # Hail gives each partition a random UUID. Normalize the one-partition
    # fixture so regenerating it does not create an unrelated Git diff.
    row_part = next((OUTPUT / "rows" / "parts").glob("part-*"))
    row_part.rename(row_part.with_name("part-0"))

    index_part = next((OUTPUT / "index").glob("part-*.idx"))
    index_part.rename(index_part.with_name("part-0.idx"))

    rows_metadata_path = OUTPUT / "rows" / "metadata.json.gz"
    with gzip.open(rows_metadata_path, "rt") as metadata_file:
        rows_metadata = json.load(metadata_file)
    rows_metadata["_partFiles"] = ["part-0"]

    # Canonicalize compressed JSON, including gzip timestamps and JSON key
    # order, so the generated files are byte-for-byte reproducible.
    for metadata_path in OUTPUT.rglob("*.json.gz"):
        if metadata_path == rows_metadata_path:
            metadata = rows_metadata
        else:
            with gzip.open(metadata_path, "rt") as metadata_file:
                metadata = json.load(metadata_file)
        encoded = json.dumps(metadata, sort_keys=True, separators=(",", ":")).encode()
        with metadata_path.open("wb") as raw_file:
            with gzip.GzipFile(fileobj=raw_file, mode="wb", mtime=0) as gzip_file:
                gzip_file.write(encoded)

    # Hadoop checksum sidecars are not needed by Genohype and add noise to the
    # fixture. Keep only files required to read the table and its index.
    for checksum in OUTPUT.rglob("*.crc"):
        checksum.unlink()
    for optional_file in (OUTPUT / "_SUCCESS", OUTPUT / "README.txt"):
        optional_file.unlink(missing_ok=True)

    print(f"Wrote {OUTPUT.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    main()
