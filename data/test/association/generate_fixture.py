#!/usr/bin/env python3
"""Regenerate the deterministic local association Hail fixture.

Run without adding a persistent Python environment to the repository:

    uv run --python 3.12 --with hail python \
      data/test/association/generate_fixture.py
"""

import argparse
import gzip
import json
from pathlib import Path

import hail as hl


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT = Path(__file__).resolve().parent / "single-variant.ht"


def association_rows():
    def row(contig, position, ref, alt, pvalue, log10=None, beta=None, se=None, af=None, ac=None):
        return hl.Struct(
            locus=hl.Locus(contig, position, reference_genome="GRCh38"),
            alleles=[ref, alt],
            Pvalue=pvalue,
            Pvalue_log10=log10,
            BETA=beta,
            SE=se,
            AF_Allele2=af,
            AC_Allele2=ac,
        )

    return [
        row("chr1", 101_001, "A", "G", 0.5, beta=0.1, se=0.05, af=0.20, ac=40.0),
        row("chr1", 101_002, "C", "T", 5e-8, beta=-0.2, se=0.04, af=0.01, ac=2.0),
        row("chr1", 101_003, "G", "A", 1e-12, beta=0.7, se=0.08, af=0.005, ac=1.0),
        # Finite invalid rows keep the CLI JSON smoke output standards-compliant.
        row("chr1", 101_004, "T", "C", -0.1),
        row("chr1", 101_005, "A", "C", 1.5, beta=0.0, se=0.2),
        row("chr4", 68_000_001, "A", "C", 0.05, beta=0.02, se=0.01, af=0.15, ac=30.0),
        # Same effective significance as chr1:101003, for deterministic tie tests.
        row("chr4", 68_000_010, "C", "G", 1e-12, beta=-0.7, se=0.08, af=0.005, ac=1.0),
        row("chr4", 68_123_456, "T", "A", 2e-30, beta=1.2, se=0.2),
        # Zero means underflow; the supplied positive -log10 value is authoritative.
        row("chr4", 68_500_000, "G", "A", 0.0, log10=320.0, beta=-1.4, se=0.3, af=0.0001, ac=1.0),
        row("chr4", 68_750_000, "A", "AT", 0.8, af=0.25, ac=50.0),
        row("chr4", 68_900_000, "C", "T", 4e-8, beta=0.4, se=0.1, af=0.03),
        row("chr4", 69_000_000, "CT", "C", 1e-6, log10=6.0, beta=0.3, se=0.07, af=0.02, ac=4.0),
    ]


def canonicalize(output: Path) -> None:
    row_part = next((output / "rows" / "parts").glob("part-*"))
    row_part.rename(row_part.with_name("part-0"))

    index_part = next((output / "index").glob("part-*.idx"))
    index_part.rename(index_part.with_name("part-0.idx"))

    rows_metadata_path = output / "rows" / "metadata.json.gz"
    with gzip.open(rows_metadata_path, "rt") as metadata_file:
        rows_metadata = json.load(metadata_file)
    rows_metadata["_partFiles"] = ["part-0"]

    for metadata_path in output.rglob("*.json.gz"):
        if metadata_path == rows_metadata_path:
            metadata = rows_metadata
        else:
            with gzip.open(metadata_path, "rt") as metadata_file:
                metadata = json.load(metadata_file)
        encoded = json.dumps(metadata, sort_keys=True, separators=(",", ":")).encode()
        with metadata_path.open("wb") as raw_file:
            with gzip.GzipFile(fileobj=raw_file, mode="wb", mtime=0) as gzip_file:
                gzip_file.write(encoded)

    for checksum in output.rglob("*.crc"):
        checksum.unlink()
    for optional_file in (output / "_SUCCESS", output / "README.txt"):
        optional_file.unlink(missing_ok=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("output", nargs="?", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    output = args.output.resolve()

    hl.init(master="local[1]", quiet=True, log="/tmp/genohype-association-fixture.log")
    schema = hl.tstruct(
        locus=hl.tlocus("GRCh38"),
        alleles=hl.tarray(hl.tstr),
        Pvalue=hl.tfloat64,
        Pvalue_log10=hl.tfloat64,
        BETA=hl.tfloat64,
        SE=hl.tfloat64,
        AF_Allele2=hl.tfloat64,
        AC_Allele2=hl.tfloat64,
    )
    table = hl.Table.parallelize(
        association_rows(), schema=schema, key=["locus", "alleles"], n_partitions=1
    )
    table.write(str(output), overwrite=True)
    canonicalize(output)

    try:
        shown = output.relative_to(REPO_ROOT)
    except ValueError:
        shown = output
    print(f"Wrote {shown}")


if __name__ == "__main__":
    main()
