#!/usr/bin/env python3
"""Query benchmark orchestrator for genohype.

Runs a test matrix of regions × projections, collects --stats-json output
and --profile Chrome trace data, and writes benchmark_results.json.

Usage:
    python3 benchmarks/run_query_benchmarks.py --dry-run       # verify matrix
    python3 benchmarks/run_query_benchmarks.py                 # full run
    python3 benchmarks/run_query_benchmarks.py --warm-runs 5   # more repetitions
"""

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

# Default table path (gnomAD exomes v4.1.1)
DEFAULT_TABLE = "gs://gcp-public-data--gnomad/release/4.1.1/ht/exomes/gnomad.exomes.v4.1.1.sites.ht"

# ── Test matrix: regions ──────────────────────────────────────────────────────

REGIONS = [
    {"label": "1kb",   "interval": "chr17:43044295-43045295", "size_kb": 1},
    {"label": "2kb",   "interval": "chr17:43044295-43046295", "size_kb": 2},
    {"label": "10kb",  "interval": "chr17:43044295-43054295", "size_kb": 10},
    {"label": "50kb",  "interval": "chr17:43044295-43094295", "size_kb": 50},
    {"label": "126kb_BRCA1", "interval": "chr17:43044295-43170245", "size_kb": 126},
    {"label": "281kb_TTN",   "interval": "chr2:178525989-178807423", "size_kb": 281},
    {"label": "1Mb",   "interval": "chr17:43044295-44044295", "size_kb": 1000},
    {"label": "10Mb",  "interval": "chr17:40000000-50000000", "size_kb": 10000},
    {"label": "full_chr17",  "interval": "chr17:1-83257441", "size_kb": 83257},
]

# ── Test matrix: projections ──────────────────────────────────────────────────

PROJECTIONS = [
    {"label": "full",             "args": []},
    {"label": "minimal",          "args": ["--fields", "locus,alleles"]},
    {"label": "freq",             "args": ["--fields", "locus,alleles,freq"]},
    {"label": "freq_continental", "args": ["--fields", "locus,alleles,freq[:11]"]},
    {"label": "clinical",         "args": ["--fields", "locus,alleles,freq[:11],filters,vep.transcript_consequences[0]"]},
    {"label": "exclude_vep",      "args": ["--exclude", "vep,vep115,histograms"]},
]

# For regions >= 1Mb, only run these projections to keep runtime reasonable
LARGE_REGION_PROJECTIONS = {"minimal", "freq_continental"}

LARGE_REGION_THRESHOLD_KB = 1000


def build_matrix(table: str, max_region_kb: int | None = None) -> list[dict]:
    """Build the full test matrix, restricting projections for large regions."""
    matrix = []
    for region in REGIONS:
        if max_region_kb is not None and region["size_kb"] > max_region_kb:
            continue
        for proj in PROJECTIONS:
            if region["size_kb"] >= LARGE_REGION_THRESHOLD_KB and proj["label"] not in LARGE_REGION_PROJECTIONS:
                continue
            matrix.append({
                "region": region,
                "projection": proj,
                "table": table,
            })
    return matrix


def parse_chrome_trace(trace_path: str) -> dict:
    """Parse a Chrome trace JSON file and extract per-phase timings in ms.

    Matches Begin/End events by thread ID and span name.
    Returns aggregated durations for known span categories.
    """
    phases = {
        "metadata_ms": 0.0,
        "partition_worker_ms": 0.0,
        "partition_open_ms": 0.0,
        "partition_decode_ms": 0.0,
        "gcs_reader_ms": 0.0,
        "pruning_ms": 0.0,
    }

    try:
        with open(trace_path) as f:
            content = f.read().strip()
            # Handle both array format and object-with-traceEvents format
            data = json.loads(content)
            if isinstance(data, dict):
                events = data.get("traceEvents", [])
            else:
                events = data
    except (json.JSONDecodeError, FileNotFoundError):
        return phases

    # Track open spans: (tid, name) -> timestamp_us
    open_spans: dict[tuple, float] = {}

    # Span name -> phase key mapping
    span_map = {
        "HailTableSource::new": "metadata_ms",
        "HailTableSource::open": "metadata_ms",
        "partition_worker": "partition_worker_ms",
        "partition_worker_projected": "partition_worker_ms",
        "partition_open": "partition_open_ms",
        "partition_decode": "partition_decode_ms",
        "create_cloud_reader": "gcs_reader_ms",
        "query_stream_with_intervals": "pruning_ms",
        "query_stream_with_projection": "pruning_ms",
    }

    for event in events:
        ph = event.get("ph")
        name = event.get("name", "")
        tid = event.get("tid", 0)
        ts = event.get("ts", 0)

        if name not in span_map:
            continue

        key = (tid, name)
        if ph == "B":
            open_spans[key] = ts
        elif ph == "E" and key in open_spans:
            duration_us = ts - open_spans.pop(key)
            phases[span_map[name]] += duration_us / 1000.0  # us -> ms

    return phases


def run_query(table: str, interval: str, proj_args: list[str],
              profile_path: str | None = None,
              genohype_bin: str = "genohype") -> dict | None:
    """Run a single genohype query and return parsed stats + trace phases."""
    cmd = [genohype_bin, "query", table, "--interval", interval, "--stats-json"]
    if profile_path:
        cmd.extend(["--profile", profile_path])
    cmd.extend(proj_args)

    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=600
        )
    except subprocess.TimeoutExpired:
        print(f"  TIMEOUT after 600s", file=sys.stderr)
        return None

    if result.returncode != 0:
        print(f"  ERROR (exit {result.returncode}): {result.stderr[:200]}", file=sys.stderr)
        return None

    # Parse stats JSON from stdout
    try:
        stats = json.loads(result.stdout.strip())
    except json.JSONDecodeError:
        print(f"  Failed to parse stats JSON: {result.stdout[:200]}", file=sys.stderr)
        return None

    # Parse trace if available
    if profile_path and os.path.exists(profile_path):
        phases = parse_chrome_trace(profile_path)
        stats.update(phases)

    return stats


def median(values: list[float]) -> float:
    """Return the median of a list of numbers."""
    s = sorted(values)
    n = len(s)
    if n == 0:
        return 0.0
    mid = n // 2
    if n % 2 == 0:
        return (s[mid - 1] + s[mid]) / 2.0
    return s[mid]


def run_benchmark(matrix: list[dict], warm_runs: int = 3,
                  genohype_bin: str = "genohype",
                  skip_cold: bool = False) -> list[dict]:
    """Execute the full benchmark matrix and return results."""
    results = []
    total = len(matrix)

    for i, entry in enumerate(matrix, 1):
        region = entry["region"]
        proj = entry["projection"]
        table = entry["table"]

        print(f"\n[{i}/{total}] {region['label']} × {proj['label']}", file=sys.stderr)

        # Cold run (after cache clear)
        if not skip_cold:
            print("  Cold run...", file=sys.stderr)
            subprocess.run(
                [genohype_bin, "cache", "clear"],
                capture_output=True, timeout=30
            )
            with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tf:
                cold_trace = tf.name
            try:
                cold_stats = run_query(
                    table, region["interval"], proj["args"],
                    profile_path=cold_trace, genohype_bin=genohype_bin
                )
            finally:
                if os.path.exists(cold_trace):
                    os.unlink(cold_trace)

            if cold_stats:
                results.append({
                    "region": region["label"],
                    "interval": region["interval"],
                    "region_size_kb": region["size_kb"],
                    "projection": proj["label"],
                    "cache": "cold",
                    **cold_stats,
                })

        # Warm runs
        warm_stats_list = []
        for w in range(warm_runs):
            print(f"  Warm run {w + 1}/{warm_runs}...", file=sys.stderr)
            with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tf:
                warm_trace = tf.name
            try:
                stats = run_query(
                    table, region["interval"], proj["args"],
                    profile_path=warm_trace, genohype_bin=genohype_bin
                )
            finally:
                if os.path.exists(warm_trace):
                    os.unlink(warm_trace)

            if stats:
                warm_stats_list.append(stats)

        if warm_stats_list:
            # Take the median of numeric fields across warm runs
            numeric_keys = [k for k in warm_stats_list[0] if isinstance(warm_stats_list[0][k], (int, float))]
            median_stats = {}
            for k in numeric_keys:
                values = [s[k] for s in warm_stats_list if k in s]
                median_stats[k] = median(values)
            # Preserve integer type for row/partition counts
            for k in ("rows", "partitions"):
                if k in median_stats:
                    median_stats[k] = int(median_stats[k])

            results.append({
                "region": region["label"],
                "interval": region["interval"],
                "region_size_kb": region["size_kb"],
                "projection": proj["label"],
                "cache": "warm",
                **median_stats,
            })

    return results


def print_dry_run(matrix: list[dict]) -> None:
    """Print the test matrix without running anything."""
    print(f"Test matrix: {len(matrix)} configurations\n")
    print(f"{'#':>3}  {'Region':<15} {'Size':>8}  {'Projection':<20} {'Interval'}")
    print("-" * 90)
    for i, entry in enumerate(matrix, 1):
        r = entry["region"]
        p = entry["projection"]
        size_str = f"{r['size_kb']}kb" if r["size_kb"] < 1000 else f"{r['size_kb'] // 1000}Mb"
        print(f"{i:>3}  {r['label']:<15} {size_str:>8}  {p['label']:<20} {r['interval']}")

    # Summary
    n_regions = len(set(e["region"]["label"] for e in matrix))
    n_projections = len(set(e["projection"]["label"] for e in matrix))
    print(f"\n{n_regions} regions × {n_projections} projections (restricted for regions >= {LARGE_REGION_THRESHOLD_KB}kb)")
    print(f"Total configurations: {len(matrix)}")
    print(f"Total runs (1 cold + 3 warm each): {len(matrix) * 4}")


def main():
    parser = argparse.ArgumentParser(description="Run genohype query benchmarks")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print the test matrix without running benchmarks")
    parser.add_argument("--table", default=DEFAULT_TABLE,
                        help="Path to the Hail table to benchmark")
    parser.add_argument("--output", default="benchmarks/benchmark_results.json",
                        help="Output file for results (default: benchmarks/benchmark_results.json)")
    parser.add_argument("--warm-runs", type=int, default=3,
                        help="Number of warm runs per configuration (default: 3)")
    parser.add_argument("--genohype", default="genohype",
                        help="Path to genohype binary (default: genohype)")
    parser.add_argument("--skip-cold", action="store_true",
                        help="Skip cold (cache-cleared) runs")
    parser.add_argument("--max-region-kb", type=int, default=None,
                        help="Only include regions up to this size in kb")
    args = parser.parse_args()

    matrix = build_matrix(args.table, max_region_kb=args.max_region_kb)

    if args.dry_run:
        print_dry_run(matrix)
        return

    print(f"Starting benchmark: {len(matrix)} configurations, "
          f"{args.warm_runs} warm runs each", file=sys.stderr)

    results = run_benchmark(
        matrix,
        warm_runs=args.warm_runs,
        genohype_bin=args.genohype,
        skip_cold=args.skip_cold,
    )

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(results, f, indent=2)

    print(f"\nWrote {len(results)} results to {output_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
