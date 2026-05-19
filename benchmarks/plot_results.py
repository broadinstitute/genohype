#!/usr/bin/env python3
"""Generate interactive Plotly HTML dashboard from benchmark results.

Usage:
    python3 benchmarks/plot_results.py                              # default input/output
    python3 benchmarks/plot_results.py -i results.json -o dash.html
"""

import argparse
import json

import pandas as pd
import plotly
import plotly.graph_objects as go
from plotly.subplots import make_subplots


# Maps projection labels to the CLI flags used
PROJECTION_FIELDS = {
    "full": "(all fields, no flags)",
    "minimal": "--fields locus,alleles",
    "freq": "--fields locus,alleles,freq",
    "freq_continental": "--fields locus,alleles,freq[:11]",
    "clinical": "--fields locus,alleles,freq[:11],filters,vep.transcript_consequences[0]",
    "exclude_vep": "--exclude vep,vep115,histograms",
}


def load_results(path: str) -> pd.DataFrame:
    with open(path) as f:
        data = json.load(f)
    df = pd.DataFrame(data)
    # Sort regions by size for consistent axis ordering
    df = df.sort_values("region_size_kb")
    return df


def fig_time_vs_region(df: pd.DataFrame) -> go.Figure:
    """Line chart: wall time vs region size, one line per projection."""
    warm = df[df["cache"] == "warm"]
    fig = go.Figure()
    for proj in warm["projection"].unique():
        subset = warm[warm["projection"] == proj].sort_values("region_size_kb")
        fig.add_trace(go.Scatter(
            x=subset["region_size_kb"],
            y=subset["total_time_ms"],
            mode="lines+markers",
            name=proj,
            hovertemplate=(
                "Region: %{customdata[0]}<br>"
                "Size: %{x}kb<br>"
                "Time: %{y:.1f}ms<br>"
                "Rows: %{customdata[1]}"
            ),
            customdata=subset[["region", "rows"]].values,
        ))
    fig.update_layout(
        title="Query Time vs Region Size (warm cache)",
        xaxis_title="Region Size (kb)",
        yaxis_title="Total Time (ms)",
        xaxis_type="log",
        yaxis_type="log",
        hovermode="x unified",
    )
    return fig


def fig_phase_breakdown(df: pd.DataFrame) -> go.Figure:
    """Stacked bar chart: per-phase timing breakdown."""
    phase_cols = ["metadata_ms", "pruning_ms", "partition_open_ms", "partition_decode_ms", "gcs_reader_ms"]
    available_phases = [c for c in phase_cols if c in df.columns]
    if not available_phases:
        fig = go.Figure()
        fig.update_layout(title="Phase Breakdown (no trace data available)")
        return fig

    # Show both cold and warm for comparison
    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=["Cold Cache", "Warm Cache"],
        shared_yaxes=True,
    )

    colors = {
        "metadata_ms": "#636EFA",
        "pruning_ms": "#EF553B",
        "partition_open_ms": "#FFA15A",
        "partition_decode_ms": "#AB63FA",
        "gcs_reader_ms": "#00CC96",
    }

    for col_idx, cache_type in enumerate(["cold", "warm"], 1):
        subset = df[df["cache"] == cache_type].sort_values("region_size_kb")
        if subset.empty:
            continue
        x_labels = [f"{r}\n({p})" for r, p in zip(subset["region"], subset["projection"])]
        for phase in available_phases:
            if phase not in subset.columns:
                continue
            fig.add_trace(
                go.Bar(
                    x=x_labels,
                    y=subset[phase].fillna(0),
                    name=phase.replace("_ms", "").replace("_", " ").title(),
                    marker_color=colors.get(phase),
                    legendgroup=phase,
                    showlegend=(col_idx == 1),
                ),
                row=1, col=col_idx,
            )

    fig.update_layout(
        barmode="stack",
        title="Phase Breakdown by Region",
        yaxis_title="Time (ms)",
        height=500,
    )
    return fig


def fig_projection_comparison(df: pd.DataFrame) -> go.Figure:
    """Grouped bar chart: projection comparison per region."""
    warm = df[df["cache"] == "warm"]
    fig = go.Figure()

    regions_ordered = warm.sort_values("region_size_kb")["region"].unique()

    for proj in warm["projection"].unique():
        subset = warm[warm["projection"] == proj]
        # Align to region order
        region_times = []
        for r in regions_ordered:
            match = subset[subset["region"] == r]
            region_times.append(match["total_time_ms"].values[0] if len(match) > 0 else 0)
        fig.add_trace(go.Bar(
            x=list(regions_ordered),
            y=region_times,
            name=proj,
        ))

    fig.update_layout(
        barmode="group",
        title="Projection Comparison by Region (warm cache)",
        xaxis_title="Region",
        yaxis_title="Total Time (ms)",
        yaxis_type="log",
    )
    return fig


def fig_throughput(df: pd.DataFrame) -> go.Figure:
    """Throughput chart: rows/second for each configuration."""
    warm = df[df["cache"] == "warm"].copy()
    warm["rows_per_sec"] = warm["rows"] / (warm["total_time_ms"] / 1000.0)

    fig = go.Figure()
    for proj in warm["projection"].unique():
        subset = warm[warm["projection"] == proj].sort_values("region_size_kb")
        fig.add_trace(go.Scatter(
            x=subset["region_size_kb"],
            y=subset["rows_per_sec"],
            mode="lines+markers",
            name=proj,
            hovertemplate=(
                "Region: %{customdata[0]}<br>"
                "Size: %{x}kb<br>"
                "Throughput: %{y:,.0f} rows/s<br>"
                "Rows: %{customdata[1]}"
            ),
            customdata=subset[["region", "rows"]].values,
        ))

    fig.update_layout(
        title="Query Throughput (warm cache)",
        xaxis_title="Region Size (kb)",
        yaxis_title="Rows / Second",
        xaxis_type="log",
        yaxis_type="log",
        hovermode="x unified",
    )
    return fig


def build_dashboard(df: pd.DataFrame) -> str:
    """Build a single HTML page with all charts."""
    figs = [
        fig_time_vs_region(df),
        fig_phase_breakdown(df),
        fig_projection_comparison(df),
        fig_throughput(df),
    ]

    # Build combined HTML
    html_parts = [
        "<!DOCTYPE html>",
        "<html><head>",
        '<meta charset="utf-8">',
        "<title>Genohype Query Benchmark Dashboard</title>",
        '<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>',
        "<style>",
        "body { font-family: sans-serif; margin: 20px; background: #fafafa; }",
        "h1 { color: #333; }",
        ".chart { margin: 20px 0; background: white; border-radius: 8px; "
        "box-shadow: 0 1px 3px rgba(0,0,0,0.1); padding: 10px; }",
        "table.proj-ref { border-collapse: collapse; margin: 10px 0; font-size: 14px; }",
        "table.proj-ref th, table.proj-ref td { border: 1px solid #ddd; padding: 6px 12px; text-align: left; }",
        "table.proj-ref th { background: #f5f5f5; }",
        "table.proj-ref code { background: #eee; padding: 2px 5px; border-radius: 3px; font-size: 13px; }",
        "</style>",
        "</head><body>",
        "<h1>Genohype Query Benchmark Dashboard</h1>",
        f"<p>{len(df)} data points</p>",
        '<table class="proj-ref"><tr><th>Projection</th><th>CLI Flags</th></tr>',
    ]
    for label, flags in PROJECTION_FIELDS.items():
        html_parts.append(f"<tr><td><b>{label}</b></td><td><code>{flags}</code></td></tr>")
    html_parts.append("</table>")

    for i, fig in enumerate(figs):
        div_id = f"chart_{i}"
        html_parts.append(f'<div class="chart" id="{div_id}"></div>')
        fig_json = fig.to_json()
        html_parts.append(f"<script>Plotly.newPlot('{div_id}', {fig_json});</script>")

    html_parts.extend(["</body></html>"])
    return "\n".join(html_parts)


def main():
    parser = argparse.ArgumentParser(description="Plot genohype benchmark results")
    parser.add_argument("-i", "--input", default="benchmarks/benchmark_results.json",
                        help="Input results JSON file")
    parser.add_argument("-o", "--output", default="benchmarks/benchmark_dashboard.html",
                        help="Output HTML dashboard file")
    args = parser.parse_args()

    df = load_results(args.input)
    print(f"Loaded {len(df)} results from {args.input}")

    html = build_dashboard(df)

    with open(args.output, "w") as f:
        f.write(html)

    print(f"Dashboard written to {args.output}")


if __name__ == "__main__":
    main()
