# Paneled figure of NAM export boost only

import pandas as pd
from message_ix_models.util import package_data_path
import matplotlib.pyplot as plt
import numpy as np
import yaml

from update_scenario_names import update_scenario_names

# Import plot configurations
with open(package_data_path('weu_security', 'reporting', 'plot_config.yaml'), 'r') as f:
    config = yaml.safe_load(f)
region_colors = config['region_colors'] 
region_labels = config['region_labels']

# Import fuel supply data
indf = pd.read_csv(package_data_path('weu_security', 'reporting', 'fuel_supply_out.csv'))
indf = indf[indf['supply_type'].isin(['Imports', 'Exports'])]
indf = indf[indf['model'] == 'weu_security'] # no reexports
indf = update_scenario_names(indf)

def make_paneled_figure(
    df,
    plot_year: int,
    plot_exporter: str,
    plot_scenarios: list[str],
    plot_regions: list[str],
    plot_fuel_types: list[str],
    plot_fuel_colors: dict[str, str] = None,
    plot_all_years: list[int] = [2025, 2030, 2035, 2040, 2045, 2050, 2055, 2060],
    plot_ref_scenario: str = 'REF',
    region_labels: dict[str, str] = None,
    region_colors: dict[str, str] = None,
    row_scenario_groups: list[list[str]] = None,
    row_keywords: list[str] = None,
):
    from matplotlib.lines import Line2D
    from matplotlib.patches import Patch

    if row_scenario_groups is None:
        keywords = row_keywords or ['REF', 'FSU2040', 'FSULONG']
        row_scenario_groups = [
            [s for s in plot_scenarios if keyword in s]
            for keyword in keywords
        ]
        row_scenario_groups = [g for g in row_scenario_groups if g]

    n_rows = len(row_scenario_groups)

    # Clean data
    plotdf = df[df['year'].isin(plot_all_years)]
    plotdf = plotdf[plotdf["region"].isin(plot_regions)]
    plotdf = plotdf[plotdf["scenario"].isin(plot_scenarios)]
    plotdf = plotdf[plotdf["fuel_type"].isin(plot_fuel_types)]

    def rlabel(code):
        if region_labels and code in region_labels:
            return region_labels[code]
        return code

    # Shared marker map
    all_non_ref = [s for s in plot_scenarios if s != plot_ref_scenario]
    marker_styles = ["o", "s", "^", "D", "v", "P", "X"]

    _keywords = ['FSU2040', 'FSULONG', 'SSP2']

    def _scenario_suffix(s: str) -> str:
        for kw in _keywords:
            prefix = kw + '_'
            if prefix in s:
                return s.split(prefix, 1)[1]
        return s

    _seen: dict[str, int] = {}
    for s in all_non_ref:
        suf = _scenario_suffix(s)
        if suf not in _seen:
            _seen[suf] = len(_seen)

    scenario_markers = {
        s: marker_styles[_seen[_scenario_suffix(s)] % len(marker_styles)]
        for s in all_non_ref
    }

    # LEFT panel color map — fuel types
    _default_fuel_colors = {
        "Gas":       "#56B4E9",
        "Coal":      "#333333",
        "Crude":     "#8B4513",
        "Fuel Oil":  "#D55E00",
        "Light Oil": "#E69F00",
        "Ethanol":   "#009E73",
    }
    if plot_fuel_colors is None:
        okabe_ito = [
            "#E69F00", "#56B4E9", "#009E73", "#F0E442",
            "#0072B2", "#D55E00", "#CC79A7", "#000000",
        ]
        fuel_colors = {
            fuel: _default_fuel_colors.get(fuel, okabe_ito[i % len(okabe_ito)])
            for i, fuel in enumerate(plot_fuel_types)
        }
    else:
        fuel_colors = plot_fuel_colors

    LEFT_TOTAL_COLOR = "black"

    # RIGHT panel color map — exporters
    tol_bright = [
        "#4477AA", "#EE6677", "#228833", "#CCBB44",
        "#66CCEE", "#AA3377", "#BBBBBB",
    ]
    all_exporters = sorted(
        e for e in plotdf[plotdf["scenario"] != plot_ref_scenario]["exporter"].unique()
        if isinstance(e, str)
    )
    if region_colors:
        exporter_colors = {
            exp: region_colors.get(exp, tol_bright[i % len(tol_bright)])
            for i, exp in enumerate(all_exporters)
        }
    else:
        exporter_colors = {
            exp: tol_bright[i % len(tol_bright)]
            for i, exp in enumerate(all_exporters)
        }

    # Precompute RIGHT panel delta data
    df_all_totals = (
        plotdf
        .groupby(["exporter", "scenario", "year"])["value"]
        .sum()
        .reset_index()
    )
    df_ref = (
        df_all_totals[df_all_totals["scenario"] == plot_ref_scenario]
        .rename(columns={"value": "ref_value"})
        [["exporter", "year", "ref_value"]]
    )
    df_delta = (
        df_all_totals[df_all_totals["scenario"] != plot_ref_scenario]
        .copy()
        .merge(df_ref, on=["exporter", "year"], how="left")
    )
    df_delta["delta"] = df_delta["value"] - df_delta["ref_value"]

    # Pre-compute shared y-axis limits
    left_vals, right_vals = [], []
    for row_scenarios in row_scenario_groups:
        row_non_ref = [s for s in row_scenarios if s != plot_ref_scenario]

        df_ll = plotdf[plotdf["exporter"] == plot_exporter].copy()
        df_ll = df_ll.groupby(["scenario", "fuel_type", "year"])["value"].sum().reset_index()
        df_ll = df_ll[df_ll["scenario"].isin(row_non_ref)]
        df_lt = df_ll.groupby(["scenario", "year"])["value"].sum().reset_index()
        left_vals += df_ll["value"].tolist() + df_lt["value"].tolist()

        df_dr = df_delta[df_delta["scenario"].isin(row_non_ref)]
        right_vals += df_dr["delta"].tolist()

    def _padded(vals, pad=0.05):
        lo, hi = min(vals), max(vals)
        margin = (hi - lo) * pad or abs(hi) * pad or 1
        return lo - margin, hi + margin

    left_ylim  = _padded(left_vals)  if left_vals  else (0, 1)
    right_ylim = _padded(right_vals) if right_vals else (0, 1)

    # Build figure
    fig = plt.figure(figsize=(14, 5 * n_rows))
    gs = fig.add_gridspec(n_rows, 6, hspace=0.25, wspace=1)

    for row_idx, row_scenarios in enumerate(row_scenario_groups):
        row_non_ref = [s for s in row_scenarios if s != plot_ref_scenario]

        row_keyword = next(
            (kw for kw in ['REF', 'FSU2040', 'FSULONG']
             if any(kw in s for s in row_scenarios)),
            ", ".join(row_scenarios),
        )

        ax_left  = fig.add_subplot(gs[row_idx, 0:2])
        ax_right = fig.add_subplot(gs[row_idx, 2:5])

        ax_left.set_title(
            f"Imports from {rlabel(plot_exporter)}, {row_keyword}",
            fontsize=11,
        )
        ax_right.set_title(
            f"Change in imports relative to baseline, {row_keyword}",
            fontsize=11,
        )

        # --- LEFT panel ---
        df_left_line = plotdf[plotdf["exporter"] == plot_exporter].copy()
        df_left_line = (
            df_left_line
            .groupby(["scenario", "fuel_type", "year"])["value"]
            .sum()
            .reset_index()
        )
        df_left_line = df_left_line[df_left_line["scenario"].isin(row_non_ref)]

        df_left_total = (
            df_left_line
            .groupby(["scenario", "year"])["value"]
            .sum()
            .reset_index()
        )

        for scenario in row_non_ref:
            marker = scenario_markers[scenario]

            for fuel in plot_fuel_types:
                subset = df_left_line[
                    (df_left_line["scenario"] == scenario) &
                    (df_left_line["fuel_type"] == fuel)
                ].sort_values("year")

                if not subset.empty:
                    ax_left.plot(
                        subset["year"], subset["value"],
                        marker=marker, markersize=7, linewidth=1.5,
                        color=fuel_colors[fuel],
                    )

            total_subset = df_left_total[
                df_left_total["scenario"] == scenario
            ].sort_values("year")

            if not total_subset.empty:
                ax_left.plot(
                    total_subset["year"], total_subset["value"],
                    linestyle="--", linewidth=1, marker=marker, markersize=7,
                    color=LEFT_TOTAL_COLOR,
                )

        ax_left.set_xlabel("")
        ax_left.set_ylabel("Imports (EJ)")
        ax_left.set_ylim(left_ylim)

        # --- RIGHT panel ---
        df_delta_row = df_delta[df_delta["scenario"].isin(row_non_ref)]

        for exporter in all_exporters:
            for scenario in row_non_ref:
                subset = df_delta_row[
                    (df_delta_row["exporter"] == exporter) &
                    (df_delta_row["scenario"] == scenario)
                ].sort_values("year")

                if subset.empty:
                    continue

                ax_right.plot(
                    subset["year"], subset["delta"],
                    marker=scenario_markers[scenario],
                    markersize=7, linewidth=1.5,
                    color=exporter_colors[exporter],
                )

        ax_right.axhline(0, color="grey", linewidth=1, linestyle="--")
        ax_right.set_xlabel("")
        ax_right.set_ylabel("Change in imports (EJ)")
        ax_right.set_ylim(right_ylim)

    # --- Build legend handles ---

    fuel_handles = [
        Patch(facecolor=fuel_colors[f], label=f)
        for f in plot_fuel_types
        if f in fuel_colors
    ]
    total_handle = Line2D(
        [0], [0], color=LEFT_TOTAL_COLOR, linewidth=2,
        linestyle="--", label="Total"
    )

    if region_labels:
        legend_exporters = [e for e in region_labels if e in all_exporters]
        legend_exporters += [e for e in all_exporters if e not in region_labels]
    else:
        legend_exporters = all_exporters

    exporter_handles = [
        Line2D([0], [0], color=exporter_colors[e], linewidth=2, label=rlabel(e))
        for e in legend_exporters
    ]

    scenario_handles = [
        Line2D(
            [0], [0], color="black",
            marker=marker_styles[idx % len(marker_styles)],
            linestyle="-", markersize=7, linewidth=1.5,
            label=suffix,
        )
        for suffix, idx in _seen.items()
    ]

    plt.tight_layout()
    plt.subplots_adjust(left=0.08, right=0.82, bottom=0.08)

    # Estimate each legend's height in figure fraction
    # (n_items * item_height + title + padding)
    def _leg_height(n_items, fig_height_inches, item_pt=10, title_pt=14, pad_pt=10):
        total_pt = n_items * item_pt + title_pt + pad_pt
        return total_pt / (fig_height_inches * 72)

    fig_h = fig.get_size_inches()[1]
    gap = 0.02  # fraction of figure height between legends

    h1 = _leg_height(len(fuel_handles) + 1, fig_h)
    h2 = _leg_height(len(exporter_handles), fig_h)
    h3 = _leg_height(len(scenario_handles), fig_h)

    total = h1 + h2 + h3 + 2 * gap
    top = 0.5 + total / 2  # center the stack around the vertical midpoint
    top = min(top, 0.98)   # clamp so it doesn't exceed the figure

    y1 = top
    y2 = y1 - h1 - gap
    y3 = y2 - h2 - gap

    fig.legend(
        handles=fuel_handles + [total_handle],
        title="Fuel type",
        fontsize=8, title_fontsize=8,
        loc="upper left",
        bbox_to_anchor=(0.73, y1),
        frameon=True, borderpad=0.6,
    )
    fig.legend(
        handles=exporter_handles,
        title="Exporter",
        fontsize=8, title_fontsize=8,
        loc="upper left",
        bbox_to_anchor=(0.73, y2),
        frameon=True, borderpad=0.6,
    )
    fig.legend(
        handles=scenario_handles,
        title="Scenario",
        fontsize=8, title_fontsize=8,
        loc="upper left",
        bbox_to_anchor=(0.73, y3),
        frameon=True, borderpad=0.6,
    )

    plt.show()

# Run all
make_paneled_figure(df = indf[indf['model'] == 'weu_security'],
                    plot_year = 2030,
                    plot_exporter = 'R12_MEA',
                    plot_regions = ['R12_EEU', 'R12_WEU'],
                    plot_scenarios = ['REF', 'REF_MEACON1.0', 'REF_MEACON0.9', 'REF_MEACON0.8', 'REF_MEACON0.75', 'REF_MEACON0.5', 'REF_MEACON0.25',
                                      'FSU2040_MEACON1.0', 'FSU2040_MEACON0.9', 'FSU2040_MEACON0.8', 'FSU2040_MEACON0.75', 'FSU2040_MEACON0.5', 'FSU2040_MEACON0.25',
                                      'FSULONG_MEACON1.0', 'FSULONG_MEACON0.9', 'FSULONG_MEACON0.8', 'FSULONG_MEACON0.75', 'FSULONG_MEACON0.5', 'FSULONG_MEACON0.25'],
                    plot_fuel_types = ['Gas', 'Coal', 'Crude', 'Fuel Oil', 'Light Oil', 'Ethanol'],
                    region_labels = region_labels,
                    region_colors = region_colors)

make_paneled_figure(df = indf[indf['model'] == 'weu_security'],
                    plot_year = 2030,
                    plot_exporter = 'R12_NAM',
                    plot_regions = ['R12_EEU', 'R12_WEU'],
                    plot_scenarios = ['REF', 'REF_NAM15EJ', 'REF_NAM20EJ', 'REF_NAM25EJ', 'REF_NAM30EJ',
                                      'FSU2040_NAM15EJ', 'FSU2040_NAM20EJ', 'FSU2040_NAM25EJ', 'FSU2040_NAM30EJ',
                                      'FSULONG_NAM15EJ', 'FSULONG_NAM20EJ', 'FSULONG_NAM25EJ', 'FSULONG_NAM30EJ'],
                    plot_fuel_types = ['Gas', 'Coal', 'Crude', 'Fuel Oil', 'Light Oil', 'Ethanol'],
                    region_labels = region_labels,
                    region_colors = region_colors)