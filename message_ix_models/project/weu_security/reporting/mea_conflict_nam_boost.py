# Paneled figure of NAM export boost vs MEA conflict

import pandas as pd
from message_ix_models.util import package_data_path
import matplotlib.pyplot as plt
import numpy as np
import yaml

# Import plot configurations
with open(package_data_path('weu_security', 'reporting', 'plot_config.yaml'), 'r') as f:
    config = yaml.safe_load(f)
region_colors = config['region_colors'] 
region_labels = config['region_labels']

# Import fuel supply data
indf = pd.read_csv(package_data_path('weu_security', 'reporting', 'fuel_supply_out.csv'))
indf = indf[indf['supply_type'].isin(['Imports', 'Exports'])]

def make_paneled_figure(
    df,
    plot_year:int,
    plot_exporter:str,
    plot_scenarios:list[str],
    plot_regions:list[str],
    plot_fuel_types:list[str],
    plot_fuel_colors:dict[str, str] = None,
    plot_all_years:list[int] = [2025, 2030, 2035, 2040, 2045, 2050],
    plot_ref_scenario:str = 'SSP2',
    region_labels:dict[str, str] = None,
    region_colors:dict[str, str] = None,
):
    """
    Parameters
    ----------
    df : pandas.DataFrame
    plot_year : int
    plot_exporter : str
    plot_scenarios : list
    plot_regions : list
    plot_fuel_types : list
    plot_fuel_colors : dict (optional)
        Mapping {fuel: color} for the LEFT panel. If None, uses Okabe-Ito.
    plot_ref_scenario : str
        Reference scenario to compare to (used in right panel delta).
    region_labels : dict (optional)
        Mapping {region_code: display_label} for legend and title text.
    region_colors : dict (optional)
        Mapping {region_code: color} for the RIGHT panel exporter lines.
    """
    from matplotlib.lines import Line2D

    # ------------------------------------------------------------------
    # Clean data
    # ------------------------------------------------------------------
    plotdf = df[df['year'].isin(plot_all_years)]
    plotdf = plotdf[plotdf["region"].isin(plot_regions)]
    plotdf = plotdf[plotdf["scenario"].isin(plot_scenarios)]
    plotdf = plotdf[plotdf["fuel_type"].isin(plot_fuel_types)]

    non_ref_scenarios = [s for s in plot_scenarios if s != plot_ref_scenario]

    # ------------------------------------------------------------------
    # Region label/color helpers  (fall back to raw code if not provided)
    # ------------------------------------------------------------------
    def rlabel(code):
        if region_labels and code in region_labels:
            return region_labels[code]
        return code

    # ------------------------------------------------------------------
    # Shared marker map: same shape per scenario across BOTH panels
    # ------------------------------------------------------------------
    marker_styles = ["o", "s", "^", "D", "v", "P", "X"]
    scenario_markers = {
        s: marker_styles[i % len(marker_styles)]
        for i, s in enumerate(non_ref_scenarios)
    }

    # ------------------------------------------------------------------
    # LEFT panel color map — fuel types, Okabe-Ito palette (colorblind-friendly)
    # ------------------------------------------------------------------
    okabe_ito = [
        "#E69F00", "#56B4E9", "#009E73", "#F0E442",
        "#0072B2", "#D55E00", "#CC79A7", "#000000",
    ]
    if plot_fuel_colors is None:
        fuel_colors = {
            fuel: okabe_ito[i % len(okabe_ito)]
            for i, fuel in enumerate(plot_fuel_types)
        }
    else:
        fuel_colors = plot_fuel_colors

    LEFT_TOTAL_COLOR = "black"

    # ------------------------------------------------------------------
    # RIGHT panel color map — exporters, region_colors if provided,
    # else Paul Tol "Bright" as fallback (colorblind-friendly)
    # ------------------------------------------------------------------
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

    # ------------------------------------------------------------------
    # Prepare LEFT panel data
    # ------------------------------------------------------------------
    df_left_line = plotdf[plotdf["exporter"] == plot_exporter].copy()
    df_left_line = (
        df_left_line
        .groupby(["scenario", "fuel_type", "year"])["value"]
        .sum()
        .reset_index()
    )
    df_left_line = df_left_line[df_left_line["scenario"] != plot_ref_scenario]

    df_left_total = (
        df_left_line
        .groupby(["scenario", "year"])["value"]
        .sum()
        .reset_index()
    )

    # ------------------------------------------------------------------
    # Prepare RIGHT panel data  (delta vs reference)
    # ------------------------------------------------------------------
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

    # ------------------------------------------------------------------
    # Build figure
    # ------------------------------------------------------------------
    fig = plt.figure(figsize=(15, 7))
    gs = fig.add_gridspec(1, 5)

    ax_left  = fig.add_subplot(gs[0, 0:2])   # 40%
    ax_right = fig.add_subplot(gs[0, 2:5])   # 60%

    # ================================================================
    # LEFT panel: fuel exports by fuel type, colored by fuel
    # ================================================================
    for scenario in non_ref_scenarios:
        marker = scenario_markers[scenario]

        for fuel in plot_fuel_types:
            subset = df_left_line[
                (df_left_line["scenario"] == scenario) &
                (df_left_line["fuel_type"] == fuel)
            ].sort_values("year")

            if not subset.empty:
                ax_left.plot(
                    subset["year"],
                    subset["value"],
                    marker=marker,
                    markersize=7,
                    linewidth=1.5,
                    color=fuel_colors[fuel],
                )

        # Dashed total line per scenario
        total_subset = df_left_total[
            df_left_total["scenario"] == scenario
        ].sort_values("year")

        if not total_subset.empty:
            ax_left.plot(
                total_subset["year"],
                total_subset["value"],
                linestyle="--",
                linewidth=2,
                marker=marker,
                markersize=7,
                color=LEFT_TOTAL_COLOR,
            )

    ax_left.set_xlabel("")
    ax_left.set_ylabel("Exports (EJ)")
    ax_left.set_title(f"Fuel Exports: {rlabel(plot_exporter)} (2025-2050)")

    # LEFT color legend: fuel types + total
    fuel_handles = [
        Line2D([0], [0], color=fuel_colors[f], linewidth=2, label=f)
        for f in plot_fuel_types
    ]
    total_handle = Line2D(
        [0], [0], color=LEFT_TOTAL_COLOR, linewidth=2,
        linestyle="--", label="Total"
    )
    ax_left.legend(
        handles=fuel_handles + [total_handle],
        title="Fuel type",
        fontsize=8,
        title_fontsize=8,
        loc="upper right",
        bbox_to_anchor=(1.0, 1.0),
    )

    # ================================================================
    # RIGHT panel: delta vs reference, colored by exporter (region)
    # ================================================================
    for exporter in all_exporters:
        for scenario in non_ref_scenarios:
            subset = df_delta[
                (df_delta["exporter"] == exporter) &
                (df_delta["scenario"] == scenario)
            ].sort_values("year")

            if subset.empty:
                continue

            ax_right.plot(
                subset["year"],
                subset["delta"],
                marker=scenario_markers[scenario],
                markersize=7,
                linewidth=1.5,
                color=exporter_colors[exporter],
            )

    # Zero reference line
    ax_right.axhline(0, color="grey", linewidth=1, linestyle="--")

    ax_right.set_xlabel("")
    ax_right.set_ylabel(f"Change vs {plot_ref_scenario} (EJ)")
    ax_right.set_title(f"Total Exports: Delta vs {plot_ref_scenario}")

    # RIGHT color legend: exporters ordered/labelled by region_labels dict
    # If region_labels is provided, use its order and only show exporters present in data.
    if region_labels:
        legend_exporters = [e for e in region_labels if e in all_exporters]
        # Append any exporters in data but missing from region_labels at the end
        legend_exporters += [e for e in all_exporters if e not in region_labels]
    else:
        legend_exporters = all_exporters

    exporter_handles = [
        Line2D([0], [0], color=exporter_colors[e], linewidth=2, label=rlabel(e))
        for e in legend_exporters
    ]
    ax_right.legend(
        handles=exporter_handles,
        title="Exporter",
        fontsize=8,
        title_fontsize=8,
        loc="upper right",
        bbox_to_anchor=(1.0, 1.0),
        frameon=True,
    )

    # ================================================================
    # SHARED shape legend — placed below the figure, centered
    # ================================================================
    scenario_handles = [
        Line2D(
            [0], [0],
            color="black",
            marker=scenario_markers[s],
            linestyle="-",
            markersize=7,
            linewidth=1.5,
            label=s,
        )
        for s in non_ref_scenarios
    ]

    fig.legend(
        handles=scenario_handles,
        title="Scenario (marker shape)",
        fontsize=8,
        title_fontsize=8,
        loc="lower center",
        ncol=len(non_ref_scenarios),
        bbox_to_anchor=(0.5, -0.08),
        frameon=True,
    )

    # Sync y-axis scale across both panels
    y_min = min(ax_left.get_ylim()[0], ax_right.get_ylim()[0])
    y_max = max(ax_left.get_ylim()[1], ax_right.get_ylim()[1])
    ax_left.set_ylim(y_min, y_max)
    ax_right.set_ylim(y_min, y_max)

    plt.tight_layout()
    plt.subplots_adjust(bottom=0.15, right=0.88)   # room for shared legend and right panel legend
    plt.show()


def make_delta_comparison_figure(
    df,
    plot_scenarios_left: list[str],
    plot_scenarios_right: list[str],
    plot_regions: list[str],
    plot_fuel_types: list[str],
    plot_all_years: list[int] = [2025, 2030, 2035, 2040, 2045, 2050],
    plot_ref_scenario: str = 'SSP2',
    region_labels: dict[str, str] = None,
    region_colors: dict[str, str] = None,
    left_title: str = None,
    right_title: str = None,
):
    """
    Two-panel figure showing delta-vs-reference line charts side by side,
    each with a different subset of scenarios.

    Parameters
    ----------
    df : pandas.DataFrame
    plot_scenarios_left : list
        Scenarios (excluding ref) to show in the LEFT panel.
    plot_scenarios_right : list
        Scenarios (excluding ref) to show in the RIGHT panel.
    plot_regions : list
        Regions to include (used as exporters in the delta chart).
    plot_fuel_types : list
        Fuel types to aggregate over.
    plot_all_years : list
        Years to include on the x-axis.
    plot_ref_scenario : str
        Reference scenario for delta calculation.
    region_labels : dict (optional)
        {region_code: display_label} for legend and axis labels.
    region_colors : dict (optional)
        {region_code: color} for exporter lines.
    left_title : str (optional)
        Override title for the left panel.
    right_title : str (optional)
        Override title for the right panel.
    """
    from matplotlib.lines import Line2D

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def rlabel(code):
        if region_labels and code in region_labels:
            return region_labels[code]
        return code

    marker_styles = ["o", "s", "^", "D", "v", "P", "X"]

    # All non-ref scenarios across both panels (for consistent marker map)
    all_non_ref = list(dict.fromkeys(
        [s for s in plot_scenarios_left  if s != plot_ref_scenario] +
        [s for s in plot_scenarios_right if s != plot_ref_scenario]
    ))
    scenario_markers = {
        s: marker_styles[i % len(marker_styles)]
        for i, s in enumerate(all_non_ref)
    }

    # ------------------------------------------------------------------
    # Color map — region_colors if provided, else Tol Bright fallback
    # ------------------------------------------------------------------
    tol_bright = [
        "#4477AA", "#EE6677", "#228833", "#CCBB44",
        "#66CCEE", "#AA3377", "#BBBBBB",
    ]

    # ------------------------------------------------------------------
    # Prepare delta data (covers all scenarios + ref)
    # ------------------------------------------------------------------
    all_scenarios = list({plot_ref_scenario} | set(plot_scenarios_left) | set(plot_scenarios_right))

    plotdf = df[df['year'].isin(plot_all_years)]
    plotdf = plotdf[plotdf["region"].isin(plot_regions)]
    plotdf = plotdf[plotdf["scenario"].isin(all_scenarios)]
    plotdf = plotdf[plotdf["fuel_type"].isin(plot_fuel_types)]

    all_exporters = sorted(
        e for e in plotdf["exporter"].unique()
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

    # Totals per exporter/scenario/year
    df_totals = (
        plotdf
        .groupby(["exporter", "scenario", "year"])["value"]
        .sum()
        .reset_index()
    )

    df_ref = (
        df_totals[df_totals["scenario"] == plot_ref_scenario]
        .rename(columns={"value": "ref_value"})
        [["exporter", "year", "ref_value"]]
    )

    df_delta = (
        df_totals[df_totals["scenario"] != plot_ref_scenario]
        .copy()
        .merge(df_ref, on=["exporter", "year"], how="left")
    )
    df_delta["delta"] = df_delta["value"] - df_delta["ref_value"]

    # Ordered legend exporters
    if region_labels:
        legend_exporters = [e for e in region_labels if e in all_exporters]
        legend_exporters += [e for e in all_exporters if e not in region_labels]
    else:
        legend_exporters = all_exporters

    # ------------------------------------------------------------------
    # FSU row groups and linestyles
    # ------------------------------------------------------------------
    # Row 0: no FSU substring, Row 1: FSU2040, Row 2: FSU2100
    fsu_groups = [
        ("no FSU",  lambda s: "FSU2040" not in s and "FSU2100" not in s, "-"),
        ("FSU2040", lambda s: "FSU2040" in s,                            "--"),
        ("FSU2100", lambda s: "FSU2100" in s,                            ":"),
    ]

    def scenario_linestyle(s):
        if "FSU2040" in s:
            return "--"
        if "FSU2100" in s:
            return ":"
        return "-"

    # ------------------------------------------------------------------
    # Build 3-row × 2-col figure
    # ------------------------------------------------------------------
    fig, axes = plt.subplots(3, 2, figsize=(15, 14))
    # axes shape: axes[row, col]  — col 0 = left scenarios, col 1 = right scenarios

    col_scenarios = [
        (0, plot_scenarios_left,  left_title  or f"Δ vs {plot_ref_scenario} — Scenario group A"),
        (1, plot_scenarios_right, right_title or f"Δ vs {plot_ref_scenario} — Scenario group B"),
    ]

    for row_idx, (fsu_label, fsu_filter, fsu_ls) in enumerate(fsu_groups):
        for col_idx, (_, col_scens, col_title) in enumerate(col_scenarios):
            ax = axes[row_idx, col_idx]

            # Filter scenarios for this cell: non-ref AND matching FSU group
            cell_scenarios = [
                s for s in col_scens
                if s != plot_ref_scenario and fsu_filter(s)
            ]

            for exporter in all_exporters:
                for scenario in cell_scenarios:
                    subset = df_delta[
                        (df_delta["exporter"] == exporter) &
                        (df_delta["scenario"] == scenario)
                    ].sort_values("year")

                    if subset.empty:
                        continue

                    ax.plot(
                        subset["year"],
                        subset["delta"],
                        marker="None",
                        linewidth=2.0,
                        linestyle=fsu_ls,
                        color=exporter_colors[exporter],
                    )

            ax.axhline(0, color="grey", linewidth=1, linestyle="--")
            ax.set_xlabel("")

            # y-label only on left column
            if col_idx == 0:
                ax.set_ylabel(f"Δ vs {plot_ref_scenario} (EJ)")
            else:
                ax.set_ylabel("")

            # FSU row label as axes title (small)
            ax.set_title(f"{fsu_label}", fontsize=9)

            # Column title as suptitle-style text above top row only
            if row_idx == 0:
                ax.text(
                    0.5, 1.10, col_title,
                    transform=ax.transAxes,
                    ha="center", va="bottom",
                    fontsize=11, fontweight="bold",
                )

    # ================================================================
    # Sync y-axis across ALL 6 panels
    # ================================================================
    all_axes = axes.flatten()
    y_min = min(ax.get_ylim()[0] for ax in all_axes)
    y_max = max(ax.get_ylim()[1] for ax in all_axes)
    for ax in all_axes:
        ax.set_ylim(y_min, y_max)

    # ================================================================
    # ONE shared exporter (color) legend — top right of figure
    # ================================================================
    exporter_handles = [
        Line2D([0], [0], color=exporter_colors[e], linewidth=2, label=rlabel(e))
        for e in legend_exporters
    ]
    axes[0, 1].legend(
        handles=exporter_handles,
        title="Exporter",
        fontsize=8,
        title_fontsize=8,
        loc="upper right",
        bbox_to_anchor=(1.0, 1.0),
        frameon=True,
    )

    # ================================================================
    # SHARED scenario legend — one entry per FSU group (linestyle only)
    # ================================================================
    fsu_legend_entries = [
        ("no FSU",  "-"),
        ("FSU2040", "--"),
        ("FSU2100", ":"),
    ]
    scenario_handles = [
        Line2D([0], [0], color="black", marker="None",
               linestyle=ls, linewidth=2.0, label=label)
        for label, ls in fsu_legend_entries
    ]

    fig.legend(
        handles=scenario_handles,
        title="FSU phase (line style)",
        fontsize=8,
        title_fontsize=8,
        loc="lower center",
        ncol=3,
        bbox_to_anchor=(0.5, -0.02),
        frameon=True,
    )

    plt.tight_layout()
    plt.subplots_adjust(bottom=0.08, top=0.93)
    plt.show()

make_delta_comparison_figure(
    df = indf,
    plot_scenarios_left=["SSP2_NAM15EJ", "SSP2_NAM20EJ", "SSP2_NAM25EJ", "SSP2_NAM30EJ",
                         "FSU2040_NAM15EJ", "FSU2040_NAM20EJ", "FSU2040_NAM25EJ", "FSU2040_NAM30EJ",
                         "FSU2100_NAM15EJ", "FSU2100_NAM20EJ", "FSU2100_NAM25EJ", "FSU2100_NAM30EJ"],
    plot_scenarios_right=["SSP2_MEACON_1.0", "SSP2_MEACON_0.9", "SSP2_MEACON_0.8", "SSP2_MEACON_0.75",
                          "SSP2_MEACON_0.5", "SSP2_MEACON_0.25",
                          "FSU2040_MEACON_1.0", "FSU2040_MEACON_0.9", "FSU2040_MEACON_0.8", "FSU2040_MEACON_0.75",
                          "FSU2040_MEACON_0.5", "FSU2040_MEACON_0.25",
                          "FSU2100_MEACON_1.0", "FSU2100_MEACON_0.9", "FSU2100_MEACON_0.8", "FSU2100_MEACON_0.75",
                          "FSU2100_MEACON_0.5", "FSU2100_MEACON_0.25"],
    plot_regions=['R12_EEU', 'R12_WEU'],
    plot_fuel_types=['Gas', 'Coal', 'Crude', 'Fuel Oil', 'Light Oil', 'Ethanol'],
    plot_ref_scenario="SSP2",
    region_labels=region_labels,
    region_colors=region_colors,
    left_title="US Trade Deal Scenarios",
    right_title="Middle East Conflict Scenarios",
)