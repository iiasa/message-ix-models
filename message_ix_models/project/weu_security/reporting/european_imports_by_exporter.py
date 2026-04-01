# Paneled figure of NAM export boost only

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
indf = indf[indf['model'] == 'weu_security'] # no reexports

def make_paneled_figure(
    df,
    plot_year:int,
    plot_exporter:str,
    plot_scenarios:list[str],
    plot_regions:list[str],
    plot_fuel_types:list[str],
    plot_fuel_colors:dict[str, str] = None,
    plot_all_years:list[int] = [2025, 2030, 2035, 2040, 2045, 2050, 2055, 2060],
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

    # Clean data
    plotdf = df[df['year'].isin(plot_all_years)]
    plotdf = plotdf[plotdf["region"].isin(plot_regions)]
    plotdf = plotdf[plotdf["scenario"].isin(plot_scenarios)]
    plotdf = plotdf[plotdf["fuel_type"].isin(plot_fuel_types)]

    non_ref_scenarios = [s for s in plot_scenarios if s != plot_ref_scenario]

    # Region label/color helpers  (fall back to raw code if not provided)
    def rlabel(code):
        if region_labels and code in region_labels:
            return region_labels[code]
        return code

    # Shared marker map: same shape per scenario across BOTH panels
    marker_styles = ["o", "s", "^", "D", "v", "P", "X"]
    scenario_markers = {
        s: marker_styles[i % len(marker_styles)]
        for i, s in enumerate(non_ref_scenarios)
    }

    # LEFT panel color map — fuel types, Okabe-Ito palette (colorblind-friendly)
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

    # Prepare LEFT panel data
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

    # Prepare RIGHT panel data  (delta vs reference)
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

    # Build figure
    fig = plt.figure(figsize=(15, 9))
    gs = fig.add_gridspec(1, 5)

    ax_left  = fig.add_subplot(gs[0, 0:2])   # 40%
    ax_right = fig.add_subplot(gs[0, 2:5])   # 60%

    # LEFT panel: fuel exports by fuel type, colored by fuel
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
    ax_left.set_title(f"Fuel Exports: {rlabel(plot_exporter)} (2025-2060)")

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

    # RIGHT panel: delta vs reference, colored by exporter (region)
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

    # SHARED shape legend — placed below the figure, centered
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
        title="Scenario",
        fontsize=8,
        title_fontsize=8,
        loc="lower center",
        ncol=5,
        bbox_to_anchor=(0.4, -0.01),
        frameon=True,
    )

    plt.tight_layout()
    plt.subplots_adjust(bottom=0.15, right=0.88)   # room for shared legend and right panel legend
    plt.show()

make_paneled_figure(df = indf[indf['model'] == 'weu_security'],
                    plot_year = 2030,
                    plot_exporter = 'R12_MEA',
                    plot_regions = ['R12_EEU', 'R12_WEU'],
                    plot_scenarios = ['SSP2', 
                                      'FSU2040_MEACON_1.0', 'FSU2040_MEACON_0.9', 'FSU2040_MEACON_0.8', 'FSU2040_MEACON_0.75', 'FSU2040_MEACON_0.5', 'FSU2040_MEACON_0.25',
                                      'FSU2100_MEACON_1.0', 'FSU2100_MEACON_0.9', 'FSU2100_MEACON_0.8', 'FSU2100_MEACON_0.75', 'FSU2100_MEACON_0.5', 'FSU2100_MEACON_0.25'],
                    plot_fuel_types = ['Gas', 'Coal', 'Crude', 'Fuel Oil', 'Light Oil', 'Ethanol'],
                    region_labels = region_labels,
                    region_colors = region_colors)

make_paneled_figure(df = indf[indf['model'] == 'weu_security'],
                    plot_year = 2030,
                    plot_exporter = 'R12_NAM',
                    plot_regions = ['R12_EEU', 'R12_WEU'],
                    plot_scenarios = ['SSP2', 'FSU2040_NAM15EJ', 'FSU2040_NAM20EJ', 'FSU2040_NAM25EJ', 'FSU2040_NAM30EJ',
                                      'FSU2100_NAM15EJ', 'FSU2100_NAM20EJ', 'FSU2100_NAM25EJ', 'FSU2100_NAM30EJ'],
                    plot_fuel_types = ['Gas', 'Coal', 'Crude', 'Fuel Oil', 'Light Oil', 'Ethanol'],
                    region_labels = region_labels,
                    region_colors = region_colors)

make_paneled_figure(df = indf[indf['model'] == 'weu_security'],
                    plot_year = 2030,
                    plot_exporter = 'R12_MEA',
                    plot_regions = ['R12_EEU', 'R12_WEU'],
                    plot_scenarios = ['SSP2',
                                      'SSP2_MEACON_1.0', 
                                      'FSU2040_MEACON_1.0', 
                                      'FSU2100_MEACON_1.0',],
                    plot_fuel_types = ['Gas', 'Coal', 'Crude', 'Fuel Oil', 'Light Oil', 'Ethanol'],
                    region_labels = region_labels,
                    region_colors = region_colors)