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

def plot_figure(
    df,
    plot_exporter: str,
    plot_scenarios: list[str],
    plot_regions: list[str],
    plot_fuel_types: list[str],
    plot_fuel_colors: dict[str, str] = None,
    plot_all_years: list[int] = [2025, 2030, 2035, 2040, 2045, 2050, 2055, 2060],
    region_labels: dict[str, str] = None,
):
    from matplotlib.lines import Line2D

    # Clean data
    plotdf = df[df['year'].isin(plot_all_years)]
    plotdf = plotdf[plotdf["region"].isin(plot_regions)]
    plotdf = plotdf[plotdf["scenario"].isin(plot_scenarios)]
    plotdf = plotdf[plotdf["fuel_type"].isin(plot_fuel_types)]

    # Helpers
    def rlabel(code):
        if region_labels and code in region_labels:
            return region_labels[code]
        return code

    marker_styles = ["o", "s", "^", "D", "v", "P", "X"]
    scenario_markers = {
        s: marker_styles[i % len(marker_styles)]
        for i, s in enumerate(plot_scenarios)
    }

    okabe_ito = [
        "#E69F00", "#56B4E9", "#009E73", "#F0E442",
        "#0072B2", "#D55E00", "#CC79A7", "#000000",
    ]
    fuel_colors = plot_fuel_colors or {
        fuel: okabe_ito[i % len(okabe_ito)]
        for i, fuel in enumerate(plot_fuel_types)
    }
    LEFT_TOTAL_COLOR = "black"

    # Prepare data
    df_line = (
        plotdf[plotdf["exporter"] == plot_exporter]
        .groupby(["scenario", "fuel_type", "year"])["value"]
        .sum()
        .reset_index()
    )

    df_total = (
        df_line
        .groupby(["scenario", "year"])["value"]
        .sum()
        .reset_index()
    )

    # Plot
    fig, ax = plt.subplots(figsize=(7, 7))

    for scenario in plot_scenarios:
        marker = scenario_markers[scenario]

        for fuel in plot_fuel_types:
            subset = df_line[
                (df_line["scenario"] == scenario) &
                (df_line["fuel_type"] == fuel)
            ].sort_values("year")

            if not subset.empty:
                ax.plot(
                    subset["year"], subset["value"],
                    marker=marker, markersize=7, linewidth=1.5,
                    color=fuel_colors[fuel],
                )

        total_subset = df_total[df_total["scenario"] == scenario].sort_values("year")
        if not total_subset.empty:
            ax.plot(
                total_subset["year"], total_subset["value"],
                linestyle="--", linewidth=2,
                marker=marker, markersize=7,
                color=LEFT_TOTAL_COLOR,
            )

    ax.set_xlabel("")
    ax.set_ylabel("Exports (EJ)")
    ax.set_title(f"Fuel Exports: {rlabel(plot_exporter)} (2025-2060)")

    # Fuel type + total legend
    fuel_handles = [
        Line2D([0], [0], color=fuel_colors[f], linewidth=2, label=f)
        for f in plot_fuel_types
    ]
    total_handle = Line2D([0], [0], color=LEFT_TOTAL_COLOR, linewidth=2, linestyle="--", label="Total")
    ax.legend(
        handles=fuel_handles + [total_handle],
        title="Fuel type", fontsize=8, title_fontsize=8,
        loc="upper right",
    )

    # Scenario shape legend below
    scenario_handles = [
        Line2D([0], [0], color="black", marker=scenario_markers[s],
               linestyle="-", markersize=7, linewidth=1.5, label=s)
        for s in plot_scenarios
    ]
    fig.legend(
        handles=scenario_handles,
        title="Scenario", fontsize=8, title_fontsize=8,
        loc="lower center", ncol=5,
        bbox_to_anchor=(0.5, -0.01), frameon=True,
    )

    plt.tight_layout()
    plt.subplots_adjust(bottom=0.15)
    plt.show()

plot_figure(df = indf[indf['model'] == 'weu_security'],
            plot_exporter = 'R12_NAM',
            plot_scenarios = ['SSP2', 'SSP2_NAM30EJ', 'SSP2_NAM30EJ_RSC_NAM'],
            plot_regions = ['R12_EEU', 'R12_WEU'],
            plot_fuel_types = ['Gas', 'Coal', 'Crude', 'Fuel Oil', 'Light Oil', 'Ethanol'],
            region_labels = region_labels)