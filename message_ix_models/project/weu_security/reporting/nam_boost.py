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

def make_paneled_figure(
    df,
    plot_year:int,
    plot_exporter:str,
    plot_scenarios:list[str],
    plot_regions:list[str],
    plot_fuel_types:list[str],
    plot_fuel_colors:dict[str, str] = None,
    plot_all_years:list[int] = [2025, 2030, 2035, 2040, 2045, 2050],
    plot_ref_scenario:str = 'SSP2'
):
    """
    Parameters
    df : pandas.DataFrame
    plot_year : int
    plot_exporter : str
    plot_scenarios : list
    plot_regions : list
    plot_fuel_types : list
    plot_fuel_colors : dict (optional)
        Mapping {fuel: color}. If None, matplotlib default cycle is used.
    plot_ref_scenario : str
        Reference scenario to compare to.
    """
    # Clean data
    plotdf = df[df['year'].isin(plot_all_years)]
    plotdf = plotdf[plotdf["region"].isin(plot_regions)]
    plotdf = plotdf[plotdf["scenario"].isin(plot_scenarios)]
    plotdf = plotdf[plotdf["fuel_type"].isin(plot_fuel_types)]

    # Create consistent fuel color map
    if plot_fuel_colors is None:
        default_colors = plt.rcParams["axes.prop_cycle"].by_key()["color"]
        fuel_colors = {
            fuel: default_colors[i % len(default_colors)]
            for i, fuel in enumerate(plot_fuel_types)
        }
    else:
        fuel_colors = plot_fuel_colors

    # Left panel: stacked bar for a given year
    df_left = plotdf[plotdf["year"] == plot_year].copy()

    pivot_left = (
        df_left
        .groupby(["scenario", "fuel_type"])["value"]
        .sum()
        .unstack()
        .reindex(index=plot_scenarios)
        .reindex(columns=plot_fuel_types)
        .fillna(0)
    )

    # Right panel: line plot for a given exporter
    df_right = plotdf[plotdf["exporter"] == plot_exporter].copy()
    df_right = df_right.groupby(["scenario", "fuel_type", "year"])["value"].sum().reset_index()
    df_right = df_right[df_right['scenario'] != plot_ref_scenario]

    # Add totals by scenario (sum across fuel types)
    df_right_total = (
        df_right
        .groupby(["scenario", "year"])["value"]
        .sum()
        .reset_index()
    )

    # Plot figure
    fig = plt.figure(figsize=(13, 5))
    gs = fig.add_gridspec(1, 3)

    ax_left = fig.add_subplot(gs[0, 0])      # 1/3
    ax_right = fig.add_subplot(gs[0, 1:3])   # 2/3

    # Plot left panel: stacked bar
    bottom = pd.Series([0] * len(pivot_left), index=pivot_left.index)

    for fuel in plot_fuel_types:
        values = pivot_left[fuel]
        ax_left.bar(
            pivot_left.index,
            values,
            bottom=bottom,
            label=fuel,
            color=fuel_colors[fuel]
        )
        bottom += values

    ax_left.set_xlabel("Scenario")
    ax_left.set_ylabel("Total Imports (EJ)")
    ax_left.set_ylim(0, 50)
    ax_left.set_title(f"Net Fuel Imports by Scenario ({plot_year})")
    ax_left.legend(title="", bbox_to_anchor=(1.05, 1))

    # Plot right panel: line plot
    marker_styles = ["o", "s", "^", "D", "v", "P", "X"]

    for i, scenario in enumerate(plot_scenarios):
        for fuel in plot_fuel_types:

            subset = df_right[
                (df_right["scenario"] == scenario) &
                (df_right["fuel_type"] == fuel)
            ].sort_values("year")

            if not subset.empty:
                ax_right.plot(
                    subset["year"],
                    subset["value"],
                    marker=marker_styles[i % len(marker_styles)],
                    markersize=8,
                    color=fuel_colors[fuel], 
                    label=f"{scenario} – {fuel}"
                )

    # Plot total line
    # ---- Plot total line per scenario ----
    for i, scenario in enumerate(plot_scenarios):

        total_subset = df_right_total[
            df_right_total["scenario"] == scenario
        ].sort_values("year")

        if not total_subset.empty:
            ax_right.plot(
                total_subset["year"],
                total_subset["value"],
                linestyle="--",
                linewidth=2,
                marker=marker_styles[i % len(marker_styles)],
                markersize=8,
                color="black",
                label=f"{scenario} – Total"
            )

    ax_right.set_xlabel("")
    ax_right.set_ylabel("Exports (EJ)")
    ax_right.set_title(f"Fuel Exports: North America to Europe (2025-2050)")

    ax_right.legend(ncol=2)

    plt.tight_layout()
    plt.show()

make_paneled_figure(df = indf,
                    plot_year = 2030,
                    plot_exporter = 'R12_NAM',
                    plot_regions = ['R12_EEU', 'R12_WEU'],
                    plot_scenarios = ['SSP2', 'NAM500', 'NAM1000'],
                    plot_fuel_types = ['Gas', 'Coal', 'Crude', 'Fuel Oil', 'Light Oil', 'Ethanol'])