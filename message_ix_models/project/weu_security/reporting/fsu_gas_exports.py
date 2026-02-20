# Line graph of changes to FSU exports

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

indf_total = indf.copy()
indf_total = indf_total[indf_total['fuel_type'].isin(['Coal', 'Crude', 'Gas', 'Fuel Oil', 'Light Oil', 'Ethanol'])]
indf_total['fuel_type'] = 'Total'

def plot_reg_exports(df:pd.DataFrame, 
                     plot_fuel:str,
                     plot_ref_scenario:str,
                     plot_scenarios:list[str],
                     plot_exporters:list[str],
                     plot_years:list[int]):
    """Plot FSU exports of a given fuel."""
    plotdf = df[df['fuel_type'] == plot_fuel]
    plotdf = plotdf[plotdf['scenario'].isin(plot_scenarios + [plot_ref_scenario])]
    plotdf = plotdf[plotdf['exporter'].isin(plot_exporters)]
    plotdf = plotdf[plotdf['year'].isin(plot_years)]
    plotdf = plotdf[plotdf['supply_type'] == 'Imports']

    plotdf = plotdf.groupby(['region', 'fuel_type', 'model', 'scenario',
                            'supply_type', 'unit', 'year', 'exporter'])['value'].sum().reset_index()

    # Calculate relative change to reference scenario
    plot_keys = ["exporter", "model", "region", "supply_type", "unit", "year"]

    ref = (plotdf[plotdf["scenario"] == plot_ref_scenario]
            .set_index(plot_keys)["value"]
            .rename("ref_value"))

    plotdf = plotdf[plotdf['scenario'].isin(plot_scenarios)]
    plotdf = plotdf.set_index(plot_keys).join(ref, how="inner").reset_index()

    # Compute change vs reference
    plotdf["delta"] = plotdf["value"] - plotdf["ref_value"]

    # Total delta across regions (within scenario + year)
    total_df = (plotdf
                .groupby(["scenario", "year", "exporter"], as_index=False)["delta"]
                .sum()
                .rename(columns={"delta": "delta_total"}))

    exporters = plot_exporters
    scenarios = plot_scenarios
    regions   = plotdf["region"].unique()

    n_rows = len(exporters)
    n_cols = len(scenarios)

    fig, axes = plt.subplots(
        nrows=n_rows,
        ncols=n_cols,
        figsize=(4.5 * n_cols, 3.5 * n_rows),
        sharex=True,
        sharey=True
    )

    # Ensure axes is always 2D
    if n_rows == 1:
        axes = np.expand_dims(axes, axis=0)
    if n_cols == 1:
        axes = np.expand_dims(axes, axis=1)

    for i, exporter in enumerate(exporters):
        for j, scenario in enumerate(scenarios):

            ax = axes[i, j]

            panel_df = plotdf[
                (plotdf["exporter"] == exporter) &
                (plotdf["scenario"] == scenario)
            ]

            # ---- Region lines ----
            for region in regions:
                sub = panel_df[
                    panel_df["region"] == region
                ].sort_values("year")

                if len(sub) == 0:
                    continue

                ax.plot(
                    sub["year"],
                    sub["delta"],
                    marker="o",
                    color=region_colors.get(region, "gray"),
                    label=region
                )

            # ---- Total line ----
            total_sub = total_df[
                (total_df["scenario"] == scenario) &
                (total_df["exporter"] == exporter)
            ].sort_values("year")

            if len(total_sub) > 0:
                ax.plot(
                    total_sub["year"],
                    total_sub["delta_total"],
                    color="black",
                    linewidth=1,
                    marker="s",
                    label="Total"
                )

            ax.axhline(0, color="black", linestyle="--", linewidth=1)
            ax.grid(True, alpha=0.3)

            # Column titles (top row only)
            if i == 0:
                ax.set_title(scenario)

            # Row labels (first column only)
            if j == 0:
                ax.set_ylabel(f"{exporter}\nΔ vs {plot_ref_scenario}")

    handles, labels = axes[0, 0].get_legend_handles_labels()

    new_labels = [
        region_labels.get(lbl, lbl) if lbl != "Total" else "Total (All Regions)"
        for lbl in labels
    ]

    fig.legend(
        handles,
        new_labels,
        title="Region",
        bbox_to_anchor=(1.02, 0.5),
        loc="center left")

    fig.suptitle(
        f"{plot_fuel} Exports — Change vs {plot_ref_scenario}",
        fontsize=14,
        y=0.95)

    plt.tight_layout(rect=[0, 0, 1, 0.96])
    plt.show()

# Plot for gas
plot_reg_exports(df = indf,
                 plot_fuel = 'Gas',
                 plot_ref_scenario = 'SSP2',
                 plot_scenarios = ['FSU2040', 'FSU2100'],
                 plot_exporters = ['R12_FSU', 'R12_MEA'],
                 plot_years = [2025, 2030, 2035, 2040, 2045, 2050, 2055, 2060, 2065, 2070])

# Plot for gas
plot_reg_exports(df = indf_total,
                 plot_fuel = 'Total',
                 plot_ref_scenario = 'SSP2',
                 plot_scenarios = ['NAM500', 'NAM1000'],
                 plot_exporters = ['R12_NAM', 'R12_FSU', 'R12_MEA'],
                 plot_years = [2025, 2030, 2035, 2040, 2045, 2050])