# Gas imports to Europe

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
df = pd.read_csv(package_data_path('weu_security', 'reporting', 'fuel_supply_out.csv'))
df['exporter'] = np.where(df['supply_type'] == 'Exports', df['region'], df['exporter'])

def plot_eur_supply(df:pd.DataFrame,
                    plot_fuel:str,
                    plot_scenarios:list[str],
                    plot_regions:list[str],
                    plot_years:list[int]):
    """Plot supply to Europe."""

    plotdf = df[df['fuel_type'].isin(plot_fuel)]
    plotdf = plotdf[plotdf['scenario'].isin(plot_scenarios)]
    plotdf = plotdf[plotdf['region'].isin(plot_regions)]
    plotdf = plotdf[plotdf['year'].isin(plot_years)]

    scenario_order = plot_scenarios

    # Stacked bars: sum over supply_type
    bars = (plotdf
            .groupby(["scenario", "year", "exporter"], as_index=False)["value"]
            .sum()
            .reset_index())

    # Line: total over exporters AND supply_type
    total = (plotdf
            .groupby(["scenario", "year"], as_index=False)["value"]
            .sum()
            .reset_index())

    bars_pivot = (
        bars
        .pivot_table(
            index=["scenario", "year"],
            columns="exporter",
            values="value",
            fill_value=0
        )
    )

    region_order = list(region_colors.keys())

    bars_pivot = (
        bars_pivot
        .reindex(columns=region_order)
    )


    scenarios = scenario_order

    ncols = min(3, len(scenarios))
    nrows = int(np.ceil(len(scenarios) / ncols))

    fig, axes = plt.subplots(
        nrows=nrows,
        ncols=ncols,
        figsize=(5 * ncols, 4 * nrows),
        sharey=True
    )

    axes = np.array(axes).flatten()

    for ax, scenario in zip(axes, scenarios):
        data = bars_pivot.loc[scenario]
        years = data.index

        bottom_pos = np.zeros(len(years))
        bottom_neg = np.zeros(len(years))

        # stacked bars by exporter — positive and negative separately
        for exporter in data.columns:
            values = data[exporter].values

            pos_vals = np.where(values > 0, values, 0)
            neg_vals = np.where(values < 0, values, 0)

            if pos_vals.any():
                ax.bar(
                    years,
                    pos_vals,
                    bottom=bottom_pos,
                    label=exporter,
                    width=2,
                    color=region_colors.get(exporter, "#cccccc")
                )
                bottom_pos += pos_vals

            if neg_vals.any():
                ax.bar(
                    years,
                    neg_vals,
                    bottom=bottom_neg,
                    label=exporter if not pos_vals.any() else "_nolegend_",
                    width=2,
                    color=region_colors.get(exporter, "#cccccc")
                )
                bottom_neg += neg_vals

        # zero line for reference
        ax.axhline(0, color="black", linewidth=0.8, linestyle="--", alpha=0.5)

        # total line (black)
        total_s = total[total["scenario"] == scenario].set_index("year")
        ax.plot(
            total_s.index,
            total_s["value"],
            color="black",
            linewidth=2,
            marker="o"
        )

        ax.set_title(scenario)
        ax.set_xlabel("")
        ax.set_ylabel("EJ")

    # remove unused panels
    for i in range(len(scenarios), len(axes)):
        fig.delaxes(axes[i])

    handles, labels = axes[0].get_legend_handles_labels()

    # map codes → nice names
    updated_labels = [region_labels.get(l, l) for l in labels]
    fig.legend(
        handles,
        updated_labels,
        title="Exporter",
        loc="lower center",
        ncol=min(6, len(updated_labels)),
        frameon=False
    )

    plt.tight_layout(rect=[0, 0.15, 1, 1])
    plt.show()

plot_eur_supply(df = df,
                plot_fuel = ["Gas"],
                plot_scenarios = ["SSP2", "FSU2040", "FSU2100"],
                plot_regions = ["R12_WEU", "R12_EEU"],
                plot_years = [2025, 2030, 2035, 2040, 2045, 2050, 2055, 2060])


plot_eur_supply(df = df,
                plot_fuel = ["Light Oil"],
                plot_scenarios = ["SSP2", "FSU2040", "FSU2100"],
                plot_regions = ["R12_WEU", "R12_EEU"],
                plot_years = [2025, 2030, 2035, 2040, 2045, 2050, 2055, 2060])
