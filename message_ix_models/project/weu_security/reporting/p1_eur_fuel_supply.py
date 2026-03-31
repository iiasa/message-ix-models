# Gas imports to Europe

import pandas as pd
from message_ix_models.util import package_data_path
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
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
df = df[df['value'] != 0]

df['transport_type'] = 'Domestic'
df['transport_type'] = np.where(df['variable'].str.contains('Pipe'), 'Pipeline', df['transport_type'])
df['transport_type'] = np.where(df['variable'].str.contains('Shipped'), 'Shipped', df['transport_type'])

# Define hatches per transport_type — extend as needed
transport_hatches = {
    "Domestic":  "",        # solid / no hatch
    "Pipeline":      "///",     # diagonal lines
    "Shipped":     "...",     # dots
}

def plot_eur_supply(df: pd.DataFrame,
                   plot_fuel: str,
                   plot_scenarios: list[str],
                   plot_regions: list[str],
                   plot_years: list[int]):
    """Plot supply to Europe."""

    plotdf = df[df['fuel_type'].isin(plot_fuel)]
    plotdf = plotdf[plotdf['scenario'].isin(plot_scenarios)]
    plotdf = plotdf[plotdf['region'].isin(plot_regions)]
    plotdf = plotdf[plotdf['year'].isin(plot_years)]

    scenario_order = plot_scenarios

    bars = (plotdf
            .groupby(["scenario", "year", "exporter", "transport_type"], as_index=False)["value"]
            .sum())

    total = (plotdf
             .groupby(["scenario", "year"], as_index=False)["value"]
             .sum())

    bars_pivot = bars.pivot_table(
        index=["scenario", "year"],
        columns=["exporter", "transport_type"],
        values="value",
        fill_value=0
    )

    region_order = list(region_colors.keys())
    all_transport_types = bars["transport_type"].unique().tolist()

    ordered_cols = [
        (exp, tt)
        for exp in region_order
        for tt in all_transport_types
        if (exp, tt) in bars_pivot.columns
    ]
    bars_pivot = bars_pivot.reindex(columns=ordered_cols)

    scenarios = scenario_order
    ncols = min(3, len(scenarios))
    nrows = int(np.ceil(len(scenarios) / ncols))

    fig, axes = plt.subplots(
        nrows=nrows, ncols=ncols,
        figsize=(4 * ncols, 5 * nrows),
        sharey=True
    )
    axes = np.array(axes).flatten()

    # Track which exporters and transport_types actually appear in the data
    seen_exporters = []
    seen_transport_types = []

    for ax, scenario in zip(axes, scenarios):
        data = bars_pivot.loc[scenario]
        years = data.index.tolist()

        bottom_pos = np.zeros(len(years))
        bottom_neg = np.zeros(len(years))

        for (exporter, transport_type) in data.columns:
            values = data[(exporter, transport_type)].values
            color = region_colors.get(exporter, "#cccccc")
            hatch = transport_hatches.get(transport_type, "")

            pos_vals = np.where(values > 0, values, 0)
            neg_vals = np.where(values < 0, values, 0)

            bar_kwargs = dict(width=2, color=color, hatch=hatch,
                              edgecolor="white", linewidth=0.4)

            if pos_vals.any():
                ax.bar(years, pos_vals, bottom=bottom_pos, **bar_kwargs)
                bottom_pos += pos_vals

            if neg_vals.any():
                ax.bar(years, neg_vals, bottom=bottom_neg, **bar_kwargs)
                bottom_neg += neg_vals

            if exporter not in seen_exporters:
                seen_exporters.append(exporter)
            if transport_type not in seen_transport_types:
                seen_transport_types.append(transport_type)

        ax.axhline(0, color="black", linewidth=0.8, linestyle="--", alpha=0.5)

        total_s = total[total["scenario"] == scenario].set_index("year")
        ax.plot(total_s.index, total_s["value"],
                color="black", linewidth=2, marker="o")

        ax.set_title(scenario)
        ax.set_xlabel("")
        ax.set_ylabel("EJ")

    for i in range(len(scenarios), len(axes)):
        fig.delaxes(axes[i])

    # ── Legend 1: Exporter (color) ────────────────────────────────────────────
    color_handles = [
        mpatches.Patch(
            facecolor=region_colors.get(exp, "#cccccc"),
            edgecolor="white",
            label=region_labels.get(exp, exp)
        )
        for exp in seen_exporters
    ]
    legend_colors = fig.legend(
        handles=color_handles,
        title="Exporter",
        loc="lower left",
        bbox_to_anchor=(0.0, -0.05),
        ncol=min(4, len(color_handles)),
        frameon=False
    )
    fig.add_artist(legend_colors)   # keep it when adding the second legend

    # ── Legend 2: Transport type (hatch) ──────────────────────────────────────
    hatch_handles = [
        mpatches.Patch(
            facecolor="grey",       # neutral color so hatch pattern is visible
            hatch=transport_hatches.get(tt, ""),
            edgecolor="white",
            label=tt
        )
        for tt in seen_transport_types
    ]
    fig.legend(
        handles=hatch_handles,
        title="Transport type",
        loc="lower right",
        bbox_to_anchor=(1.0, 0.0),
        ncol=min(6, len(hatch_handles)),
        frameon=False
    )

    plt.tight_layout(rect=[0, 0.15, 1, 1])
    plt.show()

plot_eur_supply(df = df,
                plot_fuel = ["Gas"],
                plot_scenarios = ["SSP2", "FSU2040", "FSU2100"],
                plot_regions = ["R12_EEU", "R12_WEU"],
                plot_years = [2025, 2030, 2035, 2040, 2045, 2050, 2055, 2060])

plot_eur_supply(df = df,
                plot_fuel = ["Light Oil"],
                plot_scenarios = ["SSP2", "FSU2040", "FSU2100"],
                plot_regions = ["R12_EEU", "R12_WEU"],
                plot_years = [2030, 2035, 2040, 2045, 2050, 2055, 2060])

plot_eur_supply(df = df,
                plot_fuel = ["Gas"],
                plot_scenarios = ["SSP2", "FSU2040", "FSU2100"],
                plot_regions = ["R12_CHN"],
                plot_years = [2030, 2035, 2040, 2045, 2050, 2055, 2060])
