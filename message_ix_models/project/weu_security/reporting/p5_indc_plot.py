# Gas imports to Europe

import pandas as pd
from message_ix_models.util import package_data_path
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import yaml
import matplotlib.lines as mlines

# Import plot configurations
with open(package_data_path('weu_security', 'reporting', 'plot_config.yaml'), 'r') as f:
    config = yaml.safe_load(f)
region_colors = config['region_colors'] 
region_labels = config['region_labels']

# Import fuel supply data
df = pd.read_csv(package_data_path('weu_security', 'reporting', 'fuel_supply_out.csv'))
df = df[df['model'] == 'weu_security']
df = df[['scenario','region', 'fuel_type', 'supply_type', 'exporter','unit', 'variable', 'year', 'value']]

plotdf = df.copy()
plotdf = plotdf[plotdf['supply_type'].isin(['Imports'])]
plotdf = plotdf[plotdf['exporter'].isin(['R12_EEU', 'R12_WEU']) == False] # Non-European exporters

plotdf['SSP2_INDC2030'] = np.where(plotdf['scenario'].str.contains('INDC2030'), "INDC2030", "SSP2")

# Plot imports to Europe by exporter
exporter_list = ['R12_FSU', 'R12_NAM', 'R12_MEA']
scenario_list = ["SSP2", "INDC2030", 
                 "FSU2040", "FSU2100", "MEACON_1.0", "NAM30EJ",
                 "FSU2040_MEACON_1.0", "FSU2100_MEACON_1.0", "FSU2040_NAM30EJ", "FSU2100_NAM30EJ"
                 ]
                 
plotdf = plotdf[plotdf['exporter'].isin(exporter_list)]
plotdf = plotdf[plotdf['region'].isin(['R12_EEU', 'R12_WEU'])]

plotdf = plotdf.groupby(['scenario', 'exporter', 'year', "SSP2_INDC2030"])['value'].sum().reset_index()

plotdf['scenario'] = plotdf['scenario'].str.replace('SSP2_', '')
plotdf['scenario'] = plotdf['scenario'].str.replace('INDC2030_', '')

plotdf = plotdf[plotdf['scenario'].isin(scenario_list)]
plotdf = plotdf[plotdf['year'].isin(range(2020, 2061))
]
exporters_sorted = exporter_list
ssp_categories   = ["SSP2", "INDC2030"]
scenarios_sorted = sorted(plotdf["scenario"].unique())

# ── color: SSP2_INDC2030 category ───────────────────────────────────────────
ssp_palette = {
    "SSP2":    "#2166ac",
    "INDC2030":"#d6604d",
}

# ── line style: scenario (same name appears in both SSP categories) ──────────
linestyles = ["-", "--", "-.", ":"]
markers    = ["o", "s", "^", "D", "v", "P"]
scenario_ls = {s: linestyles[i % len(linestyles)] for i, s in enumerate(scenarios_sorted)}
scenario_mk = {s: markers[i % len(markers)]       for i, s in enumerate(scenarios_sorted)}

# ────────────────────────────────────────────────────────────────────────────
fig, axes = plt.subplots(1, 3, figsize=(16, 5), sharey=False)
fig.patch.set_facecolor("#f9f9f9")

for ax, exporter in zip(axes, exporters_sorted):
    sub = plotdf[plotdf["exporter"] == exporter]

    for scen in scenarios_sorted:
        for ssp_val in ssp_categories:
            s = sub[(sub["scenario"] == scen) &
                    (sub["SSP2_INDC2030"] == ssp_val)].sort_values("year")
            if s.empty:
                continue
            ax.plot(s["year"], s["value"],
                    color=ssp_palette[ssp_val],
                    linestyle=scenario_ls[scen],
                    linewidth=2,
                    marker=scenario_mk[scen], markersize=4)

    ax.set_title(exporter, fontsize=13, fontweight="bold", pad=8)
    ax.set_xlabel("Year", fontsize=10)
    ax.set_ylabel("Total Value", fontsize=10)
    ax.tick_params(labelsize=9)
    ax.spines[["top", "right"]].set_visible(False)
    ax.set_facecolor("#ffffff")
    ax.grid(axis="y", linestyle="--", linewidth=0.5, alpha=0.6)

# ── single shared legend ─────────────────────────────────────────────────────
from matplotlib.lines import Line2D

# SSP color swatches (solid black line, colored)
ssp_handles = [
    Line2D([0], [0], color=ssp_palette[s], linewidth=3, linestyle="-", label=s)
    for s in ssp_categories
]

# Scenario linestyle entries (dark gray, varied style+marker)
scen_handles = [
    Line2D([0], [0], color="#444444", linewidth=2,
           linestyle=scenario_ls[s], marker=scenario_mk[s], markersize=5, label=s)
    for s in scenarios_sorted
]

# Section header proxies (invisible)
blank = Line2D([0], [0], color="none", label="")
header_ssp  = Line2D([0], [0], color="none", label="$\\bf{SSP2\\_INDC2030}$")
header_scen = Line2D([0], [0], color="none", label="$\\bf{Scenario}$")

legend_handles = [header_ssp] + ssp_handles + [blank, header_scen] + scen_handles

fig.legend(handles=legend_handles,
           loc="center left", bbox_to_anchor=(1.0, 0.5),
           fontsize=9, framealpha=0.9, handlelength=2.5)

fig.suptitle("Total Value by Exporter, Scenario & SSP2_INDC2030",
             fontsize=14, fontweight="bold")
plt.tight_layout()

plt.show()
