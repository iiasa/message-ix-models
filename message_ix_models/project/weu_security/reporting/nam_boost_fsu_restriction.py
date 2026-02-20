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

plot_scenarios = ['FSU2040_NAM500', ]
# Clean data
df = indf[indf['supply_type'] == 'Imports'].copy()
df = df[df['fuel_type'].isin(['Coal', 'Crude', 'Gas', 'Light Oil'])]

scenarios = 
fuel_types = sorted(df["fuel_type"].unique())

# Expecting 4 scenarios × 2 fuel types
fig, axes = plt.subplots(
    nrows=len(scenarios),
    ncols=len(fuel_types),
    figsize=(14, 18),
    sharex=True,
    sharey=True
)

# If only 1 row/col edge case
if len(scenarios) == 1:
    axes = [axes]
if len(fuel_types) == 1:
    axes = [[ax] for ax in axes]

for i, scen in enumerate(scenarios):
    for j, fuel in enumerate(fuel_types):

        ax = axes[i][j]

        subset = df[
            (df["scenario"] == scen) &
            (df["fuel_type"] == fuel)
        ]

        if subset.empty:
            ax.set_visible(False)
            continue

        # Pivot for stacked bar
        pivot = (
            subset
            .groupby(["year", "exporter"])["value"]
            .sum()
            .unstack(fill_value=0)
            .sort_index()
        )

        pivot.plot(
            kind="bar",
            stacked=True,
            ax=ax,
            legend=False
        )

        # Titles
        if i == 0:
            ax.set_title(f"{fuel}", fontsize=12)
        if j == 0:
            ax.set_ylabel(f"{scen}\nValue")
        else:
            ax.set_ylabel("")

        ax.set_xlabel("")
        ax.tick_params(axis="x", rotation=45)

# Create a single legend
handles, labels = axes[0][0].get_legend_handles_labels()
fig.legend(handles, labels, title="Exporter", bbox_to_anchor=(1.02, 0.5), loc="center left")

plt.tight_layout(rect=[0, 0, 0.9, 1])
plt.show()
