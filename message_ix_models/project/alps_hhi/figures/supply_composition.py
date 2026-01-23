# Figure: Gas supply composition

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from itertools import cycle
from message_ix_models.util import broadcast, package_data_path

df = pd.read_csv(package_data_path('alps_hhi', 'reporting', 'reporting.csv'))

plot_scenario = 'SSP2'
plot_regions = ['R12_WEU', 'R12_EEU']

# Filters
plotdf = df[df['scenario'] == plot_scenario]
plotdf = plotdf[plotdf['region'].isin(plot_regions)]

plotdf = plotdf.groupby(['year', 'exporter', 'fuel_type'])['value'].sum().reset_index()

# Get unique values
years = sorted(plotdf['year'].unique())
exporters = sorted(plotdf['exporter'].unique())
fuel_types = sorted(plotdf['fuel_type'].unique())

# Define color palette for exporters
colors = plt.cm.tab10(np.linspace(0, 1, len(exporters)))
exporter_colors = dict(zip(exporters, colors))

# Define hatch patterns for fuel types
hatches = ['/', '\\', '|', '-', '+', 'x', 'o', 'O', '.', '*']
hatch_cycle = cycle(hatches)
fuel_hatches = {ft: next(hatch_cycle) for ft in fuel_types}

# Create figure
fig, ax = plt.subplots(figsize=(12, 6))

# Width of each bar and spacing
bar_width = 0.8
x_positions = np.arange(len(years))

# Track bottom positions for stacking
bottom_dict = {year: 0 for year in years}

# Plot bars for each exporter-fuel_type combination
for exporter in exporters:
    for fuel_type in fuel_types:
        # Filter data for this combination
        subset = grouped[(grouped['exporter'] == exporter) & 
                        (grouped['fuel_type'] == fuel_type)]
        
        if len(subset) == 0:
            continue
        
        # Create values array aligned with years
        values = []
        for year in years:
            year_data = subset[subset['year'] == year]
            values.append(year_data['value'].sum() if len(year_data) > 0 else 0)
        
        # Get bottom positions for this year
        bottoms = [bottom_dict[year] for year in years]
        
        # Plot the bars
        ax.bar(x_positions, values, bar_width,
               bottom=bottoms,
               color=exporter_colors[exporter],
               hatch=fuel_hatches[fuel_type],
               edgecolor='black',
               linewidth=0.5,
               label=f'{exporter} - {fuel_type}')
        
        # Update bottom positions
        for i, year in enumerate(years):
            bottom_dict[year] += values[i]

# Customize the plot
ax.set_xlabel('Year', fontsize=12)
ax.set_ylabel('Value', fontsize=12)
ax.set_title('Stacked Bar Chart by Year, Exporter, and Fuel Type', fontsize=14)
ax.set_xticks(x_positions)
ax.set_xticklabels(years, rotation=45)
ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=8)
ax.grid(axis='y', alpha=0.3)

plt.tight_layout()
plt.show()