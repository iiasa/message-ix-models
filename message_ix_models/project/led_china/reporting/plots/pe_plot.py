"""Primary energy plot by region"""

import pandas as pd
from message_ix_models.util import package_data_path
from message_ix_models.tools.bilateralize.prepare_edit import load_config
import yaml
import matplotlib.pyplot as plt
import numpy as np
from message_ix_models.project.led_china.reporting.plots.utils import load_all_configs, pull_legacy_data

config, plot_config = load_all_configs()

#scenario_list = config['models_scenarios']
scenario_list = ['SSP2_Baseline']

def plot_primary_energy(indf: pd.DataFrame, region: str):
    df = indf[indf['Region'] == region]
    scenarios = sorted(df['Scenario'].unique())
    # Create 2x3 subplot layout
    fig, axes = plt.subplots(2, 3, figsize=(15, 10))
    fig.suptitle('Stacked Bar Charts by Scenario', fontsize=16, fontweight='bold')

    # Flatten axes for easier iteration
    axes = axes.flatten()

    # Get unique sources for consistent colors
    sources = df['Source'].unique()
    colors = plt.cm.tab10(np.linspace(0, 1, len(sources)))
    color_map = plot_config['resource_palette']

    # Plot each scenario
    for idx, scenario in enumerate(scenarios):
        ax = axes[idx]
        
        # Filter data for this scenario
        scenario_data = df[df['Scenario'] == scenario]
        
        # Pivot data for stacked bar chart
        pivot_data = scenario_data.pivot_table(
            index='Year', 
            columns='Source', 
            values='Value', 
            aggfunc='sum',
            fill_value=0
        )
        
        # Create stacked bar chart
        pivot_data.plot(
            kind='bar', 
            stacked=True, 
            ax=ax,
            color=[color_map[col] for col in pivot_data.columns],
            width=0.7
        )
        
        # Add labels for fossil fuel percentage on 2030 and 2060
        years_to_label = [2030, 2060]
        for year in years_to_label:
            if year in pivot_data.index:
                # Get data for this year
                year_data = scenario_data[scenario_data['Year'] == year]
                
                # Calculate total value and fossil fuel value
                total_value = year_data['Value'].sum()
                fossil_value = year_data[year_data['Fossil Indicator'] == True]['Value'].sum()
                
                if total_value > 0:
                    fossil_pct = (fossil_value / total_value) * 100
                    
                    # Find x position for this year
                    x_pos = list(pivot_data.index).index(year)
                    
                    # Add label on top of bar
                    ax.text(x_pos, total_value, f'{int(fossil_pct)}%', 
                        ha='center', va='bottom', fontsize=9, rotation=0)
    
    
        # Customize subplot
        ax.set_title(f'{scenario}', fontweight='bold')
        ax.set_xlabel('')
        ax.set_ylabel('EJ/yr')
        ax.legend(title='Source', bbox_to_anchor=(1.05, 1), loc='upper left')
        ax.grid(axis='y', alpha=0.3)
        ax.set_xticklabels(ax.get_xticklabels(), rotation=90)

    # Adjust layout to prevent overlap
    plt.tight_layout()
    plt.show()

# Run all
outdf = pull_legacy_data(scenario_list = scenario_list,
                  plot_type = 'plot_primary_energy',
                  plot_config = plot_config)
outdf['Fossil Indicator'] = outdf['Variable'].isin(plot_config['plot_primary_energy']['labels']['Fossil'])

plot_primary_energy(outdf, 'CHN')