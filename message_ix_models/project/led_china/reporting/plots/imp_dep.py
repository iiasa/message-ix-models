"""Primary energy import dependence plot by region"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from message_ix_models.project.led_china.reporting.plots.utils import load_all_configs, pull_legacy_data, calc_net_imports

config, plot_config = load_all_configs()

#scenario_list = config['models_scenarios']
scenario_list = ['SSP2_Baseline']

pe_df = pull_legacy_data(scenario_list = scenario_list,
                         plot_type = 'plot_primary_energy',
                         plot_config = plot_config)

nim_df = calc_net_imports(scenario_list = scenario_list,
                          plot_config = plot_config)
nim_df = nim_df.groupby(['Model', 'Region', 'Scenario', 'Unit',
                        'Year', 'Source'])[['Net Imports', 'Gross Imports']].sum().reset_index()

imdep_df = pe_df.merge(nim_df,
                       on = ['Model', 'Region', 'Scenario', 'Unit',
                             'Year', 'Source'],
                       how = 'left')                   
imdep_df = imdep_df.rename(columns = {'Value': 'Primary Energy'})
imdep_df['Net Imports'] = imdep_df['Net Imports'].fillna(0)
imdep_df['Net Imports'] = imdep_df['Net Imports'].fillna(0)

imdep_df['Import Dependence'] = imdep_df['Net Imports'] / imdep_df['Primary Energy']
imdep_df['Import Dependence'] = imdep_df['Import Dependence'].fillna(0)

def plot_import_dependence(df:pd.DataFrame, region:str):
    
    df = df[df['Region'] == region]
    scenarios = sorted(df['Scenario'].unique())
    sources = ['Coal', 'Gas', 'Oil']

    # Create 2x3 subplot layout
    fig, axes = plt.subplots(2, 3, figsize=(15, 10))
    fig.suptitle('Stacked Bar Charts by Scenario', fontsize=16, fontweight='bold')

    # Define colors for scenarios
    #colors = plot_config['resource_palette']

    # Top row - Imports
    for idx, source in enumerate(sources):
        ax = axes[0, idx]
        source_data = df[df['Source'] == source]
    
        for scenario in scenarios:
            scenario_data = source_data[source_data['Scenario'] == scenario]
            ax.plot(scenario_data['Year'], scenario_data['Net Imports'], 
                    label=scenario, marker='o', linewidth=2) # add color=colors[scenario]
    
        ax.set_title(f'{source}', fontsize=12, fontweight='bold')
        ax.set_xlabel('Year')
        ax.set_ylabel('Imports')
        ax.grid(True, alpha=0.3)
        if idx == 2:  # Add legend only to the last panel
            ax.legend(loc='best')

    # Bottom row - Import Dependence
    for idx, source in enumerate(sources):
        ax = axes[1, idx]
        source_data = df[df['Source'] == source]
        
        for scenario in scenarios:
            scenario_data = source_data[source_data['Scenario'] == scenario]
            ax.plot(scenario_data['Year'], scenario_data['Import Dependence'], 
                    label=scenario, marker='o', linewidth=2) # add color=colors[scenario]
        
        ax.set_xlabel('Year')
        ax.set_ylabel('Import Dependence')
        ax.grid(True, alpha=0.3)
        if idx == 2:  # Add legend only to the last panel
            ax.legend(loc='best')

    plt.tight_layout()
    plt.show()