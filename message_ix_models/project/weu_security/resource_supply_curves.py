# -*- coding: utf-8 -*-
"""
Add explicit re-exports
"""
from numpy._core.numerictypes import str_
from message_ix_models.tools.bilateralize.prepare_edit import *
from message_ix_models.tools.bilateralize.bare_to_scenario import *
from message_ix_models.tools.bilateralize.load_and_solve import *
import ixmp
import pandas as pd
import matplotlib.pyplot as plt
import message_ix
import numpy as np
from message_ix_models.util import package_data_path

def plot_rsc(rsc_df:pd.DataFrame):
    """Plot resource supply curves"""
    linestyle = {'Base': '-', 'Adjusted': '--'}

    fuels = rsc_df['fuel'].unique()
    n_fuels = len(fuels)

    # Assign a consistent color per technology across all panels
    technologies = rsc_df['technology'].unique()
    prop_cycle_colors = plt.rcParams['axes.prop_cycle'].by_key()['color']
    color_map = {tech: prop_cycle_colors[i % len(prop_cycle_colors)] 
                for i, tech in enumerate(technologies)}

    fig, axes = plt.subplots(1, n_fuels, figsize=(6 * n_fuels, 5), sharey=True)
    if n_fuels == 1:
        axes = [axes]

    for ax, fuel in zip(axes, fuels):
        fuel_df = rsc_df[rsc_df['fuel'] == fuel]
        for (tech, status), group in fuel_df.groupby(['technology', 'status']):
            tech_data = group.groupby('year_vtg')['value'].sum().reset_index()
            ax.plot(tech_data['year_vtg'], tech_data['value'],
                    marker='o',
                    color=color_map[tech],
                    linestyle=linestyle.get(status, '-'))
        ax.set_title(fuel)
        ax.set_xlabel('Year')
        ax.set_ylabel('Value')
        ax.grid(True, alpha=0.3)

    # Legend 1: color by technology
    tech_handles = [
        plt.Line2D([0], [0], color=color_map[tech], marker='o', label=tech)
        for tech in technologies
    ]
    # Legend 2: dash by status
    status_handles = [
        plt.Line2D([0], [0], color='gray', linestyle=ls, label=status)
        for status, ls in linestyle.items()
    ]

    axes[-1].legend(handles=tech_handles, title='Technology', loc='upper left')
    fig.legend(handles=status_handles, title='Status', loc='lower right', bbox_to_anchor=(1, 0))

    fig.suptitle('Value by Technology Over Time', y=1.02)
    plt.tight_layout()
    plt.show()

    plt.savefig(package_data_path("weu_security", "resource_supply_curves", "updated_inv_costs_rsc.png"))


def adjust_resource_supply_curves(base_scenario_name:str,
                                  adjustment_factors:dict,
                                  adjustment_regions:list,
                                  scenario_addition:str):
    """Adjust resource supply (technology) curves"""

    # Import scenario and models
    mp = ixmp.Platform()
    base_scenario = message_ix.Scenario(mp, model = 'weu_security', scenario = base_scenario_name)
    out_scenario_name = f"{base_scenario_name}_{scenario_addition}"
    out_scenario = base_scenario.clone(model = 'weu_security', scenario = out_scenario_name, keep_solution = False)

    # Adjust resource supply curves
    for p in ['var_cost', 'inv_cost']:
        rsc_in = out_scenario.par(p, filters = {'technology': ['gas_extr_1', 'gas_extr_2', 'gas_extr_3',
                                                                       'gas_extr_4', 'gas_extr_5', 'gas_extr_6',
                                                                       'gas_extr_7', 'oil_extr_1', 'oil_extr_2', 'oil_extr_3',
                                                                       'oil_extr_4', 'oil_extr_5', 'oil_extr_6', 'oil_extr_7'],
                                                          'node_loc': adjustment_regions})
        rsc_in = rsc_in[rsc_in['year_vtg'] >= 2030]
    
        rsc_out = rsc_in.copy()
        rsc_out['value'] *= rsc_out['technology'].map(adjustment_factors).fillna(1)
    
        with out_scenario.transact("update resource supply curves"):
            out_scenario.remove_par(p, rsc_in)
            out_scenario.add_par(p, rsc_out)

    #rsc_out['status'] = 'Adjusted'
    #rsc_in['status'] = 'Base'
    #rsc_full = pd.concat([rsc_in, rsc_out])
    #rsc_full['fuel'] = np.where(rsc_full['technology'].str.contains('gas'), 'Gas', 'Oil')
    
    #plot_rsc(rsc_full)

    out_scenario.solve()

    mp.close_db()

# Run functions
for scen_name in ["SSP2"]: #, "FSU2040", "FSU2040_NAM30EJ", "FSU2040_MEACON_1.0"]:
    print(f"----------Adjusting resource supply curves for {scen_name}----------")
    adjust_resource_supply_curves(base_scenario_name = scen_name,
                                adjustment_factors = {'gas_extr_1': 1.0, 'gas_extr_2': 0.7, 'gas_extr_3': 0.7, 
                                                      'gas_extr_4': 0.5, 'gas_extr_5': 0.5, 'gas_extr_6': 0.5, 'gas_extr_7': 0.5, 
                                                      'oil_extr_1': 1.0, 'oil_extr_2': 0.7, 'oil_extr_3': 0.7, 
                                                      'oil_extr_4': 0.5, 'oil_extr_5': 0.5, 'oil_extr_6': 0.5, 'oil_extr_7': 0.5},
                                adjustment_regions = ['R12_NAM'],
                                scenario_addition = "RSC_NAM")