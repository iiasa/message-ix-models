# -*- coding: utf-8 -*-
"""
Add NAM boost scenarios
"""
# Import packages
from typing import Any

import logging
import numpy as np
import pandas as pd
import ixmp
from ixmp import Platform
import message_ix
from itertools import product

# Import packages
from message_ix_models.tools.bilateralize.prepare_edit import *
from message_ix_models.tools.bilateralize.bare_to_scenario import *
from message_ix_models.tools.bilateralize.load_and_solve import *
from message_ix_models.project.gas_security.aggregate_tec import *

import os

def run_nam_boost(base_scenario_name: str,
                  out_scenario_name: str,
                  bound_level: int,
                  bound_technologies: list = ['LNG_shipped_exp_weu', 'LNG_shipped_exp_eeu',
                                              'coal_shipped_exp_weu', 'coal_shipped_exp_eeu',
                                              'biomass_shipped_exp_weu', 'biomass_shipped_exp_eeu',
                                              'eth_shipped_exp_weu', 'eth_shipped_exp_eeu',
                                              'foil_shipped_exp_weu', 'foil_shipped_exp_eeu',
                                              'loil_shipped_exp_weu', 'loil_shipped_exp_eeu',
                                              'crudeoil_shipped_exp_weu', 'crudeoil_shipped_exp_eeu',
                                              'meth_shipped_exp_weu', 'meth_shipped_exp_eeu',
                                              #'lh2_shipped_exp_weu', 'lh2_shipped_exp_eeu',
                                             ],
                  bound_commodities: list = ['LNG', 'crudeoil', 'coal', 'biomass',
                                             'ethanol', 'fueloil', 'lightoil', 'methanol', 'lh2'],
                  bound_year: str = 2030,
                  bound_exporters: list = ['R12_NAM'],
                  bound_importers: list = ['R12_EEU', 'R12_WEU'],
                  solve_scenario: bool = True):
    
    # Import scenario and models
    config, config_path = load_config(project_name = 'gas_security', config_name = 'config.yaml')

    mp = ixmp.Platform()
    base_scenario = message_ix.Scenario(mp, model = 'gas_security', scenario = base_scenario_name)
    out_scenario = base_scenario.clone(model = 'gas_security', scenario = out_scenario_name, keep_solution = False)

    # Aggregate imports for bounds
    outputdf = base_scenario.par('output')
    outputdf = outputdf[outputdf['technology'].isin(bound_technologies)]
    outputdf = outputdf[outputdf['node_loc'].isin(bound_exporters)]
    outputdf['node_dest'] = "R12_GLB"
    outputdf['commodity'] = "tracked_imports"
    outputdf['level'] = "imports_input"
    outputdf['unit'] = "GWa"

    inputdf = message_ix.make_df(
                "input",
                node_origin = "R12_GLB",
                node_loc = "R12_GLB",
                technology = "import_tracking",
                commodity = "tracked_imports",
                level = "imports_input",
                unit = "GWa",
                year_vtg = outputdf['year_vtg'].unique(),
                year_act = outputdf['year_act'].unique(),
                mode = "M1",
                time = "year", time_origin = "year",
                value = 1)
    
    outputdf2 = message_ix.make_df(
                    "output",
                    node_dest = "R12_GLB",
                    node_loc = "R12_GLB",
                    technology = "import_tracking",
                    commodity = "tracked_imports",
                    level = "imports_output",
                    unit = "GWa",
                    year_vtg = outputdf['year_vtg'].unique(),
                    year_act = outputdf['year_act'].unique(),
                    mode = "M1",
                    time = "year", time_dest = "year",
                    value = 1)

    outputdf = pd.concat([outputdf, outputdf2])

    with out_scenario.transact("Add sets"):
        out_scenario.add_set('commodity', 'tracked_imports')
        out_scenario.add_set('level', ['imports_input', 'imports_output'])
        out_scenario.add_set('technology', 'import_tracking')

    with out_scenario.transact("Aggregate tracked imports"):
        out_scenario.add_par('output', outputdf)
        out_scenario.add_par('input', inputdf)
        
    # Add activity bounds
    bounddf = message_ix.make_df(
                "bound_activity_lo",
                node_loc = "R12_GLB",
                technology = "import_tracking",
                value = bound_level,
                year_act = bound_year,
                mode = 'M1',
                time = 'year',
                unit = 'GWa')
    
    with out_scenario.transact('Add activity bound'):
        out_scenario.add_par('bound_activity_lo', bounddf)

    # Remove activity constraints on targeted technologies
    with out_scenario.transact("Remove up activity constraints on target exporters in specified year"):
         for par in ["growth_activity_up", "initial_activity_up"]:
             basepar = out_scenario.par(par, filters = {"technology": bound_technologies,
                                                        "node_loc": bound_exporters,
                                                        "year_act": bound_year})
             if len(basepar) != 0:
                 print(f"...{par}")
                 out_scenario.remove_par(par, basepar)
                 
    # Adjust low constraints (allow steeper decline) for targeted technologies
    with out_scenario.transact("Adjust lo activity constraints on target tec"):
         for par in ["growth_activity_lo"]:
             basepar = out_scenario.par(par, filters = {"technology": bound_technologies})
             adjpar = basepar.copy()
             adjpar['value'] *= 4 # slack
             
             if len(basepar) != 0:
                 print(f"...{par}")
                 out_scenario.remove_par(par, basepar)
                 out_scenario.add_par(par, adjpar)

    # Remove hi and lo activity constraints for non-targeted exporters for targeted tec
    #with out_scenario.transact("Remove activity constraints on target tec for non-target exporters"):
    #     for par in ["growth_activity_up", "initial_activity_up"]:
    #         basepar = out_scenario.par(par, filters = {"technology": bound_technologies})
    #         basepar = basepar[basepar['node_loc'].isin(bound_exporters) == False]
    ##         
     #        if len(basepar) != 0:
     #            print(f"...{par}")
     #            out_scenario.remove_par(par, basepar)
                 
    # Solve scenario             
    if solve_scenario == True:
        out_scenario.solve(quiet = False)

    mp.close_db()

# Run scenarios
run_nam_boost(base_scenario_name = 'SSP2',
              out_scenario_name = 'NAM1000',
              bound_level = 1000)
run_nam_boost(base_scenario_name = 'SSP2',
              out_scenario_name = 'NAM2500',
              bound_level = 2500)

run_nam_boost(base_scenario_name = 'FSU2040',
              out_scenario_name = 'FSU2040_NAM1000',
              bound_level = 1000)
run_nam_boost(base_scenario_name = 'FSU2040',
              out_scenario_name = 'FSU2040_NAM2500',
              bound_level = 2500)

run_nam_boost(base_scenario_name = 'FSU2100',
              out_scenario_name = 'FSU2100_NAM1000',
              bound_level = 1000)
run_nam_boost(base_scenario_name = 'FSU2100',
              out_scenario_name = 'FSU2100_NAM2500',
              bound_level = 2500)