# -*- coding: utf-8 -*-
"""
Add friction sensitivities
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
                                            'crudeoil_shipped_exp_weu', 'crudeoil_shipped_exp_eeu'],
                  bound_commodities: list = ['LNG', 'crudeoil'],
                  bound_year: str = 2030,
                  bound_exporters: list = ['NAM'],
                  bound_importers: list = ['EEU', 'WEU'],
                  solve_scenario: bool = True):
    
    # Import scenario and models
    config, config_path = load_config(project_name = 'gas_security', config_name = 'config.yaml')

    mp = ixmp.Platform()
    base_scenario = message_ix.Scenario(mp, model = 'gas_security', scenario = base_scenario_name)
    out_scenario = base_scenario.clone(model = 'gas_security', scenario = out_scenario_name, keep_solution = False)

    # Aggregate imports for bounds
    outputdf = out_scenario.par('output')
    outputdf = outputdf[outputdf['technology'].isin(bound_technologies)]
    outputdf = outputdf[outputdf['node_loc'].isin(bound_exporters)]
    outputdf['node_dest'] = "R12_GLB"
    outputdf['commodity'] = "tracked_imports"
    outputdf['level'] = "tracked_imports"
    outputdf['unit'] = "GWa"

    inputdf = message_ix.make_df(
                "input",
                node_origin = "R12_GLB",
                node_loc = "R12_GLB",
                technology = "import_tracking",
                commodity = "tracked_imports",
                level = "imports",
                unit = "GWa",
                year_vtg = list(range(2030, 2060, 5)),
                year_act = list(range(2030, 2060, 5)),
                mode = "M1",
                time = "year", time_origin = "year",
                value = 1)
    
    outputdf2 = message_ix.make_df(
                    "output",
                    node_dest = "R12_GLB",
                    node_loc = "R12_GLB",
                    technology = "import_tracking",
                    commodity = "tracked_imports",
                    level = "imports",
                    unit = "GWa",
                    year_vtg = list(range(2030, 2060, 5)),
                    year_act = list(range(2030, 2060, 5)),
                    mode = "M1",
                    time = "year", time_dest = "year",
                    value = 1)

    outputdf = pd.concat([outputdf, outputdf2])

    with out_scenario.transact("Aggregate tracked imports"):
        out_scenario.add_par('output', outputdf)
        out_scenario.add_par('input', inputdf)
        
    # Add activity bounds
    bounddf = message_ix.make_df(
                "bound_activity_lo",
                node_loc = "R12_GLB",
                technology = "import_tracking",
                value = bound_level,
                year_act = list(outputdf['year_act'].unique()),
                mode = 'M1',
                time = 'year',
                unit = 'GWa')
    
    with out_scenario.transact('Add activity bound'):
        out_scenario.add_par('bound_activity_lo', bounddf)
                
    if solve_scenario == True:
        target_scenario.solve(quiet = False)

    mp.close_db()

# Run scenarios
run_nam_boost(base_scenario_name = 'FSU2100',
              out_scenario_name = 'FSU2100_NAM250',
              bound_level = 250)
