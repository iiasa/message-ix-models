# -*- coding: utf-8 -*-
"""
Add NAM export boost sensitivities
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

import os

def run_nam_boost_scenario(base_scenario: message_ix.Scenario,
                           bound_level_ej: float,
                           bound_technologies: list = ['LNG_shipped_exp_weu', 'LNG_shipped_exp_eeu',
                                                       #'coal_shipped_exp_weu', 'coal_shipped_exp_eeu',
                                                       #'biomass_shipped_exp_weu', 'biomass_shipped_exp_eeu',
                                                       #'eth_shipped_exp_weu', 'eth_shipped_exp_eeu',
                                                       'foil_shipped_exp_weu', 'foil_shipped_exp_eeu',
                                                       'loil_shipped_exp_weu', 'loil_shipped_exp_eeu',
                                                       'crudeoil_shipped_exp_weu', 'crudeoil_shipped_exp_eeu',
                                                       #'meth_shipped_exp_weu', 'meth_shipped_exp_eeu',
                                                       #'lh2_shipped_exp_weu', 'lh2_shipped_exp_eeu',
                                                      ],
                           bound_commodities: list = ['LNG', 'crudeoil', 'coal', 'biomass',
                                                      'ethanol', 'fueloil', 'lightoil', 'methanol', 'lh2'],
                           bound_year: int = 2030,
                           bound_exporters: list = ['R12_NAM'],
                           bound_importers: list = ['R12_EEU', 'R12_WEU'],
                           solve_scenario: bool = True) -> message_ix.Scenario:
    """Clone `base_scenario`, apply a NAM export boost, and solve.

    Args:
        base_scenario: Already-solved scenario to boost (e.g. a bilateralized or
            FSU-restricted fuel_security scenario).
        bound_level_ej: NAM export boost level, in EJ, applied as a lower bound
            on tracked imports from `bound_exporters`.
        solve_scenario: Whether to solve the resulting scenario.

    Returns:
        The cloned, boosted (and, if `solve_scenario`, solved) scenario.
    """
    bound_level = bound_level_ej * 31.71 # approximation of EJ to GWa

    target_scenario_name = f"{base_scenario.scenario}_NAM{bound_level_ej}EJ"
    target_scenario = base_scenario.clone('fuel_security', target_scenario_name, keep_solution = False)
    target_scenario.set_as_default()

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

    with target_scenario.transact("Add sets"):
        target_scenario.add_set('commodity', 'tracked_imports')
        target_scenario.add_set('level', ['imports_input', 'imports_output'])
        target_scenario.add_set('technology', 'import_tracking')

    with target_scenario.transact("Aggregate tracked imports"):
        target_scenario.add_par('output', outputdf)
        target_scenario.add_par('input', inputdf)

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

    with target_scenario.transact('Add activity bound'):
        target_scenario.add_par('bound_activity_lo', bounddf)

    # Remove activity constraints on targeted technologies
    with target_scenario.transact("Remove activity constraints on target exporters in specified year"):
         for par in ["growth_activity_up", "initial_activity_up",
                     "growth_activity_lo", "initial_activity_lo"]:
             basepar = target_scenario.par(par, filters = {"technology": bound_technologies,
             #                                              "node_loc": bound_exporters,
                                                            "year_act": bound_year})
             if len(basepar) != 0:
                 print(f"...{par}")
                 target_scenario.remove_par(par, basepar)

    # Adjust low constraints (allow steeper decline) for targeted technologies (except bound year)
    with target_scenario.transact("Adjust lo activity constraints on target tec"):
         for par in ["growth_activity_lo"]:
             basepar = target_scenario.par(par, filters = {"technology": bound_technologies})
             adjpar = basepar.copy()
             adjpar['value'] *= 4 # slack

             if len(basepar) != 0:
                 print(f"...{par}")
                 target_scenario.remove_par(par, basepar)
                 target_scenario.add_par(par, adjpar)

    if solve_scenario == True:
        target_scenario.solve(quiet = False, model = 'MESSAGE', solve_options={"scaind":"-1"})

    return target_scenario
