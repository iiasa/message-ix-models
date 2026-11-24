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
from message_ix_models.project.fuel_security.lt_contracts import remove_contract_bounds

import os

def friction_dictionary(sensitivity_scenario: str,
                        friction_endyear:int):

    # Import scenario and models
    config, config_path = load_config(project_name = 'fuel_security', config_name = 'config.yaml')
    data_path = package_data_path("bilateralize")

    sens_i = config['restriction'][sensitivity_scenario]['exporters']
    sens_j = config['restriction'][sensitivity_scenario]['importers']
    sens_techs = config['restriction'][sensitivity_scenario]['technologies']

    base_years = [2030, 2035, 2040, 2045, 2050, 2055,
                  2060, 2070, 2080, 2090, 2100, 2110]
    fric_years = [y for y in base_years if y <= friction_endyear]
    
    basedf = pd.DataFrame(product(sens_i, sens_techs,
                                  fric_years,
                                  ["M1"],
                                  ["year"]))
    basedf.columns = ['node_loc', 'technology', 'year_act', 'mode', 'time']

    bound_out = message_ix.make_df(
        "bound_activity_up",
        node_loc = basedf['node_loc'],
        technology = basedf['technology'],
        value = 0,
        year_act = basedf['year_act'],
        mode = basedf['mode'],
        time = basedf['time'],
        unit = '-')

    return bound_out

def relax_lng_growth_bounds(scenario: message_ix.Scenario,
                            sensitivity_scenario: str,
                            friction_endyear: int,
                            growth: float | None,
                            initial: float | None):
    """Loosen LNG growth bounds into the restricted importers.

    Applies to LNG export routes into the importers from exporters outside the
    restriction (neither restricted exporters nor the importers themselves), and to
    LNG_shipped_imp at the importers. Existing growth_activity_up and
    initial_activity_up rows up to friction_endyear are raised to `growth` and
    `initial`. Bounds that are already looser are left alone, and routes with no
    bound stay unbounded.
    """
    config, config_path = load_config(project_name = 'fuel_security', config_name = 'config.yaml')
    sens_i = config['restriction'][sensitivity_scenario]['exporters']
    sens_j = config['restriction'][sensitivity_scenario]['importers']

    lng_exp = [f"LNG_shipped_exp_{j.split('_')[-1].lower()}" for j in sens_j]

    with scenario.transact("Relax LNG growth bounds into restricted importers"):
        for par, value in [("growth_activity_up", growth), ("initial_activity_up", initial)]:
            if value is None:
                continue
            exp = scenario.par(par, filters = {"technology": lng_exp})
            exp = exp[~exp['node_loc'].isin(sens_i + sens_j)]
            imp = scenario.par(par, filters = {"technology": ["LNG_shipped_imp"],
                                               "node_loc": sens_j})
            df = pd.concat([exp, imp])
            df = df[(df['year_act'].astype(int) <= friction_endyear) & (df['value'] < value)]
            if len(df) == 0:
                continue
            df['value'] = value
            print(f"...{par}: {len(df)} rows raised to {value}")
            scenario.add_par(par, df)

def relax_lng_decline_bounds(scenario: message_ix.Scenario,
                             sensitivity_scenario: str,
                             friction_endyear: int,
                             decline: float,
                             first_shock_year: int = 2030):
    """Loosen the decline floor on LNG_shipped_imp at the restricted importers.

    growth_activity_lo ties each period's imports to the period before, so extra
    LNG imported in the first shock year raises the minimum imports in later
    periods. This sets growth_activity_lo to `decline` (a negative annual rate,
    for example -0.3) for years after `first_shock_year` up to and including the
    first period after friction_endyear. The first shock year keeps its floor, which
    is set from history. Floors that are already looser are left alone.
    """
    if decline >= 0:
        raise ValueError(f"decline must be a negative annual rate, got {decline}")

    config, config_path = load_config(project_name = 'fuel_security', config_name = 'config.yaml')
    sens_j = config['restriction'][sensitivity_scenario]['importers']

    df = scenario.par("growth_activity_lo", filters = {"technology": ["LNG_shipped_imp"],
                                                       "node_loc": sens_j})
    if len(df) == 0:
        return
    years = sorted(df['year_act'].astype(int).unique())
    later = [y for y in years if y > friction_endyear]
    last_year = later[0] if later else friction_endyear

    yr = df['year_act'].astype(int)
    df = df[(yr > first_shock_year) & (yr <= last_year) & (df['value'] > decline)]
    if len(df) == 0:
        return
    df['value'] = decline

    with scenario.transact("Relax LNG import decline floor at restricted importers"):
        print(f"...growth_activity_lo: {len(df)} rows lowered to {decline}")
        scenario.add_par("growth_activity_lo", df)

def run_friction_scenario(base_scenario: message_ix.Scenario,
                          friction_endyear = 2110,
                          solve_scenario = True,
                          relax_lng_growth: float | None = None,
                          relax_lng_initial: float | None = None,
                          relax_lng_decline: float | None = None):
    """Restrict FSU gas exports to WEU/EEU from 2030 to friction_endyear.

    relax_lng_growth / relax_lng_initial: if either is given, LNG routes into
    WEU/EEU from other exporters get looser growth_activity_up (annual rate, for
    example 0.2) and/or initial_activity_up (GWa) over the same years (see
    relax_lng_growth_bounds). The output scenario name then ends in "_LNGrelax".

    relax_lng_decline: if given (negative annual rate, for example -0.3), the
    decline floor on WEU/EEU LNG_shipped_imp is loosened after 2030, through the
    first period after friction_endyear (see relax_lng_decline_bounds). The output
    scenario name then ends in "_LNGfloor".
    """

    # Import scenario and models
    config, config_path = load_config(project_name = 'fuel_security', config_name = 'config.yaml')

    # Build dictionary
    bound_out = friction_dictionary("FSU", friction_endyear)

    relax = relax_lng_growth is not None or relax_lng_initial is not None

    target_scenario_name = base_scenario.scenario.replace("_bilateral", "") + "_FSU" + str(friction_endyear)
    if relax:
        target_scenario_name += "_LNGrelax"
    if relax_lng_decline is not None:
        target_scenario_name += "_LNGfloor"
    target_scenario = base_scenario.clone('fuel_security', target_scenario_name, keep_solution = False)
    target_scenario.set_as_default()

    with target_scenario.transact(f"Add friction sensitivity"):
        target_scenario.add_par('bound_activity_up', bound_out)

    # Shock supersedes long-term contract bounds on restricted technologies
    remove_contract_bounds(target_scenario, bound_out)

    with target_scenario.transact("Remove constraints on shocked technologies"):
        for par in ["growth_activity_lo", "growth_activity_up", "initial_activity_lo", "initial_activity_up"]:
            basepar = target_scenario.par(par, filters = {"technology": bound_out['technology'],
                                                          "node_loc": bound_out['node_loc']})
            if len(basepar) != 0:
                print(f"...{par}")
                target_scenario.remove_par(par, basepar)

    if relax:
        relax_lng_growth_bounds(target_scenario, "FSU", friction_endyear,
                                growth = relax_lng_growth,
                                initial = relax_lng_initial)

    if relax_lng_decline is not None:
        relax_lng_decline_bounds(target_scenario, "FSU", friction_endyear,
                                 decline = relax_lng_decline)

    if solve_scenario == True:
        target_scenario.solve(quiet=False, model="MESSAGE",
                              solve_options={"lpmethod": "4", "scaind": "1", "solutiontype": "1", "threads": "8",
                                             "numericalemphasis":"1", "baralg":"1"},
                              gams_args=["--foresight=15", "--clean_tol=1e-10", "--myopic_fix_tol=1e-3"],
                              equ_list=["COMMODITY_BALANCE_GT"],
                              check_solution = False)
