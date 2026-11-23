# -*- coding: utf-8 -*-
"""
Bilateralize base scenarios for gas security analysis
"""
# Import packages
from message_ix_models.tools.bilateralize.prepare_edit import *
from message_ix_models.tools.bilateralize.bare_to_scenario import *
from message_ix_models.tools.bilateralize.load_and_solve import *
from message_ix_models.project.fuel_security.liquefaction_calibration import *
from message_ix_models.project.fuel_security.adjust_reexports import *
from message_ix_models.project.fuel_security.lt_contracts import add_contract_bounds
from message_ix_models.project.fuel_security.rebase_extraction import rebase_extraction_history
from message_ix_models.project.fuel_security.shift_resource_grades import shift_nam_gas_grades

import os
import pandas as pd
from ixmp import Platform

# Clear bare files
def clean_bare_files(data_path, config):
    """
    Clear bare files for all technologies
    """
    for tec in config['covered_trade_technologies']:
        if os.path.exists(os.path.join(data_path, tec, "bare_files")):
            for file in os.listdir(os.path.join(data_path, tec, "bare_files")):
                if os.path.isfile(os.path.join(data_path, tec, "bare_files", file)):
                    os.remove(os.path.join(data_path, tec, "bare_files", file))
        if os.path.exists(os.path.join(data_path, tec, "bare_files", "flow_technology")):
            for file in os.listdir(os.path.join(data_path, tec, "bare_files", "flow_technology")):
                if os.path.isfile(os.path.join(data_path, tec, "bare_files", "flow_technology", file)):
                    os.remove(os.path.join(data_path, tec, "bare_files", "flow_technology", file))

# Add scenario updates for project
def add_scenario_updates(project_name, config_name, data_path):
    """
    Add scenario updates for project
    """
    print("Add scenario updates for project")
    config, config_name = load_config(project_name = project_name, config_name = config_name)
    for tec in config['constrained_tec']:
        print(f"...{tec}")
        if os.path.exists(package_data_path(project_name, "scenario_updates", tec)):
            for file in os.listdir(package_data_path(project_name, "scenario_updates", tec)):
                base_file = package_data_path(project_name, "scenario_updates", tec, file)
                if ".csv" in str(base_file):
                    dest_file = os.path.join(data_path, tec, "bare_files", file)
                    shutil.copy2(base_file, dest_file)
                    print(f"Copied file from scenario_updates to bare: {file}")

def add_missing_history(scenario, history_scenario, new_technologies):
    """
    Fill historical periods the bilateralize calibration does not cover

    The bilateralize tool calibrates historical_activity and historical_new_capacity
    of trade technologies only up to 2025, i.e. it assumes a first model year of 2030.
    Policy scenarios such as INDC2030i_forever have a later first model year, so the
    periods in between (e.g. 2030) are historical but the new trade technologies have
    no activity or capacity there. Their dynamic constraints then force them to zero
    in the first model year. This copies the solved ACT and CAP_NEW of those periods
    from `history_scenario` (e.g. baseline_bilateral) into the historical parameters.

    Args:
        scenario: Bilateralized scenario to update
        history_scenario: Solved bilateralized scenario whose first model year is
            at or before the earliest missing period
        new_technologies: Technologies added by bilateralization
    """
    fmy = scenario.firstmodelyear
    hist_fmy = history_scenario.firstmodelyear
    years = [y for y in scenario.set("year").astype(int) if hist_fmy <= y < fmy]
    if not years:
        return

    if not history_scenario.has_solution():
        raise RuntimeError(
            f"{history_scenario.model}/{history_scenario.scenario} must be solved to "
            f"provide {years} history for {scenario.model}/{scenario.scenario}"
        )
    print(f"Adding {years} history from "
          f"{history_scenario.model}/{history_scenario.scenario}")

    filters = {"technology": list(new_technologies)}

    act = history_scenario.var("ACT", filters=filters)
    act = act[act["year_act"].isin(years) & (act["lvl"] > 0)]
    act = (act.groupby(["node_loc", "technology", "year_act", "mode", "time"],
                       as_index=False)["lvl"].sum()
              .rename(columns={"lvl": "value"})
              .assign(unit="???"))

    cap = history_scenario.var("CAP_NEW", filters=filters)
    cap = cap[cap["year_vtg"].isin(years) & (cap["lvl"] > 0)]
    cap = (cap[["node_loc", "technology", "year_vtg", "lvl"]]
              .rename(columns={"lvl": "value"})
              .assign(unit="???"))

    with scenario.transact(f"Add {years} history for bilateralized trade"):
        scenario.add_par("historical_activity", act)
        scenario.add_par("historical_new_capacity", cap)


def limit_pipeline_build(scenario, growth=0.05, initial=100.0, prefix="gas_pipe_"):
    """
    Limit how fast new pipeline length can be built on each route

    With the defaults a route with no recent build can add ~550 km/yr (~2,800 km
    per 5-year period, about one Power of Siberia), and the rate can grow 5%/yr.

    Args:
        growth: growth_new_capacity_up (per year)
        initial: initial_new_capacity_up (km/yr)
        prefix: Technologies to constrain
    """
    techs = [t for t in scenario.set("technology") if t.startswith(prefix)]
    nodes = (scenario.par("output", filters={"technology": techs})
             [["node_loc", "technology"]].drop_duplicates())
    if nodes.empty:
        return
    years = [int(y) for y in scenario.set("year") if int(y) >= scenario.firstmodelyear]
    df = nodes.merge(pd.DataFrame({"year_vtg": years}), how="cross")
    print(f"Limit new pipeline build on {len(nodes)} routes "
          f"(growth {growth}/yr, initial {initial} km/yr)")
    with scenario.transact("Limit new pipeline build"):
        scenario.add_par("growth_new_capacity_up", df.assign(value=growth, unit="km"))
        scenario.add_par("initial_new_capacity_up", df.assign(value=initial, unit="km"))


#: Upper bounds (GWa) on pipeline routes, from existing and announced projects;
#: 1 bcm/yr ~ 1.14 GWa. Each value holds from its year until the next listed year.
ROUTE_ACTIVITY_UP = {
    ("R12_FSU", "gas_piped_exp_chn"): {
        # Central Asia-China A/B/C (55) + Power of Siberia 1 (38) + Far East (10)
        2030: 118.0,
        # + Power of Siberia 2 (50 bcm, agreed Sep 2025)
        2035: 175.0,
        # + Central Asia-China Line D (30 bcm, shelved)
        2040: 210.0,
    },
}


def bound_route_activity(scenario, bounds=None):
    """
    Add ``bound_activity_up`` on specific trade routes

    Args:
        bounds: {(node_loc, technology): {year: GWa}}. Each value applies from its
            year until the next listed year (the last one to the end of the horizon).
            Default: :data:`ROUTE_ACTIVITY_UP`.
    """
    bounds = ROUTE_ACTIVITY_UP if bounds is None else bounds
    years = sorted(int(y) for y in scenario.set("year") if int(y) >= scenario.firstmodelyear)
    rows = []
    for (node, tec), path in bounds.items():
        steps = sorted(path.items())
        for y in years:
            value = [v for y0, v in steps if y0 <= y]
            if value:
                rows.append(dict(node_loc=node, technology=tec, year_act=y,
                                 mode="M1", time="year", value=value[-1], unit="GWa"))
    if not rows:
        return
    df = pd.DataFrame(rows)
    print(f"Bound activity on {len(bounds)} trade routes")
    with scenario.transact("Bound trade route activity"):
        scenario.add_par("bound_activity_up", df)


def set_import_growth(scenario, techs=("LNG_shipped_imp",), growth_up=0.05,
                      growth_lo=-0.05):
    """
    Add growth rates to import technologies that only have ``initial_activity_*``

    Without a growth rate the dynamic constraints let imports change by only
    initial_activity (2 GWa/yr, i.e. +/-10 GWa per period), whatever their level.
    Rows that already have a growth rate are kept.

    Args:
        techs: Import technologies to update
        growth_up: growth_activity_up (per year)
        growth_lo: growth_activity_lo (per year, negative)
    """
    idx = ["node_loc", "technology", "year_act", "time"]
    with scenario.transact("Add growth rates to import technologies"):
        for side, value in (("up", growth_up), ("lo", growth_lo)):
            init = scenario.par(f"initial_activity_{side}",
                                filters={"technology": list(techs)})
            if init.empty:
                continue
            have = scenario.par(f"growth_activity_{side}",
                                filters={"technology": list(techs)})
            df = init[idx].merge(have[idx], how="left", indicator=True)
            df = df[df["_merge"] == "left_only"].drop(columns="_merge")
            if df.empty:
                continue
            print(f"Add growth_activity_{side} = {value} on {len(df)} rows of {techs}")
            scenario.add_par(f"growth_activity_{side}",
                             df.assign(value=value, unit="%"))


def bilateralize_scenario(project_name, config_name, scenario, target_scenario = None,
                          history_scenario = None, last_model_year = None,
                          lt_contracts = True, pipeline_build_limit = True,
                          route_bounds = True, import_growth = True,
                          rebase_extraction = True, shift_nam_gas = True,
                          **contract_kwargs):
    """
    Bilateralize a given scenario

    Args:
        project_name: Name of project (message_ix_models/project/[THIS])
        config_name: Name of the bilateralize config file for this project
        scenario: Base scenario to bilateralize
        target_scenario: Name for the bilateralized output scenario, in the
            `project_name` model. Defaults to f"{scenario.scenario}_bilateral"
            so the output never collides with the input's own name. Any
            "_DEFAULT" is dropped, so baseline_DEFAULT -> baseline_bilateral.
        history_scenario: Solved bilateralized scenario used to fill historical
            periods after 2025 when `scenario` has a later first model year (see
            :func:`add_missing_history`). Required in that case.
        last_model_year: If given, set as cat_year "lastmodelyear" so the myopic
            solve loop stops after this period (later periods are still in the
            foresight window of the last iteration, but are not solved on their own).
        lt_contracts: If True (default), add long-term LNG/pipeline gas contracts as
            bound_activity_lo on the bilateral trade technologies (see
            :func:`.lt_contracts.add_contract_bounds`).
        pipeline_build_limit: If True (default), limit the build rate of new gas
            pipeline length (see :func:`limit_pipeline_build`).
        route_bounds: If True (default), cap activity on routes in
            :data:`ROUTE_ACTIVITY_UP` (see :func:`bound_route_activity`).
        import_growth: If True (default), add growth rates to LNG imports (see
            :func:`set_import_growth`).
        rebase_extraction: If True (default), rebase 2025 oil and gas extraction
            history (grades, mpen, resource extraction, new capacity) to IEA World
            Energy Balances production (see
            :func:`.rebase_extraction.rebase_extraction_history`).
        shift_nam_gas: If True (default), move R12_NAM gas resource from gas_4 into
            gas_2 so its cheap grades match US + Canadian proved reserves (see
            :func:`.shift_resource_grades.shift_nam_gas_grades`).
        **contract_kwargs: Passed to :func:`.lt_contracts.add_contract_bounds`, e.g.
            ``contract_share`` (default 0.9).
    """
    target_scenario = (target_scenario or f"{scenario.scenario}_bilateral").replace(
        "_DEFAULT", ""
    )

    # Load config
    config, config_name = load_config(project_name = project_name, config_name = config_name)
    data_path = package_data_path("bilateralize")

    # Clear bare files
    clean_bare_files(data_path, config)

    # Prepare edit files (parameters)
    prepare_edit_files(project_name = project_name, 
                       config_name = config_name,
                       P_access = True)
    
    # Add scenario updates for project
    add_scenario_updates(project_name = project_name,
                         config_name = config_name,
                         data_path = data_path)
    
    # Move data from bare files to a dictionary to update a MESSAGEix scenario
    trade_dict = bare_to_scenario(project_name = project_name, 
                                  config_name = config_name,
                                  p_drive_access = True)

    # Additional liquefaction calibration
    liquefaction_parameters = update_liquefaction_input(message_regions = "R12",
                                                        project_name = project_name,
                                                        config_name = config_name)

    # Clone and set up base scenario
    print(f"Base model: {scenario.model}/{scenario.scenario}")
    print(f"Target model: {project_name}/{target_scenario}")

    print("Setting up scenario")
    mp = scenario.platform
    load_and_solve(mp = mp,
                   trade_dict = trade_dict,
                   solve = False,
                   project_name = project_name,
                   config_name = config_name,
                   start_model = scenario.model,
                   start_scen = scenario.scenario,
                   target_model = project_name,
                   target_scen = target_scenario,
                   extra_parameter_updates = liquefaction_parameters)

    # Update extraction constraints
    print("Updating extraction constraints")
    base_scenario = message_ix.Scenario(mp, model=project_name, scenario=target_scenario)
    out_scenario = base_scenario.clone(project_name, target_scenario)
    out_scenario.set_as_default()

    # Fill historical periods between the 2025 calibration and the first model year
    new_technologies = set(out_scenario.set("technology")) - set(scenario.set("technology"))
    if history_scenario is not None:
        add_missing_history(out_scenario, history_scenario, new_technologies)
    elif out_scenario.firstmodelyear > 2030:
        raise ValueError(
            f"{scenario.model}/{scenario.scenario} has first model year "
            f"{out_scenario.firstmodelyear}; pass a solved history_scenario (e.g. "
            "baseline_bilateral) to provide trade history for the periods after 2025"
        )

    if rebase_extraction:
        print("Rebase 2025 oil/gas extraction history to IEA WEB")
        rebase_extraction_history(out_scenario, project_name=project_name,
                                  config_name=config_name, year=2025)

    if shift_nam_gas:
        print("Shift NAM gas resource from gas_4 to gas_2 to match proved reserves")
        shift_nam_gas_grades(out_scenario)

    for g in ['growth_activity_up']:
        updf = out_scenario.par(g)
        updf = updf[(updf['technology'].str.contains('gas_extr_mpen'))]
        updf = updf[updf['node_loc'].isin(['R12_WEU'])]
    
        remdf = updf.copy()
        if g == 'growth_activity_up':
            updf['value'] = 0.01
        elif g == 'growth_activity_lo':
            updf['value'] = -0.01
            
        with out_scenario.transact("update growth activity to gas_extr_mpen"):
            out_scenario.remove_par(g, remdf)
            out_scenario.add_par(g, updf)

    # Add balance equality sets
    print("Add balance equality sets")
    be_df = out_scenario.par("output", filters = {"technology": config['covered_trade_technologies']})
    be_df = be_df[be_df['level'].isin(['piped', 'shipped'])]
    be_df = be_df[['commodity', 'level']].drop_duplicates()

    with out_scenario.transact("add balance equality sets"):
        out_scenario.add_set("balance_equality", be_df)

    # Adjust re-exports for lightoil and fueloil
    print("Adjust re-exports for lightoil and fueloil")
    adjust_reexports(base_scenario = out_scenario,
                     trade_commodity_list = ['lightoil', 'fueloil'],
                     base_level = 'secondary')

    if last_model_year is not None:
        print(f"Set last model year to {last_model_year}")
        with out_scenario.transact("set lastmodelyear"):
            old = out_scenario.set("cat_year", filters={"type_year": "lastmodelyear"})
            if len(old):
                out_scenario.remove_set("cat_year", old)
            out_scenario.add_set("cat_year", ["lastmodelyear", last_model_year])

    if pipeline_build_limit:
        limit_pipeline_build(out_scenario)

    if route_bounds:
        bound_route_activity(out_scenario)

    if import_growth:
        set_import_growth(out_scenario)

    if lt_contracts:
        print("Add long-term gas contract bounds")
        add_contract_bounds(out_scenario, **contract_kwargs)

    print("Solve scenario")
    out_scenario.solve(quiet=False, model="MESSAGE",
                       solve_options={"lpmethod": "4", "scaind": "1", "solutiontype": "1", "threads": "8",
                                      "numericalemphasis":"1", "baralg":"1"},
                       gams_args=["--foresight=15", "--clean_tol=1e-10", "--myopic_fix_tol=1e-3"],
                       equ_list=["COMMODITY_BALANCE_GT"],
                       check_solution = False)

    return out_scenario