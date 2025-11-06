# -*- coding: utf-8 -*-
"""
Update MESSAGEix scenario(s) with bilateralized dictionary

This script is the third step in implementing the bilateralize tool.
It updates a specified MESSAGEix scenario with the bilateralized dictionary.
It then has options to solve the scenario within the ixmp database
or save as a GDX data file for direct solve in GAMS.
"""

# Import packages
import logging
import os
from pathlib import Path

import ixmp
import message_ix
import pandas as pd

from message_ix_models.tools.bilateralize.utils import get_logger, load_config


# %% Remove existing trade technologies
def remove_trade_tech(scen: message_ix.Scenario, log, config_tec: dict, tec: str):
    """
    Remove existing trade technologies
    """
    base_tec_name = tec.replace("_shipped", "")
    base_tec_name = base_tec_name.replace("_piped", "")

    base_tec = [
        config_tec[tec][tec + "_trade"]["trade_commodity"] + "_exp",
        config_tec[tec][tec + "_trade"]["trade_commodity"] + "_imp",
        base_tec_name + "_exp",
        base_tec_name + "_imp",
    ]
    base_tec = base_tec + [
        i
        for i in scen.set("technology")
        if config_tec[tec][tec + "_trade"]["trade_commodity"] + "_exp_" in i
    ]
    base_tec = base_tec + ["oil_exp", "oil_imp"]  # for crude

    base_tec = list(set(base_tec))
    with scen.transact("Remove base trade technologies for " + tec):
        for t in base_tec:
            if t in list(scen.set("technology")):
                log.info("Removing base technology..." + t)
                scen.remove_set("technology", t)


# %% Add sets for trade technologies
def add_trade_sets(scen: message_ix.Scenario, log, trade_dict: dict, tec: str):
    """
    Add sets for trade technologies
    """
    new_sets = dict()
    for s in ["technology", "level", "commodity", "mode"]:
        setlist = set(
            list(trade_dict[tec]["trade"]["input"][s].unique())
            + list(trade_dict[tec]["trade"]["output"][s].unique())
        )

        if "input" in trade_dict[tec]["flow"].keys():
            add_list = set(
                list(trade_dict[tec]["flow"]["input"][s].unique())
                + list(trade_dict[tec]["flow"]["output"][s].unique())
            )
            setlist = setlist.union(add_list)
        setlist_out = list(setlist)

        new_sets[s] = setlist_out

    with scen.transact("Add new sets for " + tec):
        for s in ["technology", "level", "commodity", "mode"]:
            base_set = list(scen.set(s))
            for i in new_sets[s]:
                if i not in base_set:
                    log.info("Adding set: " + s + "..." + i)
                    scen.add_set(s, i)
                else:
                    pass


# %% Add parameters for bilateralized trade
def add_trade_parameters(scen: message_ix.Scenario, log, trade_dict: dict, tec: str):
    """
    Add parameters for bilateralized trade
    """
    new_parameter_list = list(
        set(
            [
                i
                for i in list(trade_dict[tec]["trade"].keys())
                + list(trade_dict[tec]["flow"].keys())
                if "relation_" not in i
            ]
        )
    )

    with scen.transact("Add new parameters for " + tec):
        for p in new_parameter_list:
            log.info("Adding parameter for " + tec + ": " + p)
            pardf = pd.DataFrame()
            if trade_dict[tec]["trade"].get(p) is not None:
                log.info("... parameter added for trade technology")
                pardf = pd.concat([pardf, trade_dict[tec]["trade"][p]])
            if trade_dict[tec]["flow"].get(p) is not None:
                log.info("... parameter added for flow technology")
                pardf = pd.concat([pardf, trade_dict[tec]["flow"][p]])
            pardf = pardf[pardf["value"].notnull()]
            scen.add_par(p, pardf)


# %% Update relation parameters
def update_relation_parameters(
    scen: message_ix.Scenario, log, trade_dict: dict, tec: str
):
    """
    Update relation parameters
    """
    rel_parameter_list = list(
        set(
            [
                i
                for i in list(trade_dict[tec]["trade"].keys())
                + list(trade_dict[tec]["flow"].keys())
                if "relation_" in i
            ]
        )
    )

    if len(rel_parameter_list) > 0:
        for rel_par in ["relation_activity", "relation_upper", "relation_lower"]:
            rel_par_df = pd.DataFrame()

            rel_par_list = list(
                set(
                    [
                        i
                        for i in list(trade_dict[tec]["trade"].keys())
                        + list(trade_dict[tec]["flow"].keys())
                        if rel_par in i
                    ]
                )
            )

            for r in rel_par_list:
                if r in list(trade_dict[tec]["trade"].keys()):
                    rel_par_df = pd.concat([rel_par_df, trade_dict[tec]["trade"][r]])
                if r in list(trade_dict[tec]["flow"].keys()):
                    rel_par_df = pd.concat([rel_par_df, trade_dict[tec]["flow"][r]])

            if rel_par == "relation_activity":
                with scen.transact("Adding new relation sets"):
                    new_relations = list(rel_par_df["relation"].unique())
                    for nr in new_relations:
                        if nr not in scen.set("relation").unique():
                            scen.add_set("relation", nr)
            if len(rel_par_df) > 0:
                with scen.transact("Add " + rel_par + " for " + tec):
                    log.info("Adding " + rel_par + " for " + tec)
                    rel_par_df = rel_par_df[rel_par_df["value"].notnull()]
                    scen.add_par(rel_par, rel_par_df)


# %% Update bunker fuels
def update_bunker_fuels(scen: message_ix.Scenario, tec: str, log, config_tec: dict):
    """
    Update bunker fuels
    """
    bunker_tec = config_tec[tec][tec + "_trade"]["bunker_technology"]
    if bunker_tec is not None:
        for btec in bunker_tec.keys():
            bunkerdf_in = scen.par("input", filters={"technology": bunker_tec[btec]})
            bunkerdf_out = bunkerdf_in.copy()
            bunkerdf_out["level"] = "bunker"
            with scen.transact("Update bunker fuel for" + tec):
                log.info("Updating bunker level for " + tec)
                scen.remove_par("input", bunkerdf_in)
                scen.add_par("input", bunkerdf_out)


# %% Update additional parameters (separate from bilateralization)
def update_additional_parameters(
    scen: message_ix.Scenario, extra_parameter_updates: dict | None = None
):
    """
    Update additional parameters (separate from bilateralization)
    """
    if extra_parameter_updates is not None:
        for par in extra_parameter_updates.keys():
            with scen.transact("Update additional parameter: " + par):
                new_df = extra_parameter_updates[par]
                base_df = scen.par(par)

                if "value" in new_df.columns:
                    rem_df = new_df[[c for c in new_df.columns if c != "value"]]
                    base_df = base_df.merge(
                        rem_df,
                        left_on=list(rem_df.columns),
                        right_on=list(rem_df.columns),
                        how="inner",
                    )
                    add_df = base_df.copy().drop(["value"], axis=1)
                    add_df = add_df.merge(
                        new_df,
                        left_on=list(rem_df.columns),
                        right_on=list(rem_df.columns),
                        how="left",
                    )

                if "multiplier" in new_df.columns:
                    col_list = [c for c in new_df.columns if c != "multiplier"]
                    base_df = base_df.merge(
                        new_df, left_on=col_list, right_on=col_list, how="inner"
                    )
                    add_df = base_df.copy()
                    base_df = base_df.drop(["multiplier"], axis=1)
                    add_df["value"] = add_df["value"] * add_df["multiplier"]
                    add_df = add_df.drop(["multiplier"], axis=1)

                scen.remove_par(par, base_df)
                scen.add_par(par, add_df)


# %% Remove PAO constraints on MESSAGEix-GLOBIOM
def remove_pao_coal_constraint(
    scen: message_ix.Scenario, log, MESSAGEix_GLOBIOM: bool = True
):
    """
    Remove PAO coal and gas constraints on MESSAGEix-GLOBIOM
    """
    if MESSAGEix_GLOBIOM:
        with scen.transact("Remove PAO coal and gas constraints on primary energy"):
            for rel in ["domestic_coal", "domestic_gas"]:
                log.info("Removing constraints on PAO primary energy: " + rel)
                relact_df = scen.par("relation_activity", filters={"relation": rel})
                relupp_df = scen.par("relation_upper", filters={"relation": rel})
                scen.remove_par("relation_activity", relact_df)
                scen.remove_par("relation_upper", relupp_df)


# %% Write just the GDX files
def save_to_gdx(mp: ixmp.Platform, scenario, output_path: str):
    """
    Save the scenario to a GDX file.

    Args:
        mp: ixmp platform
        scenario: Scenario name
        output_path: Path to save the GDX file
    """
    from ixmp.backend import ItemType

    mp._backend.write_file(
        Path(output_path),
        ItemType.SET | ItemType.PAR,
        filters={"scenario": scenario},
    )
    # %% Solve or save scenario


def solve_or_save(
    mp: ixmp.Platform,
    scen: message_ix.Scenario,
    solve: bool = False,
    to_gdx: bool = False,
    gdx_location: str | None = None,
):
    """
    Solve or save scenario.
    """
    if to_gdx and not solve and gdx_location is not None:
        save_to_gdx(
            mp=mp,
            scenario=scen,
            output_path=os.path.join(
                gdx_location, f"MsgData_{scen.model}_{scen.scenario}.gdx"
            ),
        )

    if solve:
        solver = "MESSAGE"
        scen.solve(solver, solve_options=dict(lpmethod=4))

        print("Unlock run ID of the scenario")
        # runid = scen.run_id()
        # mp._backend.jobj.unlockRunid(runid)


def load_and_clone(
    mp: ixmp.Platform,
    log: logging.Logger,
    config_base: dict,
    start_scen: str | None = None,
    start_model: str | None = None,
    target_scen: str | None = None,
    target_model: str | None = None,
) -> message_ix.Scenario:
    """Load and clone scenario.
    Args:
        mp: ixmp platform
        log: Logger
        project_name: Name of project
        config_name: Name of config file
        start_scen: Name of scenario to start from
        start_model: Name of model to start from
        target_scen: Name of scenario to target
        target_model: Name of model to target
    """
    # Load config
    if start_model is None:
        start_model = config_base.get("scenario", {}).get("start_model")
    if start_scen is None:
        start_scen = config_base.get("scenario", {}).get("start_scen")

    base = message_ix.Scenario(mp, model=start_model, scenario=start_scen)
    log.info(f"Loaded scenario: {start_model}/{start_scen}")

    # Clone scenario
    if target_model is None:
        target_model = config_base.get("scenario", {}).get("target_model", [])
    if target_scen is None:
        target_scen = config_base.get("scenario", {}).get("target_scen")

    scen = base.clone(target_model, target_scen, keep_solution=False)
    scen.set_as_default()

    log.info("Scenario loaded and cloned.")

    return scen


# %% Clone and update scenario
def load_and_solve(
    trade_dict,
    solve=False,
    to_gdx=False,
    project_name: str | None = None,
    config_name: str | None = None,
    start_scen: str | None = None,
    start_model: str | None = None,
    target_scen: str | None = None,
    target_model: str | None = None,
    scenario: message_ix.Scenario | None = None,
    extra_parameter_updates: dict | None = None,
    gdx_location: str | None = None,
    MESSAGEix_GLOBIOM: bool = True,
):
    """
    Clone and update scenario.

    Args:
        trade_dict: Dictionary of parameter dataframes
    Optional Args:
        solve: If True, solve scenario
        to_gdx: If True, save scenario to a GDX file
        project_name: Name of project (message_ix_models/project/[THIS])
        config_name: Name of config file.
            If None, uses default config from data/bilateralize/config_default.yaml
        start_scen: Name of scenario to start from
        start_model: Name of model to start from
        start_model_name: Name of model to start from
        target_scen: Name of scenario to target
        target_model: Name of model to target
        target_model_name: Name of model to target
        scenario: Scenario to update (if None, will clone from project yaml)
        additional_parameter_updates: Dictionary of additional parameter updates
        gdx_location: Location to save GDX file
        remove_pao_coal_constraint: Remove PAO coal and gas constraints
    """
    # Load config
    config_base, config_path, config_tec = load_config(
        project_name, config_name, load_tec_config=True
    )

    log = get_logger(name="load_and_solve")
    log.info("Loading and solving scenario")

    # Load the scenario
    mp = ixmp.Platform()

    if scenario is None:
        scenario = load_and_clone(
            mp=mp,
            log=log,
            config_base=config_base,
            start_scen=start_scen,
            start_model=start_model,
            target_scen=target_scen,
            target_model=target_model,
        )
    else:
        log.info(f"Using existing scenario: {scenario.model}/{scenario.scenario}")
        scen = scenario

    # Add sets and parameters for each covered technology
    covered_tec = config_base.get("covered_trade_technologies")

    for tec in covered_tec:
        # Remove existing technologies related to trade
        remove_trade_tech(scen=scen, log=log, config_tec=config_tec, tec=tec)

        # Add to sets: technology, level, commodity, mode
        add_trade_sets(scen=scen, log=log, trade_dict=trade_dict, tec=tec)

        # Add parameters
        add_trade_parameters(scen=scen, log=log, trade_dict=trade_dict, tec=tec)

        # Relation activity, upper, and lower
        update_relation_parameters(scen=scen, log=log, trade_dict=trade_dict, tec=tec)

        # Update bunker fuels
        update_bunker_fuels(scen=scen, tec=tec, log=log, config_tec=config_tec)

    # Update additional parameters
    update_additional_parameters(
        scen=scen, extra_parameter_updates=extra_parameter_updates
    )

    # Remove PAO coal and gas constraints on MESSAGEix-GLOBIOM
    remove_pao_coal_constraint(scen=scen, log=log, MESSAGEix_GLOBIOM=MESSAGEix_GLOBIOM)

    # Solve or save scenario
    solve_or_save(
        mp=mp,
        scen=scen,
        solve=solve,
        to_gdx=to_gdx,
        gdx_location=gdx_location,
    )
