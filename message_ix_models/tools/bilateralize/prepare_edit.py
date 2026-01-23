# -*- coding: utf-8 -*-
"""
Prepare edit files for bilateralize tool

This script is the first step in implementing the bilateralize tool.
It generates empty (or default valued) parameters that are required for
    bilateralization, specified by commodity.
This step is optional in a workflow; users can move directly to the next
    step (2_bare_to_scenario).
"""

# Import packages
import itertools
import logging
import os
import shutil
from pathlib import Path
from typing import Any, Dict

import message_ix
import numpy as np
import pandas as pd
import yaml

from message_ix_models.tools.bilateralize.calculate_distance import calculate_distance
from message_ix_models.tools.bilateralize.historical_calibration import (
    build_historical_price,
)
from message_ix_models.tools.bilateralize.mariteam_calibration import calibrate_mariteam
from message_ix_models.tools.bilateralize.pull_gem import import_gem
from message_ix_models.tools.bilateralize.utils import get_logger, load_config
from message_ix_models.util import package_data_path


# %% Generate folders if missing
def folders_for_trade(covered_tec: list[str]):
    """
    Generate folders for each trade technology
    """
    for tec in covered_tec:
        tecpath = os.path.join(Path(package_data_path("bilateralize")), tec)
        if not os.path.isdir(tecpath):
            os.makedirs(tecpath)
        pathlist = [
            os.path.join(tecpath, "edit_files"),
            os.path.join(tecpath, "edit_files", "flow_technology"),
            os.path.join(tecpath, "bare_files"),
            os.path.join(tecpath, "bare_files", "flow_technology"),
        ]
        for f in pathlist:
            if not os.path.isdir(f):
                os.makedirs(f)


def define_networks(
    log,
    message_regions: str,
    covered_tec: list[str],
    config_dict: Dict[str, Dict[str, Any]],
    data_path: Path,
):
    """
    Define network dataframe

    Args:
        message_regions: Regional resolution
    """
    # Generate full combination of nodes to build technology-specific network
    node_path = package_data_path("node", message_regions + ".yaml")
    with open(node_path, "r", encoding="utf-8") as file_handler:
        node_set = yaml.safe_load(file_handler)
    node_set = [r for r in node_set.keys() if r not in ["World", "GLB"]]

    node_df = pd.DataFrame(itertools.product(node_set, node_set))
    node_df.columns = ["exporter", "importer"]
    node_df = node_df[node_df["exporter"] != node_df["importer"]]

    network_setup = {}

    for tec in covered_tec:
        log.info(f"Defining network for {tec}")
        node_df_tec = node_df.copy()

        node_df_tec["export_technology"] = config_dict["trade_technology"][tec] + "_exp"
        if config_dict["trade_tech_suffix"][tec] is not None:
            node_df_tec["export_technology"] = (
                node_df_tec["export_technology"]
                + "_"
                + config_dict["trade_tech_suffix"][tec]
            )

        node_df_tec["export_technology"] = (
            node_df_tec["export_technology"]
            + "_"
            + node_df_tec["importer"].str.lower().str.split("_").str[1]
        )

        # If there are multiple trade "routes" (not flow, but decision) per technology
        if (config_dict["trade_tech_number"][tec] is not None) & (
            config_dict["trade_tech_number"][tec] != 1
        ):
            ndt_out = pd.DataFrame()
            for i in list(range(1, config_dict["trade_tech_number"][tec] + 1)):
                ndt = node_df_tec.copy()
                ndt["export_technology"] = ndt["export_technology"] + "_" + str(i)
                ndt_out = pd.concat([ndt_out, ndt])
            node_df_tec = ndt_out.copy()

        node_df_tec["import_technology"] = config_dict["trade_technology"][tec] + "_imp"
        node_df_tec["INCLUDE? (No=0, Yes=1)"] = ""

        # Specify network (the function will stop to allow specification)
        if config_dict["specify_network"][tec] is True:
            try:
                specify_network_tec = pd.read_csv(
                    os.path.join(data_path, tec, "specify_network_" + tec + ".csv")
                )
            except FileNotFoundError:
                node_df_tec.to_csv(
                    os.path.join(data_path, tec, "specify_network_" + tec + ".csv"),
                    index=False,
                )
                raise Exception(
                    "The function stopped. Sheet specify_network_"
                    + tec
                    + ".csv has been generated. "
                    + "Fill in the specific pairs first and run again."
                )
            if not any(specify_network_tec["INCLUDE? (No=0, Yes=1)"].notnull()):
                raise Exception(
                    "The function stopped. Ensure that all values "
                    + "under 'INCLUDE? (No=0, Yes=1)' are filled"
                )
        elif config_dict["specify_network"][tec] is False:
            specify_network_tec = node_df_tec.copy()
            specify_network_tec["INCLUDE? (No=0, Yes=1)"] = 1
        else:
            raise Exception("Please use True or False.")

        network_setup[tec] = specify_network_tec[
            specify_network_tec["INCLUDE? (No=0, Yes=1)"] == 1
        ]

    return network_setup
    # %% Build base parameter dataframe based on network dataframe


def build_parameterdf(
    par_name: str,
    network_df: pd.DataFrame,
    col_values: dict,
    common_years: dict | None = None,
    common_cols: dict | None = None,
    export_only: bool = False,
):
    """
    Build parameter dataframes based on the specified network dataframe.

    Args:
        par_name: Parameter name (e.g., capacity_factor)
        network_df: Specified network dataframe
        col_values: Values for other columns to populate as default
        export_only: If True, only produces dataframe for export technology
    """

    if common_years is None:
        common_years = dict(
            year_vtg="broadcast", year_rel="broadcast", year_act="broadcast"
        )
    if common_cols is None:
        common_cols = dict(mode="M1", time="year", time_origin="year", time_dest="year")

    df_export = message_ix.make_df(
        par_name,
        node_loc=network_df["exporter"],
        technology=network_df["export_technology"],
        **col_values,
        **common_years,
        **common_cols,
    )
    df = df_export.copy()

    if not export_only:
        df_import = message_ix.make_df(
            par_name,
            node_loc=network_df["importer"],
            technology=network_df["import_technology"],
            **col_values,
            **common_years,
            **common_cols,
        )
        df = pd.concat([df, df_import])

    return df


# %% Generate input parameter (trade technology)
def build_input(
    tec: str,
    network_setup: dict,
    config_dict: dict,
    common_years: dict,
    common_cols: dict,
    parameter_outputs: dict,
):
    """
    Generate input parameter (trade technology)
    """
    # Trade Level (supply to piped/shipped)
    df_input_trade = message_ix.make_df(
        "input",
        node_origin=network_setup[tec]["exporter"],
        node_loc=network_setup[tec]["exporter"],
        technology=network_setup[tec]["export_technology"],
        commodity=config_dict["trade_commodity"][tec],
        level=config_dict["export_level"][tec],
        value=1,
        unit=config_dict["trade_units"][tec],
        **common_years,
        **common_cols,
    )

    # Import Level (piped/shipped to import)
    df_input_import = message_ix.make_df(
        "input",
        node_origin=network_setup[tec]["importer"],
        node_loc=network_setup[tec]["importer"],
        technology=network_setup[tec]["import_technology"],
        commodity=config_dict["trade_commodity"][tec],
        level=config_dict["trade_level"][tec],
        value=1,
        unit=config_dict["trade_units"][tec],
        **common_years,
        **common_cols,
    )

    df_input = pd.concat([df_input_trade, df_input_import]).drop_duplicates()

    parameter_outputs[tec]["trade"]["input"] = df_input

    return parameter_outputs


# %% Generate output parameter (trade technology)
def build_output(
    tec: str,
    network_setup: dict,
    config_dict: dict,
    common_years: dict,
    common_cols: dict,
    parameter_outputs: dict,
):
    """
    Generate output parameter (trade technology)
    """
    # Trade Level
    df_output_trade = message_ix.make_df(
        "output",
        node_loc=network_setup[tec]["exporter"],
        node_dest=network_setup[tec]["importer"],
        technology=network_setup[tec]["export_technology"],
        commodity=config_dict["trade_commodity"][tec],
        level=config_dict["trade_level"][tec],
        value=1,
        unit=config_dict["trade_units"][tec],
        **common_years,
        **common_cols,
    )

    # Import Level
    df_output_import = message_ix.make_df(
        "output",
        node_loc=network_setup[tec]["importer"],
        node_dest=network_setup[tec]["importer"],
        technology=network_setup[tec]["import_technology"],
        commodity=config_dict["trade_commodity"][tec],
        level=config_dict["import_level"][tec],
        value=1,
        unit=config_dict["trade_units"][tec],
        **common_years,
        **common_cols,
    )

    df_output = pd.concat([df_output_trade, df_output_import]).drop_duplicates()

    parameter_outputs[tec]["trade"]["output"] = df_output

    return parameter_outputs


# %% Generate technical lifetime parameter (trade technology)
def build_technical_lifetime(
    tec: str, network_setup: dict, config_dict: dict, parameter_outputs: dict, **kwargs
):
    """
    Generate technical lifetime parameter (trade technology)
    """
    df_teclt = build_parameterdf(
        "technical_lifetime",
        network_df=network_setup[tec],
        col_values=dict(
            value=config_dict["trade_technical_lifetime"][tec],
            unit="y",
        ),
    )

    parameter_outputs[tec]["trade"]["technical_lifetime"] = df_teclt.drop_duplicates()

    return parameter_outputs


# %% Generate historical activity parameter (trade technology)
def build_historical_activity(
    tec: str, network_setup: dict, config_dict: dict, parameter_outputs: dict, **kwargs
):
    """
    Generate costs for trade technology
    """
    df_hist = pd.DataFrame()

    for y in list(range(2000, 2025, 5)):
        ydf = build_parameterdf(
            "historical_activity",
            network_df=network_setup[tec],
            col_values=dict(unit=config_dict["trade_units"][tec]),
        )
        ydf["year_act"] = y
        df_hist = pd.concat([df_hist, ydf])

    parameter_outputs[tec]["trade"]["historical_activity"] = df_hist.drop_duplicates()

    return parameter_outputs


# %% Generate costs for trade technology
def build_costs(
    tec: str, network_setup: dict, config_dict: dict, parameter_outputs: dict, **kwargs
):
    """
    Generate costs for trade technology
    """
    # Create base files: inv_cost, fix_cost, var_cost
    for cost_par in ["inv_cost", "fix_cost", "var_cost"]:
        df_cost = build_parameterdf(
            cost_par,
            network_df=network_setup[tec],
            col_values=dict(unit="USD/" + config_dict["trade_units"][tec]),
        )
        parameter_outputs[tec]["trade"][cost_par] = df_cost.drop_duplicates()

    return parameter_outputs


# %% Generate capacity factor parameter (trade technology)
def build_capacity_factor(
    tec: str, network_setup: dict, config_dict: dict, parameter_outputs: dict, **kwargs
):
    """
    Generate capacity factor parameter (trade technology)
    """
    df_cf = build_parameterdf(
        "capacity_factor",
        network_df=network_setup[tec],
        col_values=dict(value=1, unit="%"),
    )

    parameter_outputs[tec]["trade"]["capacity_factor"] = df_cf.drop_duplicates()

    return parameter_outputs


# %% Generate constraints for trade technology
def build_constraints(
    tec: str, network_setup: dict, config_dict: dict, parameter_outputs: dict, **kwargs
):
    """
    Generate constraints for trade technology
    """
    for par_name in [
        "initial_activity",
        "abs_cost_activity_soft",
        "growth_activity",
        "level_cost_activity_soft",
        "soft_activity",
    ]:
        for t in ["lo", "up"]:
            df_con = build_parameterdf(
                par_name + "_" + t,
                network_df=network_setup[tec],
                col_values=dict(unit=config_dict["trade_units"][tec]),
            )

            if par_name == "growth_activity":
                if t == "lo":
                    df_con["value"] = -0.05
                if t == "up":
                    df_con["value"] = 0.05

            if par_name == "initial_activity":
                if t == "lo":
                    df_con["value"] = 2
                if t == "up":
                    df_con["value"] = 2

            df_con = df_con.drop_duplicates()
            parameter_outputs[tec]["trade"][par_name + "_" + t] = df_con

    return parameter_outputs


def build_domestic_coalgas_relation(
    tec: str,
    network_setup: dict,
    config_dict: dict,
    common_years: dict,
    common_cols: dict,
    parameter_outputs: dict,
    **kwargs,
):
    """
    Generate domestic coal and gas relation parameter (trade technology)
    """
    if tec in ["gas_piped"]:
        for rel_act in ["domestic_coal", "domestic_gas"]:
            df_rel = message_ix.make_df(
                "relation_activity",
                node_loc=network_setup[tec]["importer"],
                node_rel=network_setup[tec]["importer"],
                technology=network_setup[tec]["import_technology"],
                commodity=config_dict["trade_commodity"][tec],
                value=-1,
                unit="???",
                relation=rel_act,
                **common_years,
                **common_cols,
            )
            df_rel = df_rel.drop_duplicates()

            parameter_outputs[tec]["trade"]["relation_activity_" + rel_act] = df_rel

    return parameter_outputs


# %% Generate emission factor parameter (trade technology)
def build_emission_factor(
    tec: str, network_setup: dict, config_dict: dict, parameter_outputs: dict, **kwargs
):
    """
    Generate emission factor parameter (trade technology)
    """
    if config_dict["tracked_emissions"][tec] is not None:
        df_ef = pd.DataFrame()
        for emission_type in config_dict["tracked_emissions"][tec]:
            df_ef_t = build_parameterdf(
                "emission_factor",
                network_df=network_setup[tec],
                col_values=dict(unit=None, emission=emission_type),
            )
            df_ef = pd.concat([df_ef, df_ef_t])

        parameter_outputs[tec]["trade"]["emission_factor"] = df_ef.drop_duplicates()

    return parameter_outputs


# %% Generate CO2 relation accounting
def build_accounting_relations(
    tec: str,
    network_setup: dict,
    config_dict: dict,
    common_years: dict,
    common_cols: dict,
    parameter_outputs: dict,
    **kwargs,
):
    """
    Relations for accounting purposes:
    CO2 emissions, primary energy total, aggregate exports,
    regional exports, regional imports

    """
    # CO2 emissions
    df_rel = message_ix.make_df(
        "relation_activity",
        node_loc=network_setup[tec]["exporter"],
        node_rel=network_setup[tec]["exporter"],
        technology=network_setup[tec]["export_technology"],
        commodity=config_dict["trade_commodity"][tec],
        unit="???",
        relation="CO2_Emission",
        **common_years,
        **common_cols,
    )
    df_rel = df_rel.drop_duplicates()

    parameter_outputs[tec]["trade"]["relation_activity_CO2_Emission"] = df_rel

    # Primary energy total
    df_rel = message_ix.make_df(
        "relation_activity",
        node_loc=network_setup[tec]["exporter"],
        node_rel=network_setup[tec]["exporter"],
        technology=network_setup[tec]["export_technology"],
        commodity=config_dict["trade_commodity"][tec],
        value=-1,
        unit="???",
        relation="PE_total_traditional",
        **common_years,
        **common_cols,
    )
    df_rel = df_rel.drop_duplicates()

    parameter_outputs[tec]["trade"]["relation_activity_PE_total_traditional"] = df_rel

    # Aggregate exports
    df_rel = message_ix.make_df(
        "relation_activity",
        node_loc=network_setup[tec]["exporter"],
        node_rel=network_setup[tec]["exporter"],
        technology=network_setup[tec]["export_technology"],
        commodity=config_dict["trade_commodity"][tec],
        value=1,
        unit="???",
        relation=config_dict["trade_technology"][tec] + "_exp_global",
        **common_years,
        **common_cols,
    )
    df_rel = df_rel.drop_duplicates()

    parameter_outputs[tec]["trade"]["relation_activity_global_aggregate"] = df_rel

    # Regional exports
    rel_use = config_dict["trade_technology"][tec] + "_exp"
    df_rel = message_ix.make_df(
        "relation_activity",
        node_loc=network_setup[tec]["exporter"],
        node_rel=network_setup[tec]["exporter"],
        technology=network_setup[tec]["export_technology"],
        commodity=config_dict["trade_commodity"][tec],
        value=1,
        unit="???",
        relation=rel_use,
        **common_years,
        **common_cols,
    )
    df_rel = df_rel.drop_duplicates()

    parameter_outputs[tec]["trade"]["relation_activity_regional_exp"] = df_rel

    # Regional imports
    rel_use = config_dict["trade_technology"][tec] + "_imp"
    df_rel = message_ix.make_df(
        "relation_activity",
        node_loc=network_setup[tec]["importer"],
        node_rel=network_setup[tec]["importer"],
        technology=network_setup[tec]["import_technology"],
        commodity=config_dict["trade_commodity"][tec],
        value=1,
        unit="???",
        relation=rel_use,
        **common_years,
        **common_cols,
    )
    df_rel = df_rel.drop_duplicates()

    parameter_outputs[tec]["trade"]["relation_activity_regional_imp"] = df_rel

    return parameter_outputs


# %% Generate flow technology input parameter
def build_flow_input(
    flow_tec: str,
    tec: str,
    network_setup: dict,
    config_dict: dict,
    common_years: dict,
    common_cols: dict,
    parameter_outputs: dict,
    message_regions: str = "R12",
    **kwargs,
):
    """
    Generate flow technology input parameter
    """
    df_input = pd.DataFrame()
    # List of commodity/material inputs
    flow_inputs = config_dict["flow_fuel_input"][tec][flow_tec]

    # Build by commodity input
    for c in flow_inputs:
        if c in config_dict["flow_fuel_input"][tec][flow_tec]:
            use_unit = "GWa"
        elif c in config_dict["flow_material_input"][tec][flow_tec]:
            use_unit = "Mt"

        # If pipelines, technologies and outputs are bilateral
        if config_dict["flow_constraint"][tec] == "bilateral":
            tec_use = (
                flow_tec
                + "_"
                + network_setup[tec]["importer"].str.lower().str.split("_").str[1]
            )
            df_input_base = message_ix.make_df(
                "input",
                node_loc=network_setup[tec]["exporter"],
                node_origin=network_setup[tec]["exporter"],
                level=config_dict["export_level"][tec],
                technology=tec_use,
                commodity=c,
                unit=use_unit,
                **common_years,
                **common_cols,
            )
            df_input = pd.concat([df_input, df_input_base])

        # If shipping, technologies and outputs are global with diff modes
        elif config_dict["flow_constraint"][tec] == "global":
            exp0 = network_setup[tec]["exporter"].str.replace(message_regions + "_", "")
            imp0 = network_setup[tec]["importer"].str.replace(message_regions + "_", "")
            mode_use = exp0 + "-" + imp0
            df_input_base = message_ix.make_df(
                "input",
                node_loc=network_setup[tec]["exporter"],
                node_origin=network_setup[tec]["exporter"],
                mode=mode_use,
                level=config_dict["export_level"][tec],
                technology=flow_tec,
                commodity=c,
                unit=use_unit,
                time="year",
                time_origin="year",
                **common_years,
            )
        if "shipped" in tec:
            df_input_base["value"] = 1e-6  # Default is 1e-6 GWa
        df_input = pd.concat([df_input, df_input_base])

        # For shipped trade, set up bunker fuels
        if config_dict["bunker_technology"][tec] is not None:
            # Global bunker
            df_input_gbunk = df_input.copy()
            df_input_gbunk["technology"] = "bunker_global" + "_" + c
            df_input_gbunk["mode"] = "M1"
            df_input_gbunk["value"] = 1  # Default is 1 GWa
            df_input = pd.concat([df_input, df_input_gbunk])

        df_input = df_input.drop_duplicates()
        input_out = pd.concat([parameter_outputs[tec]["flow"]["input"], df_input])
        parameter_outputs[tec]["flow"]["input"] = input_out

    return parameter_outputs


# %% Generate flow technology output parameter
def build_flow_output(
    flow_tec: str,
    tec: str,
    network_setup: dict,
    config_dict: dict,
    common_years: dict,
    common_cols: dict,
    parameter_outputs: dict,
    message_regions: str = "R12",
    **kwargs,
):
    """
    Generate flow technology output parameter
    """
    df_output = pd.DataFrame()
    # If pipelines technologies and outputs are bilateral
    if config_dict["flow_constraint"][tec] == "bilateral":
        df_output_base = message_ix.make_df(
            "output",
            node_loc=network_setup[tec]["exporter"],
            node_dest=network_setup[tec]["exporter"],
            technology=flow_tec
            + "_"
            + network_setup[tec]["importer"].str.lower().str.split("_").str[1],
            commodity=config_dict["flow_commodity_output"][tec]
            + "_"
            + network_setup[tec]["importer"].str.lower().str.split("_").str[1],
            unit=config_dict["flow_units"][tec],
            level=config_dict["trade_level"][tec],
            **common_years,
            **common_cols,
        )
        df_output = pd.concat([df_output, df_output_base])

    # If shipping, technologies and outputs are global with diff modes
    elif config_dict["flow_constraint"][tec] == "global":
        df_output_base = message_ix.make_df(
            "output",
            node_loc=network_setup[tec]["exporter"],
            node_dest=message_regions + "_GLB",
            mode="M1",
            technology=flow_tec,
            commodity=config_dict["flow_commodity_output"][tec],
            unit=config_dict["flow_units"][tec],
            level=config_dict["trade_level"][tec],
            time="year",
            time_dest="year",
            **common_years,
        )
        df_output = pd.concat([df_output, df_output_base])

    # For shipped trade, set up bunker fuels
    if config_dict["bunker_technology"][tec] is not None:
        # Global bunker
        df_output_gbunk = df_output.copy()
        df_output_gbunk["technology"] = (
            "bunker_global_" + config_dict["flow_fuel_input"][tec][flow_tec][0]
        )
        df_output_gbunk["node_dest"] = message_regions + "_GLB"
        df_output_gbunk["level"] = "bunker"
        df_output_gbunk["unit"] = "GWa"
        df_output_gbunk["mode"] = "M1"
        df_output_gbunk["commodity"] = config_dict["flow_fuel_input"][tec][flow_tec][0]
        df_output = pd.concat([df_output, df_output_gbunk])

    df_output = df_output.drop_duplicates()

    df_output["value"] = 1
    output_out = pd.concat([parameter_outputs[tec]["flow"]["output"], df_output])
    parameter_outputs[tec]["flow"]["output"] = output_out

    return parameter_outputs


# %% Generate flow technology capacity constraints parameter
def build_flow_capacity_constraints(
    par: str,
    flow_tec: str,
    tec: str,
    network_setup: dict,
    config_dict: dict,
    common_years: dict,
    common_cols: dict,
    parameter_outputs: dict,
    **kwargs,
):
    """
    Generate flow technology capacity constraints parameter
    """
    for t in ["lo", "up"]:
        if config_dict["flow_constraint"][tec] == "bilateral":
            df_con = message_ix.make_df(
                par + "_" + t,
                node_loc=network_setup[tec]["exporter"],
                technology=flow_tec
                + "_"
                + network_setup[tec]["importer"].str.lower().str.split("_").str[1],
                unit=config_dict["flow_units"][tec],
                **common_years,
                **common_cols,
            )
        elif config_dict["flow_constraint"][tec] == "global":
            df_con = message_ix.make_df(
                par + "_" + t,
                node_loc=network_setup[tec]["exporter"],
                technology=flow_tec,
                unit=config_dict["flow_units"][tec],
                **common_years,
                **common_cols,
            )

        con_out = pd.concat([parameter_outputs[tec]["flow"][par + "_" + t], df_con])
        parameter_outputs[tec]["flow"][par + "_" + t] = con_out

    return parameter_outputs


# %% Generate flow technology costs parameter
def build_flow_FIcosts(
    flow_tec: str,
    tec: str,
    network_setup: dict,
    config_dict: dict,
    common_years: dict,
    common_cols: dict,
    parameter_outputs: dict,
    **kwargs,
):
    """
    Generate flow technology costs parameter
    """
    for cost_par in ["fix_cost", "inv_cost"]:
        if config_dict["flow_constraint"][tec] == "bilateral":
            tec_use = (
                flow_tec
                + "_"
                + network_setup[tec]["importer"].str.lower().str.split("_").str[1]
            )
            df_cost = message_ix.make_df(
                cost_par,
                node_loc=network_setup[tec]["exporter"],
                technology=tec_use,
                unit="USD/" + config_dict["flow_units"][tec],
                **common_years,
                **common_cols,
            )
        elif config_dict["flow_constraint"][tec] == "global":
            df_cost = message_ix.make_df(
                cost_par,
                node_loc=network_setup[tec]["exporter"],
                technology=flow_tec,
                unit="USD/" + config_dict["flow_units"][tec],
                **common_years,
                **common_cols,
            )
        df_cost = df_cost.drop_duplicates()

        if "shipped" in tec:
            df_cost["value"] = 0.04  # Default is 0.04 USD/GWa

        cost_out = pd.concat([parameter_outputs[tec]["flow"][cost_par], df_cost])
        parameter_outputs[tec]["flow"][cost_par] = cost_out

    return parameter_outputs


def build_flow_Vcosts(
    flow_tec: str,
    tec: str,
    network_setup: dict,
    config_dict: dict,
    common_years: dict,
    common_cols: dict,
    parameter_outputs: dict,
    message_regions: str = "R12",
    **kwargs,
):
    """
    Generate flow technology variable costs parameter
    """
    if config_dict["flow_constraint"][tec] == "bilateral":
        tec_use = (
            flow_tec
            + "_"
            + network_setup[tec]["importer"].str.lower().str.split("_").str[1]
        )
        df_vcost_base = message_ix.make_df(
            "var_cost",
            node_loc=network_setup[tec]["exporter"],
            technology=tec_use,
            unit="USD/" + config_dict["flow_units"][tec],
            **common_years,
            **common_cols,
        )

    elif config_dict["flow_constraint"][tec] == "global":
        exp0 = network_setup[tec]["exporter"].str.replace(message_regions + "_", "")
        imp0 = network_setup[tec]["importer"].str.replace(message_regions + "_", "")
        mode_use = exp0 + "-" + imp0

        df_vcost_base = message_ix.make_df(
            "var_cost",
            node_loc=network_setup[tec]["exporter"],
            mode=mode_use,
            technology=flow_tec,
            unit="USD/" + config_dict["flow_units"][tec],
            time="year",
            **common_years,
        )
    if "shipped" in tec:
        df_vcost_base["value"] = 0.002  # Default is 0.002 USD/Mt-km

    vcost_out = pd.concat([parameter_outputs[tec]["flow"]["var_cost"], df_vcost_base])
    parameter_outputs[tec]["flow"]["var_cost"] = vcost_out

    return parameter_outputs


# %% Generate flow technology capacity factor parameter
def build_flow_capacity_factor(
    flow_tec: str,
    tec: str,
    network_setup: dict,
    config_dict: dict,
    common_years: dict,
    common_cols: dict,
    parameter_outputs: dict,
    **kwargs,
):
    """
    Generate flow technology capacity factor parameter
    """
    df_cf_base = build_parameterdf(
        "capacity_factor",
        network_df=network_setup[tec],
        col_values=dict(value=1, unit="%"),
        export_only=True,
    )

    if config_dict["flow_constraint"][tec] == "bilateral":
        df_cf_base["technology"] = (
            flow_tec + "_" + df_cf_base["technology"].str.lower().str.split("_").str[-1]
        )
    elif config_dict["flow_constraint"][tec] == "global":
        df_cf_base["technology"] = flow_tec

    df_cf_base = df_cf_base.drop_duplicates()

    if config_dict["bunker_technology"][tec] is not None:
        # Add global bunker fuel technology
        bdf = df_cf_base.copy()
        bdf["technology"] = (
            "bunker_global_" + config_dict["flow_fuel_input"][tec][flow_tec][0]
        )
        df_cf_base = pd.concat([df_cf_base, bdf])

    cf_out = pd.concat([parameter_outputs[tec]["flow"]["capacity_factor"], df_cf_base])
    parameter_outputs[tec]["flow"]["capacity_factor"] = cf_out

    return parameter_outputs


# %% Generate flow technology technical lifetime parameter
def build_flow_technical_lifetime(
    flow_tec: str,
    tec: str,
    network_setup: dict,
    config_dict: dict,
    common_years: dict,
    common_cols: dict,
    parameter_outputs: dict,
    **kwargs,
):
    """
    Generate flow technology technical lifetime parameter
    """
    df_teclt_base = build_parameterdf(
        "technical_lifetime",
        network_df=network_setup[tec],
        col_values=dict(
            value=config_dict["flow_technical_lifetime"][tec],
            unit="y",
        ),
        export_only=True,
    )

    if config_dict["flow_constraint"][tec] == "bilateral":
        df_teclt_base["technology"] = (
            flow_tec
            + "_"
            + df_teclt_base["technology"].str.lower().str.split("_").str[-1]
        )
    elif config_dict["flow_constraint"][tec] == "global":
        df_teclt_base["technology"] = flow_tec

    df_teclt_base = df_teclt_base.drop_duplicates()

    if config_dict["bunker_technology"][tec] is not None:
        # Add global bunker fuel technology
        bdf = df_teclt_base.copy()
        bdf["technology"] = (
            "bunker_global_" + config_dict["flow_fuel_input"][tec][flow_tec][0]
        )
        df_teclt_base = pd.concat([df_teclt_base, bdf])

    teclt_out = pd.concat(
        [parameter_outputs[tec]["flow"]["technical_lifetime"], df_teclt_base]
    )
    parameter_outputs[tec]["flow"]["technical_lifetime"] = teclt_out

    return parameter_outputs


# %% Flow as trade input
def flow_as_trade_input(
    flow_tec: str,
    tec: str,
    network_setup: dict,
    config_dict: dict,
    common_years: dict,
    common_cols: dict,
    parameter_outputs: dict,
    message_regions: str,
    data_path: Path,
):
    """
    Generate flow technology as trade technology input parameter
    """
    if config_dict["flow_constraint"][tec] == "bilateral":
        df_input_flow = message_ix.make_df(
            "input",
            node_loc=network_setup[tec]["exporter"],
            node_origin=network_setup[tec]["exporter"],
            technology=network_setup[tec]["export_technology"],
            commodity=config_dict["flow_commodity_output"][tec]
            + "_"
            + network_setup[tec]["importer"].str.lower().str.split("_").str[1],
            unit=config_dict["flow_units"][tec],
            level=config_dict["trade_level"][tec],
            **common_years,
            **common_cols,
        ).drop_duplicates()
    elif config_dict["flow_constraint"][tec] == "global":
        df_input_flow = message_ix.make_df(
            "input",
            node_loc=network_setup[tec]["exporter"],
            node_origin=message_regions + "_GLB",
            technology=network_setup[tec]["export_technology"],
            commodity=config_dict["flow_commodity_output"][tec],
            unit=config_dict["flow_units"][tec],
            level=config_dict["trade_level"][tec],
            **common_years,
            **common_cols,
        ).drop_duplicates()

    # For shipped commodities calculate capacities based on distance & energy content
    distance_df = pd.read_csv(
        os.path.join(data_path, "distances", message_regions + "_distances.csv")
    )
    energycontent_df = pd.read_excel(os.path.join(data_path, "specific_energy.xlsx"))
    if config_dict["trade_units"][tec] == "GWa":
        tradecom = config_dict["trade_commodity"][tec]
        speccont = energycontent_df[energycontent_df["Commodity"] == tradecom]
        speccont = speccont["Specific Energy (GWa/Mt)"].reset_index(drop=True)[0]
    else:  # e.g., for materials
        speccont = 1

    mult_df = distance_df.copy()
    mult_df["node_loc"] = mult_df["Node1"]
    mult_df["technology"] = (
        config_dict["trade_technology"][tec]
        + "_exp_"
        + mult_df["Node2"].str.lower().str.split("_").str[1]
    )
    mult_df["energy_content"] = speccont
    mult_df["multiplier"] = mult_df["Distance_km"] / mult_df["energy_content"]
    mult_df = mult_df[["node_loc", "technology", "multiplier"]].drop_duplicates()

    df_input_flow = df_input_flow.merge(
        mult_df,
        left_on=["node_loc", "technology"],
        right_on=["node_loc", "technology"],
        how="left",
    )
    df_input_flow["value"] = df_input_flow["multiplier"]
    df_input_flow = df_input_flow[message_ix.make_df("input").columns]

    input_out = pd.concat([parameter_outputs[tec]["trade"]["input"], df_input_flow])
    parameter_outputs[tec]["trade"]["input"] = input_out

    return parameter_outputs


# %% Export edit files
def export_edit_files(
    covered_tec: list[str],
    log: logging.Logger,
    data_path: Path,
    parameter_outputs: dict,
):
    """
    Export edit files
    """
    for tec in covered_tec:
        log.info(f"Exporting trade parameters for {tec}")
        for parname in parameter_outputs[tec]["trade"].keys():
            parameter_outputs[tec]["trade"][parname].to_csv(
                os.path.join(data_path, tec, "edit_files", parname + ".csv"),
                index=False,
            )
            log.info(f"...trade {parname}")
        for parname in parameter_outputs[tec]["flow"].keys():
            parameter_outputs[tec]["flow"][parname].to_csv(
                os.path.join(
                    data_path, tec, "edit_files", "flow_technology", parname + ".csv"
                ),
                index=False,
            )
            log.info(f"...flow {parname}")

    ## Transfer files from edit to bare if they do not already exist
    for tec in covered_tec:
        reqpar_list = [
            os.path.join("capacity_factor.csv"),
            os.path.join("input.csv"),
            os.path.join("output.csv"),
            os.path.join("technical_lifetime.csv"),
            os.path.join(
                "relation_activity_regional_exp.csv"
            ),  # necessary for legacy reporting
            os.path.join("flow_technology", "capacity_factor.csv"),
            os.path.join("flow_technology", "input.csv"),
            os.path.join("flow_technology", "output.csv"),
            os.path.join("flow_technology", "technical_lifetime.csv"),
        ]
        for reqpar in reqpar_list:
            if not os.path.isfile(os.path.join(data_path, tec, "bare_files", reqpar)):
                base_file = os.path.join(data_path, tec, "edit_files", reqpar)
                dest_file = os.path.join(data_path, tec, "bare_files", reqpar)
                shutil.copy2(base_file, dest_file)
                log.info(f"Copied file from edit to bare: {reqpar}")
    ## Transfer cost parameters for flow technologies using shipping from edit to bare
    for tec in covered_tec:
        if "shipped" in tec:
            required_parameters = [
                os.path.join("flow_technology", "var_cost.csv"),
                os.path.join("flow_technology", "inv_cost.csv"),
            ]
            for reqpar in required_parameters:
                if not os.path.isfile(
                    os.path.join(data_path, tec, "bare_files", reqpar)
                ):
                    base_file = os.path.join(data_path, tec, "edit_files", reqpar)
                    dest_file = os.path.join(data_path, tec, "bare_files", reqpar)
                    shutil.copy2(base_file, dest_file)
                    log.info(f"Copied file from edit to bare: {reqpar}")


# %% Main function to generate bare sheets
def generate_edit_files(
    log,
    project_name: str | None = None,
    config_name: str | None = None,
    message_regions: str = "R12",
):
    """
    Generate bare sheets to collect required parameters

    Args:
        log (log, required): Log file to track progress
        project_name (str, optional): Project name (message_ix_models/project/[THIS])
        config_name (str, optional): Name of the config file.
            If None, uses default config from data/bilateralize/config_default.yaml
        message_regions (str, optional): Default is R12 regionality
    """
    data_path = package_data_path("bilateralize")
    data_path = Path(os.path.join(os.path.dirname(data_path), "bilateralize"))

    # Load config
    log.info("Loading config file")
    config_base, config_path, config_tec = load_config(
        project_name, config_name, load_tec_config=True
    )

    # Retrieve config sections
    message_regions = config_base.get("scenario", {}).get("regions")
    covered_tec = config_base.get("covered_trade_technologies", {})

    config_dict: Dict[str, Dict[str, Any]] = {}
    for tec in covered_tec:
        tec_dict = config_tec.get(tec).get(tec + "_trade", {})
        for k in tec_dict.keys():
            if k not in config_dict.keys():
                config_dict[k] = {}
            config_dict[k][tec] = tec_dict[k]

    # Create folders for trade technologies (if missing)
    folders_for_trade(covered_tec)

    # Generate full combination of nodes to build technology-specific network
    network_setup = define_networks(
        log, message_regions, covered_tec, config_dict, data_path
    )

    # Common values across parameters
    common_years = dict(
        year_vtg="broadcast", year_act="broadcast", year_rel="broadcast"
    )
    common_cols = dict(mode="M1", time="year", time_origin="year", time_dest="year")

    parameter_outputs: Dict[str, Dict[str, Any]] = {}  # This will get populated
    for tec in covered_tec:
        parameter_outputs[tec] = dict(trade=dict(), flow=dict())

    # Populate parameter_outputs dictionaries
    for tec in covered_tec:
        tec_args = {
            "tec": tec,
            "network_setup": network_setup,
            "config_dict": config_dict,
            "common_years": common_years,
            "common_cols": common_cols,
            "parameter_outputs": parameter_outputs,
        }

        # Trade input
        parameter_outputs = build_input(**tec_args)
        # Trade output
        parameter_outputs = build_output(**tec_args)

        # Trade technical lifetime
        parameter_outputs = build_technical_lifetime(**tec_args)

        # Trade historical activity
        parameter_outputs = build_historical_activity(**tec_args)

        # Trade costs
        parameter_outputs = build_costs(**tec_args)

        # Trade capacity factor
        parameter_outputs = build_capacity_factor(**tec_args)

        # Trade constraints
        parameter_outputs = build_constraints(**tec_args)

        # Trade emission factor
        parameter_outputs = build_emission_factor(**tec_args)

        # For gas- imports require relation to domestic_coal and domestic_gas
        parameter_outputs = build_domestic_coalgas_relation(**tec_args)

        # Accounting relations
        parameter_outputs = build_accounting_relations(**tec_args)

    ## FLOW TECHNOLOGY ##
    #####################
    # Create file: Input
    for tec in covered_tec:
        parameter_outputs[tec]["flow"]["input"] = pd.DataFrame()
        parameter_outputs[tec]["flow"]["output"] = pd.DataFrame()

        for par in ["growth_new_capacity", "initial_new_capacity"]:
            parameter_outputs[tec]["flow"][par + "_up"] = pd.DataFrame()
            parameter_outputs[tec]["flow"][par + "_lo"] = pd.DataFrame()

        parameter_outputs[tec]["flow"]["fix_cost"] = pd.DataFrame()
        parameter_outputs[tec]["flow"]["inv_cost"] = pd.DataFrame()
        parameter_outputs[tec]["flow"]["var_cost"] = pd.DataFrame()
        parameter_outputs[tec]["flow"]["capacity_factor"] = pd.DataFrame()
        parameter_outputs[tec]["flow"]["technical_lifetime"] = pd.DataFrame()

        for flow_tec in config_dict["flow_technologies"][tec]:
            flow_args = {
                "flow_tec": flow_tec,
                "tec": tec,
                "network_setup": network_setup,
                "config_dict": config_dict,
                "common_years": common_years,
                "common_cols": common_cols,
                "parameter_outputs": parameter_outputs,
                "message_regions": message_regions,
                "data_path": data_path,
            }

            # Flow input
            parameter_outputs = build_flow_input(**flow_args)

            # Flow output
            parameter_outputs = build_flow_output(**flow_args)

            # Flow capacity constraints
            for par in ["growth_new_capacity", "initial_new_capacity"]:
                parameter_outputs = build_flow_capacity_constraints(
                    par=par, **flow_args
                )

            # Flow costs
            parameter_outputs = build_flow_FIcosts(**flow_args)
            parameter_outputs = build_flow_Vcosts(**flow_args)

            # Flow capacity factor
            parameter_outputs = build_flow_capacity_factor(**flow_args)

            # Flow technical lifetime
            parameter_outputs = build_flow_technical_lifetime(**flow_args)

            # Add flow technology outputs as an input into the trade technology
            parameter_outputs = flow_as_trade_input(**flow_args)

    ## Export files
    export_edit_files(covered_tec, log, data_path, parameter_outputs)


def prepare_edit_files(
    project_name: str | None = None,
    config_name: str | None = None,
    P_access: bool = True,
):
    """
    Prepare edit files for bilateralize tool

    Args:
        project_name: Name of the project (e.g., 'newpathways')
        config_name: Name of the config file (e.g., 'config.yaml')
        P_access: User has access to P: drive
    """
    # Get logger
    log = get_logger(__name__)

    # Bring in configuration
    config, config_path = load_config(
        project_name=project_name, config_name=config_name
    )

    covered_tec = config["covered_trade_technologies"]
    message_regions = config["scenario"]["regions"]

    data_path = package_data_path("bilateralize")
    data_path = Path(os.path.join(os.path.dirname(data_path), "bilateralize"))

    # Calculate distances
    calculate_distance(message_regions)

    # Generate bare sheets
    generate_edit_files(
        log=log,
        project_name=project_name,
        config_name=config_name,
        message_regions=message_regions,
    )

    # Import calibration files from Global Energy Monitor
    if P_access is True:
        import_gem(
            input_file="GEM-GGIT-Gas-Pipelines-2024-12.csv",
            # input_sheet="Gas Pipelines 2024-12-17",
            trade_technology="gas_piped",
            flow_technology="gas_pipe",
            flow_commodity="gas_pipeline_capacity",
            project_name=project_name,
            config_name=config_name,
        )

        for tradetec in ["crudeoil_piped", "foil_piped", "loil_piped"]:
            import_gem(
                input_file="GEM-GOIT-Oil-NGL-Pipelines-2025-03.csv",
                # input_sheet="Pipelines",
                trade_technology=tradetec,
                flow_technology="oil_pipe",
                flow_commodity="oil_pipeline_capacity",
                project_name=project_name,
                config_name=config_name,
            )

        # Add MariTEAM calibration for maritime shipping
        calibrate_mariteam(
            covered_tec,
            message_regions,
            project_name=project_name,
            config_name=config_name,
        )

        # Add variable costs for shipped commodities
        costdf = build_historical_price(
            message_regions, project_name=project_name, config_name=config_name
        )
        costdf["technology"] = costdf["technology"].str.replace("ethanol_", "eth_")
        costdf["technology"] = costdf["technology"].str.replace("fueloil_", "foil_")

        for tec in [i for i in covered_tec if i != "gas_piped"]:
            log.info("Add fix cost for " + tec)

            if "piped" in tec:
                tec_shipped = tec.replace("piped", "shipped")
                add_df = costdf[costdf["technology"].str.contains(tec_shipped)].copy()
                add_df["technology"] = add_df["technology"].str.replace(
                    "shipped", "piped"
                )
            else:
                add_df = costdf[costdf["technology"].str.contains(tec)]

            col_list = add_df.columns
            mean_cost = add_df["value"].mean()

            input_df = pd.read_csv(
                os.path.join(data_path, tec, "edit_files", "input.csv")
            )
            input_df = input_df[
                ["node_loc", "technology", "year_act", "year_vtg"]  # "mode", "time"
            ].drop_duplicates()
            add_df = input_df.merge(
                add_df,
                left_on=[
                    "node_loc",
                    "technology",
                    "year_act",
                    "year_vtg",
                ],
                right_on=[
                    "node_loc",
                    "technology",
                    "year_act",
                    "year_vtg",
                ],
                how="left",
            )

            add_df["value"] = np.where(
                add_df["value"].isnull(), mean_cost, add_df["value"]
            )
            add_df["value"] = round(add_df["value"] / 5, 0)

            add_df["unit"] = "USD/GWa"

            add_df = add_df[col_list]

            add_df = add_df[~add_df["technology"].str.contains("_imp")]

            add_df = add_df[
                ["node_loc", "technology", "year_act", "year_vtg", "value", "unit"]
            ]
            add_df = add_df.drop_duplicates()

            add_df.to_csv(
                os.path.join(data_path, tec, "edit_files", "fix_cost.csv"), index=False
            )
            add_df.to_csv(
                os.path.join(data_path, tec, "bare_files", "fix_cost.csv"), index=False
            )
