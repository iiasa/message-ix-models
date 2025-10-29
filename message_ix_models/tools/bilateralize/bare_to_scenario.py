# -*- coding: utf-8 -*-
"""
Move data from bare files to a dictionary to update a MESSAGEix scenario

This script is the second step in implementing the bilateralize tool.
It moves data from /data/bilateralize/[your_trade_commodity]/bare_files/
to a dictionary compatible with updating a MESSAGEix scenario.
"""

# Import packages
import logging
import os
import pickle
from pathlib import Path
from typing import Any, Dict

import numpy as np
import pandas as pd

from message_ix_models.tools.bilateralize.historical_calibration import (
    build_hist_new_capacity_flow,
    build_hist_new_capacity_trade,
    build_historical_activity,
)
from message_ix_models.tools.bilateralize.utils import get_logger, load_config
from message_ix_models.util import package_data_path


# %% Broadcast vintage years
def broadcast_years(
    df: pd.DataFrame, year_type: str, year_list: list[int]
) -> pd.DataFrame:
    """
    Broadcast vintage, relation, or activity years.

    Args:
        df: Input parameter DataFrame
        year_type: Type of year to broadcast (e.g., 'year_vtg', 'year_rel', 'year_act')
        year_list: List of years to broadcast
    Returns:
        pd.DataFrame: DataFrame with expanded rows for each year
    """
    all_new_rows = []
    for _, row in df.iterrows():
        for y in year_list:
            new_row = row.copy()
            new_row[year_type] = int(y)
            all_new_rows.append(new_row)
    result_df = pd.concat([df, pd.DataFrame(all_new_rows)], ignore_index=True)
    result_df = result_df[result_df[year_type] != "broadcast"]
    return result_df.drop_duplicates()


# %% Broadcast years to create vintage-activity year pairs.
def broadcast_yv_ya(
    df: pd.DataFrame, ya_list: list[int], yv_list: list[int], tec_lifetime: pd.DataFrame
):
    """
    Broadcast years to create vintage-activity year pairs.

    Args:
        df: Input parameter DataFrame
        ya_list: List of activity years to consider
        yv_list: List of vintage years to consider
        tec_lifetime: Technical lifetime of the technology, provided via dataframe
    Returns:
        pd.DataFrame: DataFrame with expanded rows for each vintage-activity year pair
    """
    all_new_rows = []

    tecltdf = tec_lifetime.copy()
    tecltdf["teclt"] = tecltdf["value"]

    lts = df.merge(
        tecltdf[["node_loc", "technology", "teclt"]].drop_duplicates(),
        left_on=["node_loc", "technology"],
        right_on=["node_loc", "technology"],
        how="left",
    )

    # Process each row in the original DataFrame
    for _, row in lts.iterrows():
        teclt_row = row["teclt"]

        # For each activity year
        for ya in ya_list:
            # Get all vintage years that are <=year_act for a period<tec_lifetime
            yv_list = [yv for yv in ya_list if yv <= ya]
            yv_list = [yv for yv in yv_list if yv >= ya - teclt_row]

            # Create new rows for each vintage year
            for yv in yv_list:
                new_row = row.copy()
                new_row["year_act"] = int(ya)
                new_row["year_vtg"] = int(yv)
                all_new_rows.append(new_row)

    # Combine original DataFrame with new rows
    result_df = pd.DataFrame(all_new_rows).drop(["teclt"], axis=1)
    result_df = result_df[result_df["year_vtg"] != "broadcast"]
    return result_df


# %% Full broadcast function
def full_broadcast(
    data_dict: dict,
    tec: str,
    ty: str,
    ya_list: list[int],
    yv_list: list[int],
    log: logging.Logger,
) -> dict:
    """
    Full broadcast function
    Args:
        data_dict: Dictionary of parameter dataframes
        tec: Technology name
        ty: Type of parameter (trade or flow)
        ya_list: List of activity years
        yv_list: List of vintage years
        log: Logger
    Outputs:
        data_dict: Dictionary of parameter dataframes with broadcasted years
    """
    for i in data_dict[ty].keys():
        if "year_rel" in data_dict[ty][i].columns:
            log.info(f"Parameter {i} in {tec} {ty} broadcasted for year_rel")
            if data_dict[ty][i]["year_rel"].iloc[0] == "broadcast":
                data_dict[ty][i] = broadcast_years(
                    df=data_dict[ty][i], year_type="year_rel", year_list=ya_list
                )
                data_dict[ty][i]["year_act"] = data_dict[ty][i]["year_rel"]
        else:
            pass

        if (
            "year_vtg" in data_dict[ty][i].columns
            and "year_act" in data_dict[ty][i].columns
        ):
            if (
                data_dict[ty][i]["year_vtg"].iloc[0] == "broadcast"
                and data_dict[ty][i]["year_act"].iloc[0] == "broadcast"
            ):
                log.info(f"{i} in {tec} {ty} broadcasted for year_vtg+year_act")
                teclt_df = data_dict[ty]["technical_lifetime"].copy()
                data_dict[ty][i] = broadcast_yv_ya(
                    df=data_dict[ty][i],
                    ya_list=ya_list,
                    yv_list=yv_list,
                    tec_lifetime=teclt_df,
                )
            elif (
                data_dict[ty][i]["year_vtg"].iloc[0] == "broadcast"
                and data_dict[ty][i]["year_act"].iloc[0] != "broadcast"
            ):
                log.info(f"{i} in {tec} {ty} broadcasted for year_vtg")
                data_dict[ty][i] = broadcast_years(
                    df=data_dict[ty][i], year_type="year_vtg", year_list=yv_list
                )
        elif (
            "year_vtg" in data_dict[ty][i].columns
            and "year_act" not in data_dict[ty][i].columns
        ):
            if data_dict[ty][i]["year_vtg"].iloc[0] == "broadcast":
                log.info(f"{i} in {tec} {ty} broadcasted for year_vtg")
                data_dict[ty][i] = broadcast_years(
                    df=data_dict[ty][i], year_type="year_vtg", year_list=yv_list
                )
        elif (
            "year_vtg" not in data_dict[ty][i].columns
            and "year_act" in data_dict[ty][i].columns
        ):
            if data_dict[ty][i]["year_act"].iloc[0] == "broadcast":
                log.info(f"{i} in {tec} {ty} broadcasted for year_act")
                data_dict[ty][i] = broadcast_years(
                    df=data_dict[ty][i], year_type="year_act", year_list=ya_list
                )
        else:
            pass
    return data_dict


# %% Build out bare sheets
def build_parameter_sheets(
    log, project_name: str | None = None, config_name: str | None = None
):
    """
    Read the input csv files and build the tech sets and parameters.

    Args:
        project_name (str, optional): Project name (message_ix_models/project/[THIS])
        config_name (str, optional): Name of the config file.
            If None, uses default config from data/bilateralize/config_default.yaml
    Returns:
        outdict: Dictionary of parameter dataframes
    """
    # Load config
    config, config_path = load_config(project_name, config_name)

    covered_tec = config.get("covered_trade_technologies", {})

    outdict = dict()

    ya_list = config["timeframes"]["year_act_list"]
    yv_list = config["timeframes"]["year_vtg_list"]

    for tec in covered_tec:
        tecpath = os.path.join(Path(package_data_path("bilateralize")), tec)

        data_dict: Dict[str, Dict[str, Any]] = {"trade": {}, "flow": {}}

        for ty in ["trade", "flow"]:
            if ty == "trade":
                tpath = os.path.join(tecpath, "bare_files")
            elif ty == "flow":
                tpath = os.path.join(tecpath, "bare_files", "flow_technology")

            csv_files = [f for f in Path(tpath).glob("*.csv")]

            for csv_file in csv_files:
                key = csv_file.stem
                data_dict[ty][key] = pd.read_csv(csv_file)

        # Broadcast the data
        for ty in ["trade", "flow"]:
            data_dict = full_broadcast(
                data_dict=data_dict,
                tec=tec,
                ty=ty,
                ya_list=ya_list,
                yv_list=yv_list,
                log=log,
            )

        # Imports do not vintage
        for par in ["capacity_factor", "input", "output"]:
            vdf = data_dict["trade"][par]
            vdf = vdf[
                (
                    (vdf["technology"].str.contains("_imp"))
                    & (vdf["year_vtg"] == vdf["year_act"])
                )
                | (vdf["technology"].str.contains("_exp_"))
            ]
            data_dict["trade"][par] = vdf

        # Variable costs for flows should not broadcast
        for par in ["var_cost"]:
            if par in list(data_dict["flow"].keys()):
                vdf = data_dict["flow"][par]
                vdf = vdf[vdf["year_act"] == vdf["year_vtg"]]
                data_dict["flow"][par] = vdf

        outdict[tec] = data_dict

    return outdict


def bare_to_scenario(
    project_name: str | None = None,
    config_name: str | None = None,
    scenario_parameter_name: str = "scenario_parameters.pkl",
    p_drive_access: bool = False,
):
    """
    Move data from bare files to a dictionary to update a MESSAGEix scenario

    Args:
        project_name: Name of the project (e.g., 'newpathways')
        config_name: Name of the config file (e.g., 'config.yaml')
        scenario_parameter_name: Name of the scenario parameter file

    Output:
        trade_dict: Dictionary compatible with updating a MESSAGEix scenario
    """
    # Bring in configuration
    config, config_path, tec_config = load_config(
        project_name=project_name, config_name=config_name, load_tec_config=True
    )

    covered_tec = config["covered_trade_technologies"]
    message_regions = config["scenario"]["regions"]

    # Get logger
    log = get_logger(__name__)

    # Read and inflate sheets based on model horizon
    trade_dict = build_parameter_sheets(
        log=log, project_name=project_name, config_name=config_name
    )

    if p_drive_access:
        # Historical calibration for trade technology
        histdf = build_historical_activity(
            message_regions=message_regions,
            project_name=project_name,
            config_name=config_name,
            reimport_BACI=False,
        )
        histdf = histdf[histdf["year_act"].isin([2000, 2005, 2010, 2015, 2020, 2023])]
        histdf["year_act"] = np.where(
            (histdf["year_act"] == 2023),
            2025,  # TODO: 2023 to 2025 only for now
            histdf["year_act"],
        )
        histdf = histdf[histdf["value"] > 0]
        histdf["technology"] = histdf["technology"].str.replace("ethanol_", "eth_")
        histdf["technology"] = histdf["technology"].str.replace("fueloil_", "foil_")

        histnc = build_hist_new_capacity_trade(
            message_regions=message_regions,
            project_name=project_name,
            config_name=config_name,
        )

        hist_tec = {}
        for tec in [
            c
            for c in covered_tec
            if c not in ["crudeoil_piped", "foil_piped", "loil_piped"]
        ]:
            add_tec = tec_config[tec][tec + "_trade"]["trade_technology"] + "_exp"
            hist_tec[tec] = add_tec

        for tec in hist_tec.keys():
            log.info("Add historical activity for " + tec)
            add_df = histdf[histdf["technology"].str.contains(hist_tec[tec])]
            trade_dict[tec]["trade"]["historical_activity"] = add_df

            log.info("Add historical new capacity for " + tec)
            add_df = histnc[histnc["technology"].str.contains(hist_tec[tec])]
            trade_dict[tec]["trade"]["historical_new_capacity"] = add_df

        # Historical new capacity for maritime shipping
        shipping_fuel_dict = config["shipping_fuels"]
        # TODO: Add coal
        hist_cr_loil = build_hist_new_capacity_flow(
            infile="Crude Tankers.csv",
            ship_type="crudeoil_tanker_loil",
            project_name=project_name,
            config_name=config_name,
        )
        hist_lh2_loil = build_hist_new_capacity_flow(
            infile="LH2 Tankers.csv",
            ship_type="lh2_tanker_loil",
            project_name=project_name,
            config_name=config_name,
        )
        hist_lng = pd.DataFrame()
        for f in ["loil", "LNG"]:
            hist_lng_f = build_hist_new_capacity_flow(
                infile="LNG Tankers.csv",
                ship_type="LNG_tanker_" + f,
                project_name=project_name,
                config_name=config_name,
            )
            hist_lng_f["value"] *= shipping_fuel_dict["LNG_tanker"]["LNG_tanker_" + f]
            hist_lng = pd.concat([hist_lng, hist_lng_f])

        hist_oil = pd.DataFrame()
        for f in ["loil", "foil", "eth"]:
            hist_oil_f = build_hist_new_capacity_flow(
                infile="Oil Tankers.csv",
                ship_type="oil_tanker_" + f,
                project_name=project_name,
                config_name=config_name,
            )
            hist_oil_f["value"] *= shipping_fuel_dict["oil_tanker"]["oil_tanker_" + f]
            hist_oil = pd.concat([hist_oil, hist_oil_f])
        hist_eth = hist_oil[hist_oil["technology"] != "oil_tanker_foil"]

        trade_dict["crudeoil_shipped"]["flow"]["historical_new_capacity"] = hist_cr_loil
        trade_dict["lh2_shipped"]["flow"]["historical_new_capacity"] = hist_lh2_loil
        trade_dict["LNG_shipped"]["flow"]["historical_new_capacity"] = hist_lng
        trade_dict["eth_shipped"]["flow"]["historical_new_capacity"] = hist_eth
        trade_dict["foil_shipped"]["flow"]["historical_new_capacity"] = hist_oil
        trade_dict["loil_shipped"]["flow"]["historical_new_capacity"] = hist_oil

        # Historical activity should only be added for technologies in input
        for tec in covered_tec:
            input_tecs = trade_dict[tec]["trade"]["input"]["technology"]

            if "historical_activity" in trade_dict[tec]["trade"].keys():
                tdf = trade_dict[tec]["trade"]["historical_activity"]
                tdf = tdf[tdf["technology"].isin(input_tecs)]
                trade_dict[tec]["trade"]["historical_activity"] = tdf

            if "historical_new_capacity" in trade_dict[tec]["trade"].keys():
                tdf = trade_dict[tec]["trade"]["historical_new_capacity"]
                tdf = tdf[tdf["technology"].isin(input_tecs)]
                trade_dict[tec]["trade"]["historical_new_capacity"] = tdf

    # Ensure flow technologies are only added once
    covered_flow_tec: list[str] = []
    for tec in covered_tec:
        flow_tecs = list(trade_dict[tec]["flow"]["input"]["technology"].unique())
        for par in trade_dict[tec]["flow"].keys():
            trade_dict[tec]["flow"][par] = trade_dict[tec]["flow"][par][
                ~trade_dict[tec]["flow"][par]["technology"].isin(covered_flow_tec)
            ]
        covered_flow_tec = covered_flow_tec + flow_tecs

    # Save trade_dictionary
    tdf = os.path.join(os.path.dirname(config_path), scenario_parameter_name)
    with open(tdf, "wb") as file_handler:
        pickle.dump(trade_dict, file_handler)

    return trade_dict
