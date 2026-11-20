# -*- coding: utf-8 -*-
"""
Historical Calibration
"""

# Import packages
import os
from pathlib import Path

import message_ix
import numpy as np
import pandas as pd
import yaml

from message_ix_models.tools.bilateralize.calculate_distance import calculate_pipeline_distances
from message_ix_models.tools.bilateralize.utils import load_config
from message_ix_models.util import package_data_path

oil_pipeline_file = "GEM-GOIT-Oil-NGL-Pipelines-2025-03.xlsx"
oil_pipeline_sheet = "Pipelines"

gas_pipeline_file = "GEM-GGIT-Gas-Pipelines-2024-12.xlsx"
gas_pipeline_sheet = "Gas Pipelines 2024-12-17"


# Set up MESSAGE regions
def gem_region(project_name: str | None = None, config_name: str | None = None):
    """
    Set up MESSAGE regions

    Args:
        project_name: Name of project
        config_name: Name of config file
    """

    config, config_path = load_config(
        project_name=project_name, config_name=config_name
    )
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    message_regions = config["scenario"]["regions"]

    full_path = package_data_path("node", message_regions + ".yaml")
    with open(full_path, "r", encoding="utf-8") as f:
        message_regions = yaml.safe_load(f)
    message_regions_list = [
        r for r in message_regions.keys() if r not in ["World", "GLB"]
    ]

    return message_regions_list, message_regions


# Import files
def import_gem(
    input_file: str | Path,
    # input_sheet: str,
    trade_technology: str,
    flow_technology: str,
    flow_commodity: str,
    project_name: str | None = None,
    config_name: str | None = None,
    first_model_year: int = 2030,
    regional_specification: str = "R12",
    inv_cost_per_km: float | None = None,
    ref_line_capacity: float = 20.0,
    statuses: tuple[str, ...] | None = ("operating",),
    min_vintage: int | None = None,
):
    """
    Import Global Energy Monitor data

    Args:
        input_file: Name of input file
        input_sheet: Name of input sheet
        trade_technology: Name of trade technology
        flow_technology: Name of flow technology
        flow_commodity: Name of flow commodity
        project_name: Name of project
        config_name: Name of config file
        first_model_year: First model year
        inv_cost_per_km: If given, investment cost (MUSD/km) used for every route of
            the flow technology instead of the GEM-derived values. GEM's CostUSD is
            missing for many segments, so cost/length per route is unreliable.
        ref_line_capacity: Capacity (GWa) of one new pipeline, used for routes with
            no GEM data: input = haversine distance (km) / ref_line_capacity.
        statuses: GEM ``Status`` values to keep (default: operating lines only).
            None keeps every status.
        min_vintage: Earliest vintage year. Older lines are assigned this vintage,
            so with a 40-year lifetime they retire in min_vintage + 40. Default: the
            first year of ``timeframes.year_vtg_list`` in the config. Vintages before
            that get no technical_lifetime, so MESSAGE gives them no capacity.
    """

    # Pull in configuration
    config, config_path = load_config(
        project_name=project_name, config_name=config_name
    )
    p_drive = config["p_drive_location"]
    if min_vintage is None:
        min_vintage = min(int(y) for y in config["timeframes"]["year_vtg_list"])

    # Data paths
    data_path = os.path.join(p_drive, "MESSAGE_trade")
    gem_path = os.path.join(data_path, "Global Energy Monitor")

    # df = pd.read_excel(os.path.join(gem_path, input_file), sheet_name=input_sheet)
    df = pd.read_csv(os.path.join(gem_path, input_file), encoding="utf-8")

    df = df[df["StopYear"].isnull()]  # Only continuing projects
    # Only lines in service: GEM also lists cancelled, shelved, mothballed (e.g.
    # Nord Stream 2), idle (e.g. Yamal-Europe) and proposed lines without StopYear
    if statuses is not None and "Status" in df.columns:
        df = df[df["Status"].str.lower().isin([s.lower() for s in statuses])]

    df = df[
        [
            "StartYear1",
            "StartCountry",
            "EndCountry",
            "CapacityBOEd",
            "CostUSD",
            "LengthMergedKm",
        ]
    ].drop_duplicates()

    # Clean up country codes
    cw = pd.read_csv(os.path.join(gem_path, "country_crosswalk.csv"))
    for i in ["Start", "End"]:
        df = df.merge(cw, left_on=i + "Country", right_on="GEM Country", how="left")
        df = df.rename(columns={"ISO": i + "ISO"})

    # Add MESSAGE regions
    message_regions_list, message_regions = gem_region(project_name, config_name)
    df["EXPORTER"] = ""
    df["IMPORTER"] = ""
    for r in message_regions_list:
        df["EXPORTER"] = np.where(
            df["StartISO"].isin(message_regions[r]["child"]), r, df["EXPORTER"]
        )
        df["IMPORTER"] = np.where(
            df["EndISO"].isin(message_regions[r]["child"]), r, df["IMPORTER"]
        )

    # Collapse
    df["CapacityBOEd"] = df["CapacityBOEd"].replace("--", "0", regex=True).astype(float)
    df["CostUSD"] = df["CostUSD"].replace("--", "0", regex=True).astype(float)
    df["LengthMergedKm"] = (
        df["LengthMergedKm"].replace("--", "0", regex=True).astype(float)
    )

    df = df[(~df["CapacityBOEd"].isnull()) & (~df["CostUSD"].isnull())]

    df = df[df["StartYear1"] < first_model_year]  # No planned capacity
    df["YEAR"] = (
        round(df["StartYear1"].astype(float) / 5) * 5
    )  # Round year to the nearest 5
    # Pipelines older than min_vintage (e.g. Transmed 1983, Maghreb-Europe 1996)
    # are given vintage min_vintage: earlier vintages have no technical_lifetime in
    # the scenario, so their capacity would be dropped
    df["YEAR"] = df["YEAR"].clip(lower=min_vintage)

    df = (
        df.groupby(["EXPORTER", "IMPORTER", "YEAR"])[
            ["CapacityBOEd", "CostUSD", "LengthMergedKm"]
        ]
        .sum()
        .reset_index()
    )

    # Convert units
    df["Capacity (BOEa)"] = df["CapacityBOEd"] * 365
    df["Capacity (TJ)"] = df["Capacity (BOEa)"] * 0.006  # BOEa to TJ
    df["Capacity (GWa)"] = df["Capacity (TJ)"] * (3.1712 * 1e-5)  # TJ to GWa

    # Generate investment costs
    df["InvCost (USD/km)"] = (df["CostUSD"]) / df["LengthMergedKm"]
    # TODO: Add industry-specific deflators

    # Generate capacity
    df["Capacity (GWa/km)"] = (df["Capacity (GWa)"]) / df["LengthMergedKm"]

    # Cut down
    df = df[df["EXPORTER"] != df["IMPORTER"]]
    df = df[(df["EXPORTER"] != "") & (df["IMPORTER"] != "")]

    # Base file with all historical timesteps
    hist_base = pd.DataFrame()
    for y in list(range(2000, 2030, 5)):
        ydf = df[["EXPORTER", "IMPORTER"]].drop_duplicates().copy()
        ydf["YEAR"] = y
        hist_base = pd.concat([hist_base, ydf])
    hist_base = hist_base[(hist_base["EXPORTER"] != "") & (hist_base["IMPORTER"] != "")]

    df = hist_base.merge(
        df,
        left_on=["EXPORTER", "IMPORTER", "YEAR"],
        right_on=["EXPORTER", "IMPORTER", "YEAR"],
        how="outer",
    )  # Set 2005 to 0 if missing
    for c in [c for c in df.columns if c not in ["EXPORTER", "IMPORTER", "YEAR"]]:
        df[c] = np.where((df[c].isnull()) & (df["YEAR"] == 2000), 0, df[c])

    df["trade_technology"] = trade_technology
    df["flow_technology"] = flow_technology

    # Output to trade_technology
    export_dir = package_data_path("bilateralize", trade_technology)
    gem_dir_out = os.path.join(os.path.dirname(export_dir), trade_technology, "GEM")

    trade_dir = os.path.join(os.path.dirname(gem_dir_out), "edit_files")
    flow_dir = os.path.join(
        os.path.dirname(gem_dir_out), "edit_files", "flow_technology"
    )
    trade_dir_out = os.path.join(os.path.dirname(gem_dir_out), "bare_files")
    flow_dir_out = os.path.join(
        os.path.dirname(gem_dir_out), "bare_files", "flow_technology"
    )
    if not os.path.isdir(gem_dir_out):
        os.makedirs(Path(gem_dir_out))

    df.to_csv(os.path.join(gem_dir_out, "GEM.csv"))

    # Investment Costs
    inv_cost = (
        df.groupby(["EXPORTER", "IMPORTER"])[["CostUSD", "LengthMergedKm"]]
        .sum()
        .reset_index()
    )
    inv_cost["InvCost (USD/km)"] = (inv_cost["CostUSD"]) / inv_cost["LengthMergedKm"]
    inv_cost = inv_cost[["EXPORTER", "IMPORTER", "InvCost (USD/km)"]].drop_duplicates()
    inv_cost["node_loc"] = inv_cost["EXPORTER"]
    inv_cost["technology"] = (
        flow_technology + "_" + inv_cost["IMPORTER"].str.lower().str.split("_").str[-1]
    )
    inv_cost["value_update"] = inv_cost["InvCost (USD/km)"] / 1e6  # in MUSD/km
    inv_cost = inv_cost[["node_loc", "technology", "value_update"]]
    inv_cost.to_csv(os.path.join(gem_dir_out, "inv_cost_GEM.csv"), index=False)

    basedf = pd.read_csv(os.path.join(flow_dir, "inv_cost.csv"))
    basedf["value"] = 10
    inv_cost = basedf.merge(
        inv_cost,
        left_on=["node_loc", "technology"],
        right_on=["node_loc", "technology"],
        how="left",
    )
    inv_cost["value"] = np.where(
        inv_cost["value_update"] > 0,
        round(inv_cost["value_update"], 0),
        inv_cost["value"],
    )
    inv_cost["value"] = np.where(
        inv_cost["value"] < 2, 2, # Make investment cost floor 2MUSD/km
        inv_cost["value"]
    )
    if inv_cost_per_km is not None:
        inv_cost["value"] = inv_cost_per_km  # Uniform cost on all routes

    inv_cost["year_vtg"] = "broadcast"
    inv_cost["unit"] = "USD/km"
    inv_cost = inv_cost[
        ["node_loc", "technology", "year_vtg", "value", "unit"]
    ].drop_duplicates()
    inv_cost.to_csv(os.path.join(flow_dir, "inv_cost.csv"), index=False)
    inv_cost.to_csv(os.path.join(flow_dir_out, "inv_cost.csv"), index=False)

    # Historical activity (flow)
    hist_act = df[["EXPORTER", "IMPORTER", "YEAR", "LengthMergedKm"]].drop_duplicates()
    hist_act["LengthMergedKm"] = np.where(
        hist_act["LengthMergedKm"].isnull(), 0, hist_act["LengthMergedKm"]
    )
    hist_act["LengthMergedKm"] = hist_act.groupby(["EXPORTER", "IMPORTER"])[
        "LengthMergedKm"
    ].transform(pd.Series.cumsum)
    hist_act["node_loc"] = hist_act["EXPORTER"]
    hist_act["technology"] = (
        flow_technology + "_" + hist_act["IMPORTER"].str.lower().str.split("_").str[-1]
    )
    hist_act["value"] = round(hist_act["LengthMergedKm"], 0)
    hist_act["year_act"] = hist_act["YEAR"].astype(int)
    hist_act["unit"] = "km"
    hist_act["mode"] = "M1"
    hist_act["time"] = "year"
    hist_act = hist_act[
        ["node_loc", "technology", "year_act", "value", "unit", "mode", "time"]
    ]
    hist_act.to_csv(
        os.path.join(gem_dir_out, "historical_activity_GEM.csv"), index=False
    )
    hist_act.to_csv(os.path.join(flow_dir, "historical_activity.csv"), index=False)
    hist_act.to_csv(os.path.join(flow_dir_out, "historical_activity.csv"), index=False)

    # Historical activity (trade for oil pipelines only)
    # Share of oil pipeline capacity used for commodity (crude/light oil/fuel oil)
    share_oil_pipelines = {"crudeoil_piped": 0.8, "loil_piped": 0.1, "foil_piped": 0.1}

    if trade_technology in ["crudeoil_piped", "loil_piped", "foil_piped"]:
        hist_tra = df[
            ["EXPORTER", "IMPORTER", "YEAR", "Capacity (GWa)"]
        ].drop_duplicates()
        hist_tra["Capacity (GWa)"] = np.where(
            hist_tra["Capacity (GWa)"].isnull(), 0, hist_tra["Capacity (GWa)"]
        )
        hist_tra["Capacity (GWa)"] = hist_tra.groupby(["EXPORTER", "IMPORTER"])[
            "Capacity (GWa)"
        ].transform(pd.Series.cumsum)
        hist_tra["node_loc"] = hist_tra["EXPORTER"]
        hist_tra["technology"] = (
            trade_technology
            + "_exp_"
            + hist_tra["IMPORTER"].str.lower().str.split("_").str[-1]
        )

        hist_tra["value"] = round(hist_tra["Capacity (GWa)"], 0)
        hist_tra["value"] = hist_tra["value"] * share_oil_pipelines[trade_technology]

        hist_tra["year_act"] = hist_tra["YEAR"].astype(int)
        hist_tra["unit"] = "GWa"
        hist_tra["mode"] = "M1"
        hist_tra["time"] = "year"
        hist_tra = hist_tra[
            ["node_loc", "technology", "year_act", "value", "unit", "mode", "time"]
        ]
        hist_tra["year_act"] = hist_tra["year_act"].astype(int)
        hist_tra.to_csv(
            os.path.join(gem_dir_out, "historical_activity_trade_GEM.csv"), index=False
        )
        hist_tra.to_csv(os.path.join(trade_dir, "historical_activity.csv"), index=False)
        hist_tra.to_csv(
            os.path.join(trade_dir_out, "historical_activity.csv"), index=False
        )

    # Historical new capacity
    hist_cap = df[["EXPORTER", "IMPORTER", "YEAR", "LengthMergedKm"]]
    hist_cap = hist_cap.rename(columns={"LengthMergedKm": "CAPACITY_KM"})

    hist_cap = (
        hist_cap.groupby(["EXPORTER", "IMPORTER", "YEAR"])["CAPACITY_KM"]
        .sum()
        .reset_index()
    )
    hist_cap = hist_cap.sort_values(by=["EXPORTER", "IMPORTER", "YEAR"], ascending=True)
    hist_cap = hist_cap[
        (hist_cap["YEAR"] < first_model_year) & (hist_cap["YEAR"] > 1999)
    ]

    hist_cap = hist_cap.sort_values(
        by=["EXPORTER", "IMPORTER", "YEAR"], ascending=False
    )
    hist_cap = hist_cap[["EXPORTER", "IMPORTER", "YEAR", "CAPACITY_KM"]]

    hist_cap["CAPACITY_KM"] = hist_cap["CAPACITY_KM"] / 5  # Duration time is 5 years

    hist_cap["node_loc"] = hist_cap["EXPORTER"]
    hist_cap["technology"] = (
        flow_technology + "_" + hist_cap["IMPORTER"].str.lower().str.split("_").str[-1]
    )
    hist_cap["value"] = round(hist_cap["CAPACITY_KM"], 0)
    hist_cap["YEAR"] = hist_cap["YEAR"].astype(int)
    hist_cap = message_ix.make_df(
        "historical_new_capacity",
        node_loc=hist_cap["node_loc"],
        technology=hist_cap["technology"],
        year_vtg=hist_cap["YEAR"],
        value=hist_cap["value"],
        unit="km",
    )
    hist_cap.to_csv(
        os.path.join(gem_dir_out, "historical_new_capacity_GEM.csv"), index=False
    )
    hist_cap.to_csv(os.path.join(flow_dir, "historical_new_capacity.csv"), index=False)
    hist_cap.to_csv(
        os.path.join(flow_dir_out, "historical_new_capacity.csv"), index=False
    )

    # Input: km of pipeline per GWa of trade (K/C)
    # K = total pipeline length on the route, C = total GEM throughput capacity on
    # the route. The flow technology has output = 1 (see prepare_edit), so a route's
    # historical pipeline stock (K km) supports exactly C GWa of trade, and new
    # build costs K/C km per GWa. Use the same vintages as historical_new_capacity.
    route = df[(df["YEAR"] > 1999) & (df["YEAR"] < first_model_year)]
    route = (
        route.groupby(["EXPORTER", "IMPORTER"])[["LengthMergedKm", "Capacity (GWa)"]]
        .sum()
        .reset_index()
    )
    route = route[(route["LengthMergedKm"] > 0) & (route["Capacity (GWa)"] > 0)]
    route["km_per_GWa"] = route["LengthMergedKm"] / route["Capacity (GWa)"]
    inputdf = route[["EXPORTER", "IMPORTER", "km_per_GWa"]]

    # Routes with GEM data in only one direction use the reverse direction's value
    reverse = inputdf.rename(columns={"EXPORTER": "IMPORTER", "IMPORTER": "EXPORTER"})
    reverse = reverse.merge(
        inputdf[["EXPORTER", "IMPORTER"]], how="left", indicator=True
    )
    reverse = reverse[reverse["_merge"] == "left_only"].drop(columns="_merge")
    inputdf = pd.concat([inputdf, reverse], ignore_index=True)

    inputdf["node_loc"] = inputdf["EXPORTER"]
    inputdf["technology"] = (
        trade_technology
        + "_exp_"
        + inputdf["IMPORTER"].str.lower().str.split("_").str[-1]
    )
    inputdf["value_update"] = round(inputdf["km_per_GWa"], 1)
    inputdf["commodity"] = (
        flow_commodity + "_" + inputdf["IMPORTER"].str.lower().str.split("_").str[-1]
    )
    inputdf = inputdf[["node_loc", "technology", "value_update", "commodity"]]  # km/GWa

    basedf = pd.read_csv(os.path.join(trade_dir, "input.csv"))
    
    # Where historical pipeline is missing, assume one new line of length equal to
    # the haversine distance and capacity ref_line_capacity (GWa)
    haversine_distances = calculate_pipeline_distances(regional_specification)

    basedf['Node1'] = basedf['node_loc']
    basedf['Node2'] = regional_specification + '_' + basedf['technology'].str.split('_').str[-1].str.upper()
    basedf = basedf.merge(haversine_distances, left_on=["Node1", "Node2"], right_on=["Node1", "Node2"], how="left")

    basedf["value"] = np.where(
        basedf["commodity"].str.contains(flow_commodity),
        basedf["Distance_km"] / ref_line_capacity,
        basedf["value"],
    )
    basedf = basedf.drop(["Node1", "Node2", "Distance_km"], axis=1)
    
    inputdf = basedf.merge(
        inputdf,
        left_on=["node_loc", "technology", "commodity"],
        right_on=["node_loc", "technology", "commodity"],
        how="left",
    )
    inputdf["value"] = np.where(
        (~inputdf["value_update"].isnull())
        & (inputdf["value_update"] < 10000)
        & (inputdf["commodity"].str.contains(flow_commodity)),
        inputdf["value_update"],
        inputdf["value"],
    )
    inputdf = inputdf.drop(["value_update"], axis=1)
    
    inputdf.to_csv(os.path.join(gem_dir_out, "inputs_GEM.csv"), index=False)
    inputdf.to_csv(os.path.join(trade_dir, "input.csv"), index=False)
    inputdf.to_csv(os.path.join(trade_dir_out, "input.csv"), index=False)
