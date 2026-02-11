# -*- coding: utf-8 -*-
"""
Historical Calibration
"""

# Import packages
import os
import pickle

import message_ix
import numpy as np
import pandas as pd
import yaml

from message_ix_models.tools.bilateralize.utils import load_config
from message_ix_models.util import package_data_path

# Reimport large files?
reimport_IEA = False
reimport_BACI = False


# Set up data paths
def setup_datapath(project_name: str | None = None, config_name: str | None = None):
    """
    Set up data paths.

    Args:
        project_name: Name of project
        config_name: Name of config file
    Outputs:
        data_paths: Dictionary of data paths
    """
    # Pull in configuration
    config, config_path = load_config(
        project_name=project_name, config_name=config_name
    )
    p_drive = config["p_drive_location"]

    # Data paths
    data_path = os.path.join(p_drive, "MESSAGE_trade")
    iea_path = os.path.join(data_path, "IEA")
    iea_diagnostics_path = os.path.join(iea_path, "diagnostics")
    iea_web_path = os.path.join(iea_path, "WEB2025")
    iea_gas_path = os.path.join(iea_path, "NATGAS")
    baci_path = os.path.join(data_path, "UN Comtrade", "BACI")
    imo_path = os.path.join(data_path, "IMO")

    data_paths = dict(
        iea_web=iea_web_path,
        iea_gas=iea_gas_path,
        iea_diag=iea_diagnostics_path,
        baci=baci_path,
        imo=imo_path,
    )

    return data_paths


# Dictionaries of ISO - IEA - MESSAGE Regions
def generate_cfdict(
    message_regions: str,
    project_name: str | None = None,
    config_name: str | None = None,
):
    """
    Generate conversion factor dictionary.

    Args:
        message_regions: Regional resolution
        project_name: Name of project (e.g., 'newpathways')
        config_name: Name of config file
    """
    dict_dir = package_data_path("node",message_regions + ".yaml")
    with open(dict_dir, "r") as f:
        dict_message_regions = yaml.safe_load(f)
    region_list = [i for i in list(dict_message_regions.keys()) if i != "World"]

    data_paths = setup_datapath(project_name=project_name, config_name=config_name)

    print("Import conversion factors")
    cfdf = pd.read_csv(
        os.path.join(data_paths["iea_web"], "CONV.txt"),
        sep=r"\s+",
        header=None,
        encoding="windows-1252",
    )
    cfdf.columns = ["units", "country", "commodity", "metric", "year", "value"]
    cfdf = cfdf[cfdf["year"] > 1990]
    cfdf = cfdf[cfdf["units"] == "KJKG"]  # KJ/KG
    cfdf = cfdf[cfdf["metric"].isin(["NAVERAGE", "NINDPROD"])]  # Mean NCV and NCV
    cfdf = cfdf[cfdf["value"] != "x"]

    cfdf["conversion (TJ/t)"] = (cfdf["value"].astype(float) * 1000) * (1e-9)

    cf_out = (
        cfdf.groupby(["country", "commodity"])[["conversion (TJ/t)"]]
        .mean()
        .reset_index()
    )

    # Link ISO codes
    cf_cw = pd.read_csv(os.path.join(data_paths["iea_web"], "CONV_country_codes.csv"))
    cf_out = cf_out.merge(cf_cw, left_on="country", right_on="IEA COUNTRY", how="inner")

    regvar = message_regions + "_REGION"
    cf_out[regvar] = ""
    for k in region_list:
        if "child" in dict_message_regions[k].keys():
            cf_out[regvar] = np.where(
                cf_out["ISO"].isin(dict_message_regions[k]["child"]), k, cf_out[regvar]
            )

    print("Collapse conversion factors to REGION level")
    cf_region = (
        cf_out.groupby([regvar, "commodity"])[["conversion (TJ/t)"]]
        .mean()
        .reset_index()
    )

    print("Collapse conversion factors to FUEL level")
    cf_fuel = cf_out.groupby(["commodity"])["conversion (TJ/t)"].mean().reset_index()

    print("Clean up ISO level data")
    cf_iso = cf_out[["ISO", "R12_REGION", "commodity", "conversion (TJ/t)"]].copy()

    print("Save conversion factors")
    cf_region.to_csv(os.path.join(data_paths["iea_web"], "conv_by_region.csv"))
    cf_fuel.to_csv(os.path.join(data_paths["iea_web"], "conv_by_fuel.csv"))
    cf_iso.to_csv(os.path.join(data_paths["iea_web"], "conv_by_iso.csv"))

    print("Full dictionaries")
    full_dict = {message_regions: cf_region, "fuel": cf_fuel, "ISO": cf_iso}

    picklepath = os.path.join(data_paths["iea_web"], "conversion_factors.pickle")
    with open(picklepath, "wb") as file_handler:
        pickle.dump(full_dict, file_handler)


# Import UN Comtrade data and link to conversion factors
# This does not include natural gas pipelines or LNG, which are from IEA
def import_uncomtrade(
    update_year: int = 2024,
    project_name: str | None = None,
    config_name: str | None = None,
):
    """
    Import UN Comtrade data and link to conversion factors, save as CSV and pickle.

    Args:
        update_year: Year of last data update
        project_name: Name of project (e.g., 'newpathways')
        config_name: Name of config file
    """
    dict_dir = package_data_path("bilateralize", "commodity_codes.yaml")
    with open(dict_dir, "r") as f:
        commodity_codes = yaml.safe_load(f)

    full_hs_list: list[str] = []
    for c in commodity_codes.keys():
        full_hs_list = full_hs_list + commodity_codes[c]["HS"]

    data_paths = setup_datapath(project_name=project_name, config_name=config_name)
    print("Build BACI")
    df = pd.DataFrame()
    for y in list(range(2005, update_year, 1)):
        print("Importing BACI" + str(y))
        ydf = pd.read_csv(
            os.path.join(data_paths["baci"], "BACI_HS92_Y" + str(y) + "_V202501.csv"),
            encoding="windows-1252",
        )
        ydf["k"] = ydf["k"].astype(str).str.zfill(6)
        ydf["hs4"] = ydf["k"].str[0:4]
        ydf["hs5"] = ydf["k"].str[0:5]
        ydf["hs6"] = ydf["k"].str[0:6]
        ydf = ydf[
            (ydf["hs4"].isin(full_hs_list))
            | (ydf["hs5"].isin(full_hs_list))
            | (ydf["hs6"].isin(full_hs_list))
        ].copy()
        df = pd.concat([df, ydf])

    print("Save pickle")
    picklepath = os.path.join(data_paths["baci"], "full_2005-2023.pickle")
    with open(picklepath, "wb") as file_handler:
        pickle.dump(df, file_handler)

    df["MESSAGE Commodity"] = ""
    for c in commodity_codes.keys():
        df["MESSAGE Commodity"] = np.where(
            (df["hs4"].isin(commodity_codes[c]["HS"]))
            | (df["hs5"].isin(commodity_codes[c]["HS"]))
            | (df["hs6"].isin(commodity_codes[c]["HS"])),
            commodity_codes[c]["MESSAGE Commodity"],
            df["MESSAGE Commodity"],
        )

    countrycw = pd.read_csv(
        os.path.join(data_paths["baci"], "country_codes_V202401b.csv")
    )
    df = df.merge(
        countrycw[["country_code", "country_iso3"]],
        left_on="i",
        right_on="country_code",
        how="left",
    )
    df = df.rename(columns={"country_iso3": "i_iso3"})
    df = df.merge(
        countrycw[["country_code", "country_iso3"]],
        left_on="j",
        right_on="country_code",
        how="left",
    )
    df = df.rename(columns={"country_iso3": "j_iso3"})
    df = df[["t", "i", "j", "i_iso3", "j_iso3", "k", "MESSAGE Commodity", "v", "q"]]

    df.to_csv(os.path.join(data_paths["baci"], "shortenedBACI.csv"))


# Convert trade values
def convert_trade(
    message_regions: str,
    project_name: str | None = None,
    config_name: str | None = None,
):
    """
    Convert trade values to energy units.

    Args:
        message_regions: Regional resolution
        project_name: Name of project (e.g., 'newpathways')
        config_name: Name of config file
    """
    data_paths = setup_datapath(project_name=project_name, config_name=config_name)

    df = pd.read_csv(os.path.join(data_paths["baci"], "shortenedBACI.csv"))

    with open(
        os.path.join(data_paths["iea_web"], "conversion_factors.pickle"), "rb"
    ) as file_handler:
        conversion_factors = pickle.load(file_handler)
    with open(
        os.path.join(data_paths["iea_web"], "CONV_addl.yaml"), "r"
    ) as file_handler:
        conversion_addl = yaml.safe_load(file_handler)
    cf_codes = pd.read_csv(os.path.join(data_paths["iea_web"], "CONV_hs.csv"))

    df["k"] = df["k"].astype(str)
    cf_codes["HS"] = cf_codes["HS"].astype(str)

    df["HS"] = ""
    for hs in [i for i in cf_codes["HS"] if len(i) == 4]:  # 4 digit HS
        df["HS"] = np.where(df["k"].str[0:4] == hs, hs, df["HS"])
    df["HS"] = np.where(df["HS"] == "", df["k"], df["HS"])

    # Add MESSAGE regions
    dict_dir = package_data_path("node", message_regions + ".yaml")
    with open(dict_dir, "r") as f:
        dict_message_regions = yaml.safe_load(f)
    region_list = [i for i in list(dict_message_regions.keys()) if i != "World"]

    df["MESSAGE Region"] = ""
    for r in region_list:
        df["MESSAGE Region"] = np.where(
            df["i_iso3"].isin(dict_message_regions[r]["child"]), r, df["MESSAGE Region"]
        )
    # Add IEA conversion factors
    df = df.merge(cf_codes, left_on="HS", right_on="HS", how="left")

    df = df.merge(
        conversion_factors["ISO"][["ISO", "commodity", "conversion (TJ/t)"]],
        left_on=["i_iso3", "IEA CONV Commodity"],
        right_on=["ISO", "commodity"],
        how="left",
    )
    df = df.rename(columns={"conversion (TJ/t)": "ISO conversion (TJ/t)"})

    df = df.merge(
        conversion_factors[message_regions],
        left_on=["MESSAGE Region", "IEA CONV Commodity"],
        right_on=[message_regions + "_REGION", "commodity"],
        how="left",
    )
    df = df.rename(columns={"conversion (TJ/t)": "Region conversion (TJ/t)"})

    df = df.merge(
        conversion_factors["fuel"],
        left_on=["IEA CONV Commodity"],
        right_on=["commodity"],
        how="left",
    )
    df = df.rename(columns={"conversion (TJ/t)": "Fuel conversion (TJ/t)"})

    df["conversion (TJ/t)"] = df["ISO conversion (TJ/t)"]
    df["conversion (TJ/t)"] = np.where(
        df["conversion (TJ/t)"].isnull(),
        df["Region conversion (TJ/t)"],
        df["conversion (TJ/t)"],
    )
    df["conversion (TJ/t)"] = df["ISO conversion (TJ/t)"]
    df["conversion (TJ/t)"] = np.where(
        df["conversion (TJ/t)"].isnull(),
        df["Fuel conversion (TJ/t)"],
        df["conversion (TJ/t)"],
    )

    df = df[
        [
            "t",
            "i",
            "j",
            "i_iso3",
            "j_iso3",
            "k",
            "MESSAGE Commodity",
            "v",
            "q",
            "conversion (TJ/t)",
        ]
    ]

    # Add additional conversion factors if missing
    for f in conversion_addl.keys():
        df["conversion (TJ/t)"] = np.where(
            (df["conversion (TJ/t)"].isnull()) & (df["MESSAGE Commodity"] == f),
            conversion_addl[f],
            df["conversion (TJ/t)"],
        )

    # Convert to energy units
    df["conversion (TJ/t)"] = df["conversion (TJ/t)"].astype(float)

    df = df.rename(
        columns={
            "t": "YEAR",
            "i_iso3": "EXPORTER",
            "j_iso3": "IMPORTER",
            "k": "HS",
            "v": "VALUE (1000USD)",
            "q": "WEIGHT (t)",
            "MESSAGE Commodity": "MESSAGE COMMODITY",
        }
    )
    df["WEIGHT (t)"] = df["WEIGHT (t)"].astype(str)
    df = df[~df["WEIGHT (t)"].str.contains("NA")]
    df["WEIGHT (t)"] = df["WEIGHT (t)"].astype(float)
    df["ENERGY (TJ)"] = df["WEIGHT (t)"] * df["conversion (TJ/t)"]

    df = df[
        [
            "YEAR",
            "EXPORTER",
            "IMPORTER",
            "HS",
            "MESSAGE COMMODITY",
            "ENERGY (TJ)",
            "VALUE (1000USD)",
        ]
    ]

    return df


# Import IEA for LNG and pipeline gas
def import_iea_gas(project_name: str | None = None, config_name: str | None = None):
    """
    Import IEA data for LNG and pipeline gas.

    Args:
        project_name: Name of project (e.g., 'newpathways')
        config_name: Name of config file
    """
    data_paths = setup_datapath(project_name=project_name, config_name=config_name)

    ngd = pd.read_csv(os.path.join(data_paths["iea_gas"], "WIMPDAT.txt"), sep=r"\s+")
    ngd.columns = ["YEAR", "PRODUCT", "IMPORTER", "EXPORTER", "VALUE"]

    ngd = ngd[ngd["YEAR"] > 1989]  # Keep after 1990 only
    ngd = ngd[ngd["PRODUCT"].isin(["LNGTJ", "PIPETJ"])]  # Keep only TJ values

    ngd["ENERGY (TJ)"] = np.where(
        ngd["VALUE"].isin(["..", "x", "c"]), np.nan, ngd["VALUE"]
    )
    ngd["ENERGY (TJ)"] = ngd["ENERGY (TJ)"].astype(float)

    ngd["MESSAGE COMMODITY"] = ""
    ngd["MESSAGE COMMODITY"] = np.where(
        ngd["PRODUCT"] == "LNGTJ", "LNG_shipped", ngd["MESSAGE COMMODITY"]
    )
    ngd["MESSAGE COMMODITY"] = np.where(
        ngd["PRODUCT"] == "PIPETJ", "gas_piped", ngd["MESSAGE COMMODITY"]
    )

    cf_cw = pd.read_csv(os.path.join(data_paths["iea_web"], "CONV_country_codes.csv"))
    for t in ["EXPORTER", "IMPORTER"]:
        ngd = ngd.merge(cf_cw, left_on=t, right_on="IEA COUNTRY", how="left")
        ngd[t] = ngd["ISO"]
        ngd = ngd.drop(["ISO", "IEA COUNTRY"], axis=1)
        ngd = ngd[~ngd[t].isnull()]

    ngd = ngd[["YEAR", "EXPORTER", "IMPORTER", "MESSAGE COMMODITY", "ENERGY (TJ)"]]

    ngd = (
        ngd.groupby(["YEAR", "EXPORTER", "IMPORTER", "MESSAGE COMMODITY"])[
            "ENERGY (TJ)"
        ]
        .sum()
        .reset_index()
    )

    return ngd


# Check against IEA balances
def import_iea_balances(
    project_name: str | None = None, config_name: str | None = None
):
    """
    Import IEA balances and save as CSV.

    Args:
        project_name: Name of project (e.g., 'newpathways')
        config_name: Name of config file
    """
    data_paths = setup_datapath(project_name=project_name, config_name=config_name)

    ieadf1 = pd.read_csv(
        os.path.join(data_paths["iea_web"], "EARLYBIG1.txt"), sep=r"\s+", header=None
    )
    ieadf2 = pd.read_csv(
        os.path.join(data_paths["iea_web"], "EARLYBIG2.txt"), sep=r"\s+", header=None
    )

    ieadf = pd.concat([ieadf1, ieadf2])
    ieadf.columns = [
        "region",
        "fuel",
        "year",
        "flow",
        "unit",
        "value",
        "statisticalerror",
    ]

    iea_out = pd.DataFrame()
    for t in ["EXPORTS", "IMPORTS"]:
        tdf = ieadf[ieadf["flow"] == t].copy()
        tdf = tdf[tdf["unit"] == "TJ"]

        tdf = tdf[["region", "fuel", "year", "flow", "unit", "value"]]
        tdf = tdf.rename(
            columns={
                "region": "REGION",
                "fuel": "IEA-WEB COMMODITY",
                "year": "YEAR",
                "flow": "FLOW",
                "unit": "IEA-WEB UNIT",
                "value": "IEA-WEB VALUE",
            }
        )
        iea_out = pd.concat([iea_out, tdf])

    iea_out.to_csv(os.path.join(data_paths["iea_web"], "WEB_TRADEFLOWS.csv"))


def check_iea_balances(
    indf, project_name: str | None = None, config_name: str | None = None
):
    """
    Check against IEA balances.

    Args:
        indf: Input dataframe
        project_name: Name of project (e.g., 'newpathways')
        config_name: Name of config file
    """
    data_paths = setup_datapath(project_name=project_name, config_name=config_name)

    iea = pd.read_csv(os.path.join(data_paths["iea_web"], "WEB_TRADEFLOWS.csv"))
    ieacw = pd.read_csv(os.path.join(data_paths["iea_web"], "country_crosswalk.csv"))
    iea = iea.merge(ieacw, left_on="REGION", right_on="REGION", how="left")
    iea["IEA-WEB VALUE"] = np.where(
        iea["FLOW"] == "EXPORTS", iea["IEA-WEB VALUE"] * -1, iea["IEA-WEB VALUE"]
    )

    # LNG and pipe gas are directly from IEA
    indf = indf[~indf["MESSAGE COMMODITY"].isin(["gas_piped", "LNG_shipped"])].copy()

    dict_dir = package_data_path("bilateralize", "commodity_codes.yaml")
    with open(dict_dir, "r") as f:
        commodity_codes = yaml.safe_load(f)

    iea["COMMODITY"] = ""
    indf["COMMODITY"] = ""
    for c in commodity_codes.keys():
        iea["COMMODITY"] = np.where(
            iea["IEA-WEB COMMODITY"].isin(commodity_codes[c]["IEA-WEB"]),
            c,
            iea["COMMODITY"],
        )
        indf["COMMODITY"] = np.where(
            indf["MESSAGE COMMODITY"] == commodity_codes[c]["MESSAGE Commodity"],
            c,
            indf["COMMODITY"],
        )

    exports = (
        indf.groupby(["YEAR", "EXPORTER", "COMMODITY"])["ENERGY (TJ)"]
        .sum()
        .reset_index()
    )
    imports = (
        indf.groupby(["YEAR", "IMPORTER", "COMMODITY"])["ENERGY (TJ)"]
        .sum()
        .reset_index()
    )

    exports = exports.merge(
        iea[iea["FLOW"] == "EXPORTS"][
            ["ISO", "COMMODITY", "YEAR", "IEA-WEB UNIT", "IEA-WEB VALUE"]
        ],
        left_on=["YEAR", "EXPORTER", "COMMODITY"],
        right_on=["YEAR", "ISO", "COMMODITY"],
        how="left",
    )
    imports = imports.merge(
        iea[iea["FLOW"] == "IMPORTS"][
            ["ISO", "COMMODITY", "YEAR", "IEA-WEB UNIT", "IEA-WEB VALUE"]
        ],
        left_on=["YEAR", "IMPORTER", "COMMODITY"],
        right_on=["YEAR", "ISO", "COMMODITY"],
        how="left",
    )

    exports["DIFFERENCE"] = (
        exports["ENERGY (TJ)"] - exports["IEA-WEB VALUE"]
    ) / exports["IEA-WEB VALUE"]
    imports["DIFFERENCE"] = (
        imports["ENERGY (TJ)"] - imports["IEA-WEB VALUE"]
    ) / imports["IEA-WEB VALUE"]

    exports.to_csv(os.path.join(data_paths["iea_diag"], "iea_calibration_exports.csv"))
    imports.to_csv(os.path.join(data_paths["iea_diag"], "iea_calibration_imports.csv"))


# Aggregate UN Comtrade data to MESSAGE regions
def reformat_to_parameter(
    indf,
    message_regions,
    parameter_name,
    project_name=None,
    config_name=None,
    exports_only=False,
):
    """
    Aggregate UN Comtrade data to MESSAGE regions and
    set up historical activity parameter dataframe.

    Args:
        indf: Input dataframe
        message_regions: Regional resolution
        parameter_name: Name of parameter
        project_name: Name of project (e.g., 'newpathways')
        config_name: Name of config file
        exports_only: If True, only include exports
    """
    dict_dir = package_data_path("node", message_regions + ".yaml")
    with open(dict_dir, "r") as f:
        dict_message_regions = yaml.safe_load(f)
    region_list = [i for i in list(dict_message_regions.keys()) if i != "World"]

    indf["EXPORTER REGION"] = ""
    indf["IMPORTER REGION"] = ""
    for t in ["EXPORTER", "IMPORTER"]:
        for r in region_list:
            indf[t + " REGION"] = np.where(
                indf[t].isin(dict_message_regions[r]["child"]), r, indf[t + " REGION"]
            )

    # Collapse to regional level
    if parameter_name in ["historical_activity"]:
        indf = (
            indf.groupby(
                ["YEAR", "EXPORTER REGION", "IMPORTER REGION", "MESSAGE COMMODITY"]
            )[["ENERGY (GWa)"]]
            .sum()
            .reset_index()
        )
        metric_name = "ENERGY (GWa)"
    elif parameter_name in ["var_cost", "inv_cost", "fix_cost"]:
        indf = (
            indf.groupby(["EXPORTER REGION", "IMPORTER REGION", "MESSAGE COMMODITY"])[
                ["ENERGY (GWa)", "VALUE (MUSD)"]
            ]
            .sum()
            .reset_index()
        )
        indf["PRICE (MUSD/GWa)"] = indf["VALUE (MUSD)"] / indf["ENERGY (GWa)"]
        indf["YEAR"] = "broadcast"
        metric_name = "PRICE (MUSD/GWa)"

    indf = indf[(indf["EXPORTER REGION"] != "") & (indf["IMPORTER REGION"] != "")]
    indf = indf[indf["EXPORTER REGION"] != indf["IMPORTER REGION"]]

    # Add MESSAGE columns for exports
    exdf = message_ix.make_df(
        parameter_name,
        node_loc=indf["EXPORTER REGION"],
        technology=indf["MESSAGE COMMODITY"]
        + "_exp_"
        + indf["IMPORTER REGION"].str.replace(message_regions + "_", "").str.lower(),
        year_act=indf["YEAR"],
        year_vtg=indf["YEAR"],
        value=indf[metric_name],
        mode="M1",
        time="year",
    )
    outdf = exdf.copy()

    # Add MESSAGE columns for imports
    if not exports_only:
        imdf = message_ix.make_df(
            parameter_name,
            node_loc=indf["IMPORTER REGION"],
            technology=indf["MESSAGE COMMODITY"] + "_imp",
            year_act=indf["YEAR"],
            year_vtg=indf["YEAR"],
            value=indf[metric_name],
            mode="M1",
            time="year",
        )

        outdf = pd.concat([outdf, imdf])

    return outdf


# Run all for historical activity
def build_historical_activity(
    message_regions="R12",
    project_name: str | None = None,
    config_name: str | None = None,
    reimport_IEA=False,
    reimport_BACI=False,
):
    """
    Build historical activity parameter dataframe.

    Args:
        message_regions: Regional resolution
        project_name: Name of project (e.g., 'newpathways')
        config_name: Name of config file
        reimport_IEA: If True, reimport IEA data
        reimport_BACI: If True, reimport BACI data
    """
    if reimport_IEA:
        generate_cfdict(
            message_regions=message_regions,
            project_name=project_name,
            config_name=config_name,
        )
        import_iea_balances(project_name=project_name, config_name=config_name)
    if reimport_BACI:
        import_uncomtrade(project_name=project_name, config_name=config_name)

    bacidf = convert_trade(
        message_regions=message_regions,
        project_name=project_name,
        config_name=config_name,
    )
    bacidf = bacidf[bacidf["MESSAGE COMMODITY"] != "lng"]  # Get LNG from IEA instead
    bacidf["MESSAGE COMMODITY"] = bacidf["MESSAGE COMMODITY"] + "_shipped"

    ngdf = import_iea_gas(project_name=project_name, config_name=config_name)

    tradedf = bacidf.merge(
        ngdf,
        left_on=["YEAR", "EXPORTER", "IMPORTER", "MESSAGE COMMODITY"],
        right_on=["YEAR", "EXPORTER", "IMPORTER", "MESSAGE COMMODITY"],
        how="outer",
    )
    tradedf["ENERGY (TJ)"] = tradedf["ENERGY (TJ)_x"]
    tradedf["ENERGY (TJ)"] = np.where(
        tradedf["MESSAGE COMMODITY"].isin(["LNG_shipped", "gas_piped"]),
        tradedf["ENERGY (TJ)_y"],
        tradedf["ENERGY (TJ)"],
    )
    tradedf["ENERGY (TJ)"] = tradedf["ENERGY (TJ)"].astype(float)
    tradedf = tradedf[
        ["YEAR", "EXPORTER", "IMPORTER", "HS", "MESSAGE COMMODITY", "ENERGY (TJ)"]
    ].reset_index()

    check_iea_balances(indf=tradedf, project_name=project_name, config_name=config_name)

    tradedf["ENERGY (GWa)"] = tradedf["ENERGY (TJ)"] * (3.1712 * 1e-5)  # TJ to GWa

    outdf = reformat_to_parameter(
        indf=tradedf,
        message_regions=message_regions,
        parameter_name="historical_activity",
        project_name=project_name,
        config_name=config_name,
    )
    outdf["unit"] = "GWa"

    return outdf.drop_duplicates()


# Calculate historical new capacity based on activity
def build_hist_new_capacity_trade(
    message_regions="R12",
    project_name: str | None = None,
    config_name: str | None = None,
):
    """
    Build historical new capacity based on activity.

    Args:
        message_regions: Regional resolution
        project_name: Name of project (e.g., 'newpathways')
        config_name: Name of config file
    """
    indf = build_historical_activity(
        message_regions=message_regions,
        project_name=project_name,
        config_name=config_name,
    )

    basedf = pd.DataFrame()
    for y in list(range(2000, 2030, 5)):
        ydf = (
            indf[["node_loc", "technology", "mode", "time", "unit"]]
            .drop_duplicates()
            .copy()
        )
        ydf["year_act"] = y
        basedf = pd.concat([basedf, ydf])

    df = basedf.merge(
        indf,
        left_on=["node_loc", "technology", "mode", "time", "unit", "year_act"],
        right_on=["node_loc", "technology", "mode", "time", "unit", "year_act"],
        how="outer",
    )

    df["year_5"] = round(df["year_act"] / 5, 0) * 5  # Get closest 5
    df["year_act"] = np.where(df["year_act"] == 2023, 2025, df["year_act"])
    df = df[df["year_act"] == df["year_5"]]
    df["value"] = np.where(df["value"].isnull(), 0, df["value"])

    df = df.sort_values(by=["node_loc", "technology", "mode", "time", "year_act"])
    df["value"] = df["value"].diff()

    df = df[df["year_act"] > 2000]  # Start at 2000, this ensures diff starts correctly
    df["value"] = np.where(df["value"] < 0, 0, df["value"])
    df["year_vtg"] = df["year_act"].astype(int)

    df = df[["node_loc", "technology", "year_vtg", "value", "unit"]].drop_duplicates()

    return df


# Run all for price
def build_historical_price(
    message_regions="R12",
    project_name: str | None = None,
    config_name: str | None = None,
):
    """
    Build historical price parameter dataframe.

    Args:
        message_regions: Regional resolution
        project_name: Name of project (e.g., 'newpathways')
        config_name: Name of config file
    """
    if reimport_BACI:
        import_uncomtrade(project_name=project_name, config_name=config_name)

    bacidf = convert_trade(
        message_regions=message_regions,
        project_name=project_name,
        config_name=config_name,
    )
    bacidf["MESSAGE COMMODITY"] = np.where(
        bacidf["MESSAGE COMMODITY"] == "lng", "LNG", bacidf["MESSAGE COMMODITY"]
    )
    bacidf["MESSAGE COMMODITY"] = bacidf["MESSAGE COMMODITY"] + "_shipped"
    bacidf = bacidf[bacidf["YEAR"] > 2020]

    bacidf["ENERGY (GWa)"] = bacidf["ENERGY (TJ)"] * (3.1712 * 1e-5)  # TJ to GWa
    bacidf["VALUE (MUSD)"] = bacidf["VALUE (1000USD)"] * 1e-3
    bacidf["PRICE (MUSD/GWa)"] = bacidf["VALUE (MUSD)"] / bacidf["ENERGY (GWa)"]

    bacidf = bacidf[bacidf["ENERGY (TJ)"] > 0.5]  # Keep linkages >0.5TJ

    bacidf = (
        bacidf.groupby(["EXPORTER", "IMPORTER", "MESSAGE COMMODITY"])[
            ["ENERGY (GWa)", "VALUE (MUSD)"]
        ]
        .sum()
        .reset_index()
    )
    bacidf["YEAR"] = "broadcast"

    outdf = reformat_to_parameter(
        indf=bacidf,
        message_regions=message_regions,
        parameter_name="var_cost",
        project_name=project_name,
        config_name=config_name,
        exports_only=True,
    )
    outdf["unit"] = "USD/GWa"

    outdf["value"] = outdf["value"] * 0.50  # TODO: Fix this deflator (2024-2005?)
    outdf["value"] = round(outdf["value"], 0)

    return outdf


# Build for historical new capacity of a given maritime shipment (e.g., LNG tanker)
def build_hist_new_capacity_flow(
    infile: str,
    ship_type: str,
    message_regions: str = "R12",
    project_name: str | None = None,
    config_name: str | None = None,
    annual_mileage=100000,
):
    """
    Build historical new capacity of a given maritime shipment (e.g., LNG tanker).

    Args:
        infile: Name of GISIS input file
        ship_type: Ship type (e.g., 'LNG_tanker_loil')
        message_regions: Regional resolution
        project_name: Name of project (e.g., 'newpathways')
        config_name: Name of config file
        annual_mileage: Average annual mileage of ship in km
    """
    # Regions
    dict_dir = package_data_path("node", message_regions + ".yaml")
    with open(dict_dir, "r") as f:
        dict_message_regions = yaml.safe_load(f)
    region_list = [i for i in list(dict_message_regions.keys()) if i != "World"]

    # IMO data
    data_paths = setup_datapath(project_name=project_name, config_name=config_name)
    imodf = pd.read_csv(os.path.join(data_paths["imo"], "GISIS", infile))

    # Get MESSAGE regions
    imodf[message_regions] = ""
    for r in region_list:
        imodf[message_regions] = np.where(
            imodf["Flag ISO3"].isin(dict_message_regions[r]["child"]),
            r,
            imodf[message_regions],
        )

    # Calculate capacity
    if imodf["Gross Tonnage"].dtype in ["O", "str"]:
        imodf["Gross Tonnage"] = imodf["Gross Tonnage"].str.replace(",", "").astype(int)
    imodf["Capacity (Mt-km)"] = (imodf["Gross Tonnage"] / 1e6) * annual_mileage

    # Collapse
    imodf["Year of Build (5)"] = round(imodf["Year of Build"].astype(float) / 5) * 5
    imodf = (
        imodf.groupby([message_regions, "Year of Build (5)"])[["Capacity (Mt-km)"]]
        .sum()
        .reset_index()
    )
    imodf["Capacity (Mt-km) (Annualized)"] = round(imodf["Capacity (Mt-km)"] / 5, 0)

    # Parameterize
    imodf = imodf.rename(
        columns={
            message_regions: "node_loc",
            "Year of Build (5)": "year_vtg",
            "Capacity (Mt-km) (Annualized)": "value",
        }
    )
    imodf["technology"] = ship_type
    imodf["unit"] = "Mt-km"
    imodf["year_vtg"] = imodf["year_vtg"].astype(int)

    imodf = imodf[["node_loc", "technology", "year_vtg", "value", "unit"]]
    imodf = imodf[imodf["node_loc"] != ""]

    return imodf
