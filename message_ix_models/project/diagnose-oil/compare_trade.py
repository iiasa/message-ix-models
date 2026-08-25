'''
Diagnose issues with crude oil trade (global pool)
'''

import ixmp
import message_ix
import pandas as pd
import plotnine
import numpy as np
import os
import yaml

from message_ix_models.tools.bilateralize.utils import load_config
from message_ix_models.util import package_data_path

ssp = 1

# Call scenario
mp = ixmp.Platform()
scen = message_ix.Scenario("oil-test", f"SSP{ssp} - High Emissions")

# Collect activity from model
df = scen.var("ACT", filters = {"technology": ["oil_exp", "oil_imp"]})
df['value'] = df['value'].astype(float)*0.03154 # To EJ
exdf = df[df['technology'] == 'oil_exp']
impdf = df[df['technology'] == 'oil_imp']

exdf = exdf.rename(columns = {'value': 'exports'})[['node_loc', 'year_act', 'exports']]
impdf = impdf.rename(columns = {'value': 'imports'})[['node_loc', 'year_act', 'imports']]
df = pd.merge(exdf, impdf, on = ['node_loc', 'year_act'], how = 'outer')
df['exports'] = df['exports'].fillna(0)
df['imports'] = df['imports'].fillna(0)
df['net_exports'] = df['exports'] - df['imports']

df.to_csv(f"oil_trade_{ssp}.csv", index=False)

# Collect trade data from IEA
def setup_datapath(project_name: str | None = "diagnose-oil", 
                   config_name: str | None = "config.yaml"):
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
    cw_path = os.path.join(data_path, "crosswalks")

    data_paths = dict(
        iea_web=iea_web_path,
        iea_gas=iea_gas_path,
        iea_diag=iea_diagnostics_path,
        baci=baci_path,
        imo=imo_path,
        cw=cw_path,
    )

    return data_paths

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
    with open(dict_dir, "r", encoding="utf8") as f:
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

    return exports, imports

iea_exports, iea_imports = check_iea_balances(df, project_name="diagnose-oil", config_name="config.yaml")
iea_exports.to_csv(f"iea_exports.csv", index=False)
iea_imports.to_csv(f"iea_imports.csv", index=False)