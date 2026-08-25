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
import requests

from message_ix_models.tools.bilateralize.utils import load_config
from message_ix_models.util import package_data_path

ssp = 1

# Call scenario
mp = ixmp.Platform()
scen = message_ix.Scenario(mp, "oil-test", f"SSP{ssp} - High Emissions")

# Collect activity from model
df = scen.var("ACT", filters = {"technology": ["oil_exp", "oil_imp"]})
df['lvl'] = df['lvl'].astype(float)*0.03154 # To EJ
df = df.groupby(['node_loc', 'technology', 'year_act'])['lvl'].sum().reset_index()

exdf = df[df['technology'] == 'oil_exp']
impdf = df[df['technology'] == 'oil_imp']

exdf = exdf.rename(columns = {'lvl': 'exports'})[['node_loc', 'year_act', 'exports']]
impdf = impdf.rename(columns = {'lvl': 'imports'})[['node_loc', 'year_act', 'imports']]
df = pd.merge(exdf, impdf, on = ['node_loc', 'year_act'], how = 'outer')
df['exports'] = df['exports'].fillna(0)
df['imports'] = df['imports'].fillna(0)
df['net_exports'] = df['exports'] - df['imports']

df.to_csv(f"oil_trade_{ssp}.csv", index=False)

# Collect trade data from IEA
GITHUB_REPO      = "iiasa/message-ix-models"
GITHUB_REF       = os.environ.get("MESSAGE_IX_MODELS_REF", "main")
GITHUB_NODE_PATH = "message_ix_models/data/node"

_github_list_cache = None
_github_yaml_cache = {}

def _github_schema_yaml(name):
    """Raw yaml text for one schema, or None if it doesn't exist at this ref."""
    if name not in _github_yaml_cache:
        url = f"https://raw.githubusercontent.com/{GITHUB_REPO}/{GITHUB_REF}/{GITHUB_NODE_PATH}/{name}.yaml"
        resp = requests.get(url, timeout=10)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        _github_yaml_cache[name] = resp.text
    return _github_yaml_cache[name]

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
    project_name: str | None = None, config_name: str | None = None
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
    ieacw = pd.read_csv(os.path.join(data_paths["iea_web"], "country_iso3.csv"))
    iea = iea.merge(ieacw, left_on="REGION", right_on="REGION", how="left")
    iea["IEA-WEB VALUE"] = np.where(
        iea["FLOW"] == "EXPORTS", iea["IEA-WEB VALUE"] * -1, iea["IEA-WEB VALUE"]
    )
    iea = iea.groupby(["YEAR", "ISO3", "IEA-WEB COMMODITY", "IEA-WEB UNIT", "FLOW"])["IEA-WEB VALUE"].sum().reset_index()

    # Reclassify to MESSAGE commodities
    dict_dir = package_data_path("bilateralize", "commodity_codes.yaml")
    with open(dict_dir, "r", encoding="utf8") as f:
        commodity_codes = yaml.safe_load(f)

    iea["COMMODITY"] = ""
    for c in commodity_codes.keys():
        iea["COMMODITY"] = np.where(
            iea["IEA-WEB COMMODITY"].isin(commodity_codes[c]["IEA-WEB"]),
            c,
            iea["COMMODITY"],
        )
    
    # Reclassify to MESSAGE nodes
    region_schema = yaml.safe_load(_github_schema_yaml("R12"))
    iea["node"] = None
    for k in region_schema.keys():
        if "child" in region_schema[k].keys():
            iea["node"] = np.where(iea['ISO3'].isin(region_schema[k]['child']), k, iea["node"])

    # Split into exports and imports
    exports = iea[iea["FLOW"] == "EXPORTS"]
    imports = iea[iea["FLOW"] == "IMPORTS"]

    return exports, imports

iea_exports, iea_imports = check_iea_balances(project_name="diagnose-oil", config_name="config.yaml")
iea_exports.to_csv(f"iea_exports.csv", index=False)
iea_imports.to_csv(f"iea_imports.csv", index=False)