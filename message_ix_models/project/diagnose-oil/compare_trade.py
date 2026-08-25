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
msg_df = pd.DataFrame()
for ssp in [1, 2, 3, 4, 5]:
    scen = message_ix.Scenario(mp, f"SSP_SSP{ssp}_v6.5_rep_upd2", f"SSP{ssp} - High Emissions")

    # Collect historical activity from model
    df = scen.par('historical_activity', filters = {'technology': ['oil_exp', 'oil_imp']})
    df['value'] = df['value'].astype(float)*0.03154 # to EJ
    df = df.groupby(['node_loc', 'technology', 'year_act'])['value'].sum().reset_index()
    df = df.rename(columns = {'value': 'lvl'})
    df = df[['node_loc', 'technology', 'year_act', 'lvl']]
    df_hist = df.copy()
    
    # Collect model activity from model
    df = scen.var("ACT", filters = {"technology": ["oil_exp", "oil_imp"]})
    df['lvl'] = df['lvl'].astype(float)*0.03154 # To EJ
    df = df.groupby(['node_loc', 'technology', 'year_act'])['lvl'].sum().reset_index()
    df = pd.concat([df_hist, df])

    exdf = df[df['technology'] == 'oil_exp']
    impdf = df[df['technology'] == 'oil_imp']

    exdf = exdf.rename(columns = {'lvl': 'exports'})[['node_loc', 'year_act', 'exports']]
    impdf = impdf.rename(columns = {'lvl': 'imports'})[['node_loc', 'year_act', 'imports']]
    message_df = pd.merge(exdf, impdf, on = ['node_loc', 'year_act'], how = 'outer')
    message_df['exports'] = message_df['exports'].fillna(0)
    message_df['imports'] = message_df['imports'].fillna(0)
    message_df['net_exports_MIX'] = message_df['exports'] - message_df['imports']
    message_df = message_df.rename(columns = {'exports': 'exports_MIX', 'imports': 'imports_MIX'})
    message_df['SSP'] = ssp
    msg_df = pd.concat([msg_df, message_df])
msg_df.to_csv(f"oil_trade_MIX.csv", index=False)
mp.close_db()

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

    iea = iea.groupby(['YEAR', 'FLOW', 'COMMODITY', 'node'])['IEA-WEB VALUE'].sum().reset_index()
    iea['IEA-WEB VALUE'] = iea['IEA-WEB VALUE'] * 10^-6 # TJ to EJ
    
    # Split into exports and imports
    exports = iea[iea["FLOW"] == "EXPORTS"]
    exports = exports.rename(columns = {'IEA-WEB VALUE': 'exports'})
    exports = exports.drop(columns = ['FLOW'])

    imports = iea[iea["FLOW"] == "IMPORTS"]
    imports = imports.rename(columns = {'IEA-WEB VALUE': 'imports'})
    imports = imports.drop(columns = ['FLOW'])

    iea_df = pd.merge(exports, imports, on = ['YEAR', 'COMMODITY', 'node'], how = 'outer')
    iea_df['net_exports_IEA'] = iea_df['exports'] - iea_df['imports']
    iea_df = iea_df.rename(columns = {'exports': 'exports_IEA', 'imports': 'imports_IEA'})
    
    return iea_df

iea_df = check_iea_balances(project_name="diagnose-oil", config_name="config.yaml")

# Combine MIX and IEA data
iea_df = iea_df[iea_df['COMMODITY'] == 'Crude Oil']
iea_df = iea_df[['node', 'YEAR', 'exports_IEA', 'imports_IEA', 'net_exports_IEA']].drop_duplicates()

msg_df = msg_df[['SSP','node_loc', 'year_act', 'exports_MIX', 'imports_MIX', 'net_exports_MIX']]
msg_df = msg_df.rename(columns = {'node_loc': 'node', 'year_act': 'YEAR'})

outdf = pd.merge(msg_df, iea_df, on = ['YEAR', 'node'], how = 'outer')
outdf.to_csv(f"oil_trade_comparison.csv", index=False)