import numpy as np
import pandas as pd
import pint_pandas  # noqa: F401

from message_ix_models import ScenarioInfo
from message_ix_models.model.material.util import read_yaml_file
from message_ix_models.project.digsy.utils import DIGSY_SCENS
from message_ix_models.util import (
    package_data_path,
    private_data_path,
)


def read_config() -> dict:
    config = read_yaml_file(package_data_path("digsy", "config.yaml"))
    return config


def read_file(path, **kwargs) -> pd.DataFrame:
    if path.suffix == ".csv":
        df = pd.read_csv(path)
    elif path.suffix in (".xls", ".xlsx"):
        df = pd.read_excel(path, **kwargs)
    else:
        raise ValueError(f"File type of {path} not supported.")
    return df


def read_industry_file(config: dict) -> pd.DataFrame:
    path = private_data_path("projects", "digsy", config["industry_input"]["file_name"])
    df = read_file(path, sheet_name=config["industry_input"]["sheet_name"])
    df.columns = [i.replace("TRModified_agg_", "") for i in df.columns]
    df.columns = [i if not i.isdigit() else int(i) for i in df.columns]
    return df


def get_industry_modifiers(scenario: DIGSY_SCENS) -> pd.DataFrame:
    config = read_config()
    df = read_industry_file(config)
    df["subsector"] = df["subsector"] + (
        df["Electric or thermal"].fillna("").astype(str)
    )
    df = df.drop(columns=["Electric or thermal"])
    mapping = pd.DataFrame(config["subsector_message_map"]).T
    df["subsector"] = df["subsector"].str.strip()
    df = df.set_index("subsector").join(mapping).reset_index()
    df = df[df["scenario"] == scenario]
    df = df[df["par"].notna()]
    df = df.drop(columns=["subsector", "scenario", "Variable"])
    df = df.explode("technology").explode("commodity")
    return df


def extrapolate_modifiers_past_2050(df: pd.DataFrame, s_info: "ScenarioInfo"):
    model_year_past_2050 = [i for i in s_info.Y if i > 2050]
    df[model_year_past_2050] = np.tile(
        df[2050].values.reshape(-1, 1), len(model_year_past_2050)
    )
    return df


def apply_industry_modifiers(mods: pd.DataFrame, pars: dict) -> dict:
    for par, group in mods.groupby("par"):
        if par not in pars.keys():
            continue
        par_data = pars[par]
        par_node_col = [i for i in par_data.columns if "node" in i][0]
        par_yr_col = [i for i in par_data.columns if "year" in i][0]
        group["region"] = "R12_" + group["region"]
        mod_tmp = (
            group.rename(columns={"region": par_node_col})
            .drop(columns=["sector", "par"])
            .set_index(
                [par_node_col, *[i for i in group.columns if i in par_data.columns]]
            )
            .melt(ignore_index=False, var_name=par_yr_col, value_name="value")
            .set_index(par_yr_col, append=True)
        )
        mod_tmp["value"] = mod_tmp["value"].add(1)
        mod_tmp = mod_tmp.groupby(mod_tmp.index.names).prod()
        par_data_modified = (
            par_data.set_index(mod_tmp.index.names)
            .join(mod_tmp, rsuffix="_mod")
            .fillna(1)
        )
        par_data_modified["value"] = (
            par_data_modified["value"] * par_data_modified["value_mod"]
        )
        par_data_modified.drop(columns=["value_mod"], inplace=True)
        par_data_modified.reset_index(inplace=True)
        pars[par] = par_data_modified
    return pars


def read_rc_materials(
    digsy_scenario: DIGSY_SCENS,
):
    f_suffix = f"_{digsy_scenario}" if digsy_scenario != "baseline" else ""
    path = private_data_path(
        "projects", "digsy", "buildings", f"rc_material_demand_SSP2{f_suffix}.csv"
    )
    df = pd.read_csv(path)
    df = df[df["commodity"].str.contains("_mat_dem")]
    comms = {i.split("_")[-1] for i in df["commodity"].unique()}
    dfs = []
    for comm in comms:
        dfs.append(
            df[df["commodity"].str.endswith(comm)]
            .groupby(["node", "year"])
            .sum(numeric_only=True)
            .assign(commodity=comm)
        )
    df_agg = pd.concat(dfs)
    return df_agg


def read_trp_materials(digsy_scenario) -> pd.DataFrame:
    if digsy_scenario == "baseline":
        digsy_scenario = "BASE"
    path = private_data_path(
        "projects", "digsy", "transport", "MixT material handover #1585.csv"
    )
    df = pd.read_csv(path)
    df.value /= 1000
    df = df[df["digsy_scenario"] == digsy_scenario]
    df["commodity"] = df["commodity"].str.split("_").str[-1]
    df = df.set_index(["node", "year"])[["value", "commodity"]]
    return df


def adjust_mat_dem(
    total_demand: pd.DataFrame,
    sector_demand_base: pd.DataFrame,
    sector_demand_scen: pd.DataFrame,
) -> pd.DataFrame:
    sector_demand_commodity = sector_demand_base[
        sector_demand_base["commodity"].isin(total_demand["commodity"].unique())
    ]
    sector_demand_commodity_scen = sector_demand_scen[
        sector_demand_scen["commodity"].isin(total_demand["commodity"].unique())
    ]
    sector_demand_commodity.set_index("commodity", append=True, inplace=True)
    sector_demand_commodity_scen.set_index("commodity", append=True, inplace=True)
    total_demand_excl_sector_base = (
        total_demand.set_index([i for i in total_demand.columns if i != "value"])
        .sub(sector_demand_commodity, fill_value=0)
        .reset_index()
    )
    total_demand_adjusted = (
        total_demand_excl_sector_base.set_index(
            [i for i in total_demand_excl_sector_base.columns if i != "value"]
        )
        .add(sector_demand_commodity_scen, fill_value=0)
        .reset_index()
    )
    return total_demand_adjusted
