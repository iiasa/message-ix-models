from typing import Literal

import pandas as pd
import pint_pandas  # noqa: F401
from message_ix.util import make_df

from message_ix_models.model.material.util import read_yaml_file
from message_ix_models.util import private_data_path, package_data_path, broadcast


def read_config() -> dict:
    config = read_yaml_file(package_data_path("digsy", "config.yaml"))
    return config


def read_industry_file(config: dict) -> pd.DataFrame:
    path = private_data_path("projects", "digsy", config["industry_input"]["file_name"])
    df = pd.read_excel(path, sheet_name=config["industry_input"]["sheet_name"])
    df.columns = [i.replace("TRModified_agg_", "") for i in df.columns]
    df.columns = [i if not i.isdigit() else int(i) for i in df.columns]
    return df


def get_industry_modifiers(scenario: Literal["BEST", "WORST"]) -> pd.DataFrame:
    config = read_config()
    df = read_industry_file(config)
    df["subsector"] = df["subsector"] + (
        df["Electric or thermal"].fillna("").astype(str)
    )
    df = df.drop(columns=["Electric or thermal"])
    mapping = pd.DataFrame(config["subsector_message_map"]).T
    df = df.set_index("subsector").join(mapping).reset_index()
    df = df[df["scenario"] == scenario]
    df = df[df["par"].notna()]
    df = df.drop(columns=["subsector", "scenario", "Variable"])
    df = df.explode("technology").explode("commodity")
    return df


def apply_industry_modifiers(mods: pd.DataFrame, pars: dict) -> dict:
    for par, group in mods.groupby("par"):
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


def read_ict_demand(scenario="DIGSY-BEST") -> pd.DataFrame:
    path = private_data_path("projects", "digsy", "DIGSY-MESSAGE_ICTs.xls")
    dfs = pd.read_excel(path, sheet_name=None)

    scen_map = {
        "DIGSY-BEST": "Lower Bound",
        "DIGSY-WORST": "Upper Bound",
        "PRISMA": "Mean",
    }
    ssp = "SSP2"

    df2030 = (
        dfs["2030"]
        .drop(columns=["Parent_Region"])
        .set_index(["Region", "Year"])["Allocated_TWh"]
    )
    df_proj = (
        dfs[scen_map[scenario]]
        .drop(columns=["Parent_Region", "Source"])
        .set_index(["Scenario", "Region", "Year"])["Allocated_TWh"]
    )
    df = pd.concat([df2030, df_proj.loc[ssp]])
    df.name = "value"

    df = make_df(
        "demand",
        **df.astype("pint[TWh]")
        .pint.to("GWa")
        .pint.magnitude.to_frame()
        .assign(unit="GWa")
        .reset_index()
        .rename(columns={"Region": "node", "Year": "year"}),
        commodity="electr",
        level="final",
        time="year",
    )
    # keep demand constant post 2050
    post_2050 = (
        df[df["year"] == df["year"].max()]
        .assign(year=None)
        .pipe(broadcast, year=[i for i in [2055, *[i for i in range(2060, 2111, 10)]]])
    )
    df = pd.concat([df, post_2050])
    return df


if __name__ == "__main__":
    read_ict_demand()
