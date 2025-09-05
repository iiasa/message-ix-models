from typing import Literal

import message_ix
import numpy as np
import pandas as pd
import pint_pandas  # noqa: F401
from message_ix.util import make_df

# if TYPE_CHECKING:
from message_ix_models import ScenarioInfo
from message_ix_models.model.material.util import read_yaml_file
from message_ix_models.types import ParameterData
from message_ix_models.util import (
    broadcast,
    make_io,
    merge_data,
    nodes_ex_world,
    package_data_path,
    private_data_path,
    same_node,
)

DIGSY_SCENS = Literal["BEST", "WORST", "baseline", "BESTEST", "WORSTEST"]


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


def read_ict_demand(scenario: DIGSY_SCENS) -> pd.DataFrame:
    path = private_data_path("projects", "digsy", "DIGSY-MESSAGE_ICTs.xls")
    dfs = pd.read_excel(path, sheet_name=None)

    scen_map = {
        "BEST": "Lower Bound",
        "WORST": "Upper Bound",
        "baseline": "Mean",
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


def read_ict_v2(
    digsy_scenario: DIGSY_SCENS,
) -> pd.DataFrame:
    path = private_data_path("projects", "digsy", "R12_Clean Version v2.xlsx")
    scen_map = {
        "baseline": {
            "Data centre": "Scenario_Weighted_Demand (TWh)",
            "Telecom Network": "Scenario_Weighted_Demand - Telecom Network [MEAN ratio] (TWh)",
        },
        "BEST": {
            "Data centre": "Lower Bound (TWh)",
            "Telecom Network": "Lower Bound - Telecom Network [LOW ratio] (TWh)",
        },
        "WORST": {
            "Data centre": "Upper Bound (TWh)",
            "Telecom Network": "Upper Bound - Telecom Network [HIGH ratio] (TWh)",
        },
        "BESTEST": {
            "Data centre": "Lower Bound (TWh)",
            "Telecom Network": "Lower Bound - Telecom Network [LOW ratio] (TWh)",
        },
        "WORSTEST": {
            "Data centre": "Upper Bound (TWh)",
            "Telecom Network": "Upper Bound - Telecom Network [HIGH ratio] (TWh)",
        },
    }
    comm_map = {"Data centre": "data_centre_elec", "Telecom Network": "tele_comm_elec"}
    df = pd.read_excel(path, sheet_name="R12")
    df = df.melt(
        id_vars=["Region", "Year", "Scenario"], var_name="Variable", value_name="Value"
    )
    df = df[df["Scenario"].isin(["IEA (Base)", "SSP2"])].drop(columns=["Scenario"])
    df = df[df["Variable"].isin(scen_map[digsy_scenario].values())]
    df.set_index(["Region", "Year", "Variable"], inplace=True)
    df = make_df(
        "demand",
        **df["Value"]
        .astype("pint[TWh]")
        .pint.to("GWa")
        .pint.magnitude.to_frame()
        .assign(unit="GWa")
        .reset_index()
        .rename(
            columns={
                "Region": "node",
                "Year": "year",
                "Value": "value",
                "Variable": "commodity",
            }
        ),
        level="demand",
        time="year",
    )
    df["commodity"] = df["commodity"].map(
        {scen_map[digsy_scenario][k]: comm_map[k] for k in comm_map.keys()}
    )
    return df


def add_ict_elec_tecs(info: "ScenarioInfo") -> ParameterData:
    common = dict(
        time="year",
        time_origin="year",
        time_dest="year",
        mode="M1",
        year_vtg=info.Y,
        year_act=info.Y,
    )
    tec_comm = (
        ("tele_comm_elec", "tele_comm_elec"),
        ("data_centre_elec", "data_centre_elec"),
    )
    nodes = nodes_ex_world(info.N)
    pars = []
    for tec, comm in tec_comm:
        df1 = make_io(
            ("electr", "final", "GWa"),
            (comm, "demand", "GWa"),
            1,
            technology=tec,
            **common,
        )
        df1 = {
            k: df.pipe(broadcast, node_loc=nodes).pipe(same_node)
            for k, df in df1.items()
        }
        pars.append(df1)
    merge_data(pars[0], pars[1])
    return pars[0]


def extrapolate_post_2050(
    ict: pd.DataFrame, scenario: message_ix.Scenario
) -> pd.DataFrame:
    df = read_rc_elec("scenario", scenario)
    # keep demand share of ICT constant post 2050
    ict_2050 = ict[ict["year"] == ict["year"].max()]
    share_2050 = (
        ict_2050.set_index([i for i in ict_2050.columns if i != "value"])
        .div(df.set_index(["node", "year"])["value"], axis=0)
        .dropna()
        .reset_index()
        .assign(year=None)
    )
    post_2050 = (
        share_2050.pipe(
            broadcast, year=[i for i in [2055, *[i for i in range(2060, 2111, 10)]]]
        )
        .set_index([i for i in ict_2050.columns if i != "value"])
        .mul(df.set_index(["node", "year"])["value"], axis=0)
        .dropna()
        .reset_index()
    )
    ict = pd.concat([ict, post_2050])
    return ict


def adjust_rc_elec(scenario: message_ix.Scenario, ict: pd.DataFrame) -> pd.DataFrame:
    df = read_rc_elec("scenario", scenario)
    ict_tot = ict.groupby(["year", "node"]).sum(numeric_only=True)
    ict_tot_2020 = ict_tot.loc[2020]
    df_adj = (
        df.set_index([i for i in df.columns if i != "value"])
        .sub(ict_tot_2020, fill_value=0)
        .reset_index()
    )
    return df_adj


def read_rc_elec(
    source: Literal["scenario", "file"], scenario: message_ix.Scenario
) -> pd.DataFrame:
    if source == "scenario":
        rc_elec = scenario.par("demand", filters={"commodity": "rc_spec"})
    else:
        rc = pd.read_csv(
            "/Users/florianmaczek/PycharmProjects/message_single_country/models/data/demand/rc_sector/rc_demands_v11.csv"
        )
        rc_elec = (
            rc[(rc["commodity"] == "comm_other_uses_electr") & (rc["ssp"] == "SSP2")]
            .drop(columns=["ssp", "commodity"])
            .melt(id_vars=["node"], var_name="year", value_name="value")
        )
        rc_elec["node"] = "R12_" + rc_elec["node"]
    return rc_elec


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


def fe_to_ue(df: pd.DataFrame, scen: message_ix.Scenario) -> pd.DataFrame:
    inp = scen.par("input", filters={"technology": "sp_el_RC"})
    inp = (
        inp[["node_loc", "year_act", "value"]]
        .rename(columns={"node_loc": "node", "year_act": "year", "value": "efficiency"})
        .set_index(["node", "year"])
    )
    df_new = (
        df.set_index(["node", "year"])
        .join(inp)
        .assign(value=lambda x: x["value"] / x["efficiency"])
        .reset_index()
        .drop(columns=["efficiency"])
    )
    return df_new


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


def adjust_act_calib(ict: pd.DataFrame, scen: message_ix.Scenario):
    for par in ["bound_activity_up", "bound_activity_lo"]:
        bound = scen.par(par, filters={"technology": "sp_el_RC"})
        ict_tot = (
            ict.rename(columns={"node": "node_loc", "year": "year_act"})
            .groupby(
                [
                    "year_act",
                    "node_loc",
                ]
            )
            .sum(numeric_only=True)
            .loc[2020]
        )
        new_bound = (
            bound.set_index([i for i in bound.columns if i != "value"])
            .sub(ict_tot)
            .reset_index()
        ).dropna()
        with scen.transact():
            scen.add_par(par, new_bound)
