from typing import Literal

import message_ix
import pandas as pd
import pint_pandas  # noqa: F401
from message_ix.util import make_df

from message_ix_models import ScenarioInfo
from message_ix_models.model.material.util import get_ssp_from_context
from message_ix_models.project.digsy.utils import (
    DIGSY_SCENS,
    adjust_act_calib,
    fe_to_ue,
)
from message_ix_models.types import ParameterData
from message_ix_models.util import (
    broadcast,
    make_io,
    merge_data,
    nodes_ex_world,
    private_data_path,
    same_node,
)


def read_ict_demand(scenario: DIGSY_SCENS, ssp, version=3) -> pd.DataFrame:
    read = {1: read_ict_v1, 2: read_ict_v2, 3: read_ict_v3, "prisma": read_ict_v3}
    if version == 3:
        scen_map = {
            "BESTEST": {
                "Data centre": "DC lower Bound (TWh)",
                "Telecom Network": "BESTEST ICT (TWh)",
            },
            "BEST": {
                "Data centre": "DC lower Bound (TWh)",
                "Telecom Network": "BEST ICT (TWh)",
            },
            "baseline": {
                "Data centre": "DC Central Estimate (TWh)",
                "Telecom Network": "Central Estimate (TWh)",
            },
            "WORST": {
                "Data centre": "DC Upper Bound (TWh)",
                "Telecom Network": "WORST ICT (TWh)",
            },
            "WORSTEST": {
                "Data centre": "DC Upper Bound (TWh)",
                "Telecom Network": "WORSTEST ICT  (TWh)",
            },
        }
        dc_scen = scen_map[scenario]["Data centre"]
        tele_scen = scen_map[scenario]["Telecom Network"]
        df = read[version](dc_scen, tele_scen, ssp)
    elif version == "prisma":
        scen_map = {
            "Low": {
                "Data centre": "DC lower Bound (TWh)",
                "Telecom Network": "BESTEST ICT (TWh)",
            },
            "Medium": {
                "Data centre": "DC Central Estimate (TWh)",
                "Telecom Network": "Central Estimate (TWh)",
            },
            "High": {
                "Data centre": "DC Upper Bound (TWh)",
                "Telecom Network": "WORSTEST ICT  (TWh)",
            },
        }
        dc_scen = scen_map[scenario]["Data centre"]
        tele_scen = scen_map[scenario]["Telecom Network"]
        df = read[version](dc_scen, tele_scen, ssp)
    else:
        df = read[version](scenario, ssp)
    return df


def read_ict_v1(scenario: DIGSY_SCENS, ssp):
    path = private_data_path("projects", "digsy", "DIGSY-MESSAGE_ICTs.xls")
    dfs = pd.read_excel(path, sheet_name=None)

    scen_map = {
        "BEST": "Lower Bound",
        "WORST": "Upper Bound",
        "baseline": "Mean",
    }

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
    return df


def read_ict_v2(
    digsy_scenario: DIGSY_SCENS,
    ssp="SSP2",
) -> pd.DataFrame:
    path = private_data_path("projects", "digsy", "R12_Clean Version v2.xlsx")
    scen_map = {
        "baseline": {
            "Data centre": "Scenario_Weighted_Demand (TWh)",
            "Telecom Network": "Scenario_Weighted_Demand - Telecom Network [MEAN ratio] (TWh)",
        },
        "BEST": {
            "Data centre": "Lower Bound (TWh)",
            "Telecom Network": "Lower Bound - Telecom Network [MEAN ratio] (TWh)",
        },
        "WORST": {
            "Data centre": "Upper Bound (TWh)",
            "Telecom Network": "Upper Bound - Telecom Network [MEAN ratio] (TWh)",
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
    df = df[df["Scenario"].isin(["IEA (Base)", ssp])].drop(columns=["Scenario"])
    df = df[
        ((df["Variable"].isin(scen_map["baseline"].values())) & (df["Year"] < 2030))
        | (
            (df["Variable"].isin(scen_map[digsy_scenario].values()))
            & (df["Year"] >= 2030)
        )
    ]
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
    comm_map_final = {scen_map[digsy_scenario][k]: comm_map[k] for k in comm_map.keys()}
    comm_map_final.update(
        {scen_map["baseline"][k]: comm_map[k] for k in comm_map.keys()}
    )
    df["commodity"] = df["commodity"].map(comm_map_final)
    return df


def read_ict_v3(dc_scen, tele_scen, ssp="SSP2") -> pd.DataFrame:
    path = private_data_path(
        "projects", "digsy", "R12 Clean MESSAGE version_Finalised.xlsx"
    )
    comm_map = {dc_scen: "data_centre_elec", tele_scen: "tele_comm_elec"}
    df = pd.read_excel(path, sheet_name="Option 1")
    df[tele_scen] = df[tele_scen] - df[dc_scen]
    df = df.melt(
        id_vars=["Region", "Year", "Scenario"], var_name="Variable", value_name="Value"
    )
    df = df[df["Scenario"].isin(["IEA (Base)", ssp])].drop(columns=["Scenario"])
    df = df[
        (
            (
                df["Variable"].isin(
                    ["DC Central Estimate (TWh)", "Central Estimate (TWh)"]
                )
            )
            & (df["Year"] < 2030)
        )
        | ((df["Variable"].isin([dc_scen, tele_scen])) & (df["Year"] >= 2030))
    ]
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
    df["commodity"] = df["commodity"].map(comm_map)
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


def gen_ict_demands(context, scenario, ict_scenario, ict_version) -> pd.DataFrame:
    ict_demand = read_ict_demand(
        ict_scenario, get_ssp_from_context(context), ict_version
    )
    ict_demand = extrapolate_post_2050(ict_demand, scenario)
    ict_demand_ue = fe_to_ue(ict_demand, scenario)
    rc_demand_adjusted = adjust_rc_elec(scenario, ict_demand_ue)
    adjust_act_calib(ict_demand_ue, scenario)
    return pd.concat([ict_demand, rc_demand_adjusted])
