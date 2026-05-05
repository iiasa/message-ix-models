from typing import TYPE_CHECKING, Literal

import ixmp
import message_ix
import pandas as pd
import pint_pandas  # noqa: F401
import pyam
from message_ix.util import make_df

from message_ix_models import ScenarioInfo
from message_ix_models.model.material.util import get_ssp_from_context
from message_ix_models.project.digsy.ict_new import generate_demand
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

if TYPE_CHECKING:
    from message_ix import Scenario


def read_ict_demand(scenario: DIGSY_SCENS, ssp, version=3, s_info=None) -> pd.DataFrame:
    read = {
        1: read_ict_v1,
        2: read_ict_v2,
        3: read_ict_v3,
        "prisma": read_ict_v3,
        "prisma2": generate_demand,
        "prisma_convergence": generate_demand,
    }
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
    elif version == "prisma2":
        df = read[version](scenario, ssp, s_info)
    elif version == "prisma_convergence":
        df = read[version](scenario, ssp, s_info)
        dc = df[df["commodity"] == "data_centre_elec"]
        tc = df[df["commodity"] == "tele_comm_elec"]
        dfs = []
        for ict_dem in [dc, tc]:
            dfs.append(run_convergence(ict_dem, s_info))
        df = pd.concat(dfs)
    else:
        df = read[version](scenario, ssp)
    return df


def read_ict_v1(scenario: DIGSY_SCENS, ssp):
    path = private_data_path("projects", "digsy", "ict", "DIGSY-MESSAGE_ICTs.xls")
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
    path = private_data_path("projects", "digsy", "ict", "R12_Clean Version v2.xlsx")
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
        "projects", "digsy", "ict", "R12 Clean MESSAGE version_Finalised.xlsx"
    )
    comm_map = {
        dc_scen: "data_centre_elec",
        tele_scen: "tele_comm_elec",
        "DC Central Estimate (TWh)": "data_centre_elec",
        "Central Estimate (TWh)": "tele_comm_elec",
    }
    df = pd.read_excel(path, sheet_name="Option 1", index_col=[2, 1, 0])
    df_iea = df.loc["IEA (Base)"].loc[[2020, 2025]][
        ["DC Central Estimate (TWh)", "Central Estimate (TWh)"]
    ]
    df_iea["Central Estimate (TWh)"] = (
        df_iea["Central Estimate (TWh)"] - df_iea["DC Central Estimate (TWh)"]
    )
    df_2030 = df.loc["IEA (Base)"].loc[[2030]][[dc_scen, tele_scen]]
    df_2030[tele_scen] = df_2030[tele_scen] - df_2030[dc_scen]

    df_ssp = df.loc[ssp][[dc_scen, tele_scen]]
    df_ssp[tele_scen] = df_ssp[tele_scen] - df_ssp[dc_scen]
    df = (
        pd.concat([df_iea, df_2030, df_ssp])
        .melt(ignore_index=False, var_name="Variable", value_name="Value")
        .dropna()
    )
    df.set_index("Variable", append=True, inplace=True)
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


def extrapolate_post_2050(ict: pd.DataFrame, scenario: "Scenario") -> pd.DataFrame:
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


def adjust_rc_elec(scenario: "Scenario", ict: pd.DataFrame) -> pd.DataFrame:
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
    source: Literal["scenario", "file"], scenario: "Scenario"
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


def gen_ict_demands(
    context, scenario, ict_scenario, ict_version, s_info
) -> pd.DataFrame:
    ict_demand = read_ict_demand(
        ict_scenario, get_ssp_from_context(context), ict_version, s_info
    )
    if ict_version not in ["prisma2", "prisma_convergence"]:
        ict_demand = extrapolate_post_2050(ict_demand, scenario)
    ict_demand_ue = fe_to_ue(ict_demand, scenario)
    rc_demand_adjusted = adjust_rc_elec(scenario, ict_demand_ue)
    adjust_act_calib(ict_demand_ue, scenario)
    return pd.concat([ict_demand, rc_demand_adjusted])


def read_ict_r5(scenario, ssp):
    scen_map = {
        "Low": {
            "Data centre": "Lower Bound DC (TWh)",
            "Telecom": 0.78,
        },
        "Medium": {
            "Data centre": "Mean DC (TWh)",
            "Telecom": 0.78,
        },
        "High": {
            "Data centre": "Upper Bound DC (TWh)",
            "Telecom": 0.91,
        },
    }
    path = private_data_path(
        "projects", "digsy", "ict", "R12_Clean IAM version_Finalised.xlsx"
    )
    df = pd.read_excel(path, sheet_name="R5 DC", index_col=[2, 1, 0])
    df_iea = df.loc["IEA (Base)"].loc[[2020, 2025]]["Central Estimate DC (TWh)"]
    df_2030 = df.loc["IEA (Base)"].loc[[2030]][scen_map[scenario]["Data centre"]]
    df_ssp = df.loc[ssp][scen_map[scenario]["Data centre"]]
    df = (
        pd.DataFrame(pd.concat([df_iea, df_2030, df_ssp]))
        .rename(columns={0: "Value"})
        .reset_index()
    ).assign(Unit="GWa", scenario=f"{ssp} - {scenario} ICT", model="Input data")
    df_dc = df.assign(Variable="Final Energy|Commercial|ICT|Data Centers")
    df_tc = df_dc.assign(
        Value=lambda x: x["Value"] * scen_map[scenario]["Telecom"],
        Variable="Final Energy|Commercial|ICT|Infrastructure",
    )
    py_df = pyam.IamDataFrame(pd.concat([df_dc, df_tc])).convert_unit("GWa", "EJ")
    return py_df


def get_population_data(s_info: "ScenarioInfo") -> pd.Series:
    # TODO: replace with .project.ssp.data.SSPUpdate
    mp = ixmp.Platform()
    scen = message_ix.Scenario(mp, s_info.model, s_info.scenario)
    pop = (
        scen.par("bound_activity_up", filters={"technology": "Population"})
        .rename(columns={"node_loc": "node", "year_act": "year"})
        .set_index(["node", "year"])["value"]
        .div(1000)
    )
    return pop


def calc_demand_per_cap(
    demand: pd.DataFrame, pop: pd.Series
) -> tuple[pd.Series, pd.Series]:
    pop_wld = pop.groupby("year").sum(numeric_only=True)
    demand = demand.groupby(["node", "year"]).sum()["value"]
    demand_wld = demand.groupby("year").sum(numeric_only=True)
    demand_pcap = demand.div(pop).dropna()
    demand_pcap_wld = demand_wld.div(pop_wld).dropna()
    return demand_pcap, demand_pcap_wld


def gen_conv_parameters(offset_target, exponent_target) -> tuple[pd.Series, pd.Series]:
    y_target = 2050
    y_start = 2030
    offsets = {
        2020: 0,
        2025: 0,
        y_start: 0,
        2035: None,
        2040: None,
        2045: None,
        y_target: offset_target,
    }
    exponents = {
        2020: 1,
        2025: 1,
        y_start: 1,
        2035: None,
        2040: None,
        2045: None,
        y_target: exponent_target,
    }
    y_prev = None
    scaler = {2035: 1 / 8}

    def interpolation_func(x_prev, y, target, start):
        return x_prev + scaler.get(y, 1 / 4) * (target - start)

    for y, v in exponents.items():
        if v is None:
            exponents[y] = interpolation_func(
                exponents[y_prev], y, exponents[y_target], exponents[y_start]
            )
        y_prev = y
    y_prev = None
    for y, v in offsets.items():
        if v is None:
            offsets[y] = interpolation_func(
                offsets[y_prev], y, offsets[y_target], offsets[y_start]
            )
        y_prev = y

    df_exp = pd.Series(exponents).rename("value").rename_axis("year")
    df_off = pd.Series(offsets).rename("value").rename_axis("year")
    return df_off, df_exp


def run_convergence(ict_demand: pd.DataFrame, s_info: "ScenarioInfo") -> pd.DataFrame:
    pop = get_population_data(s_info)
    demands_pcap, demand_pcap_wld = calc_demand_per_cap(ict_demand, pop)
    demand_pcap_ratio = demands_pcap.div(demand_pcap_wld)
    offset_target = 0.31
    expontent_target = 1.5
    offsets, exponents = gen_conv_parameters(offset_target, expontent_target)
    demand_pcap_ratio_conv = demand_pcap_ratio.add(
        offsets, fill_value=offset_target
    ).pow(1 / exponents, fill_value=1 / expontent_target)
    demand_conv = demand_pcap_ratio_conv.mul(demand_pcap_wld).mul(pop)
    demand_par = ict_demand.drop("value", axis=1).merge(
        demand_conv.reset_index(), on=["node", "year"]
    )
    return demand_par
