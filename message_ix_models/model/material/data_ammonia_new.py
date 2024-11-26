from typing import Any, cast

import message_ix
import numpy as np
import pandas as pd
from message_ix import make_df

from message_ix_models import ScenarioInfo
from message_ix_models.model.material.material_demand import material_demand_calc
from message_ix_models.model.material.util import maybe_remove_water_tec, read_config
from message_ix_models.util import (
    broadcast,
    nodes_ex_world,
    package_data_path,
    same_node,
)

CONVERSION_FACTOR_NH3_N = 17 / 14

ssp_mode_map = {
    "SSP1": "CTS core",
    "SSP2": "RTS core",
    "SSP3": "RTS high",
    "SSP4": "CTS high",
    "SSP5": "RTS high",
    "LED": "CTS core",  # TODO: move to even lower projection
}

iea_elasticity_map = {
    "CTS core": (0.3, 0.3),
    "CTS high": (0.48, 0.48),
    "RTS core": (0.3, 0.3),
    "RTS high": (0.48, 0.48),
}


def gen_all_NH3_fert(
    scenario: message_ix.Scenario, dry_run: bool = False
) -> dict[str, pd.DataFrame]:
    return {
        **gen_data(scenario),
        **gen_data_rel(scenario),
        **gen_data_ts(scenario),
        **gen_demand(),
        **gen_land_input(scenario),
        **gen_resid_demand_NH3(scenario),
    }


def broadcast_years(
    df_new: pd.DataFrame, max_lt: int, act_years: pd.Series, vtg_years: pd.Series
) -> pd.DataFrame:
    if "year_act" in df_new.columns:
        df_new = df_new.pipe(same_node).pipe(broadcast, year_act=act_years)

        if "year_vtg" in df_new.columns:
            df_new = df_new.pipe(
                broadcast,
                year_vtg=np.linspace(
                    0, int(max_lt / 5), int(max_lt / 5 + 1), dtype=int
                ),
            )
            df_new["year_vtg"] = df_new["year_act"] - 5 * df_new["year_vtg"]
            # remove years that are not in scenario set
            df_new = df_new[~df_new["year_vtg"].isin([2065, 2075, 2085, 2095, 2105])]
    else:
        if "year_vtg" in df_new.columns:
            df_new = df_new.pipe(same_node).pipe(broadcast, year_vtg=vtg_years)
    return df_new


def gen_data(
    scenario: message_ix.Scenario,
    dry_run=False,
    add_ccs: bool = True,
    lower_costs: bool = False,
) -> dict[str, pd.DataFrame]:
    s_info = ScenarioInfo(scenario)
    # s_info.yv_ya
    nodes = nodes_ex_world(s_info.N)

    df = pd.read_excel(
        package_data_path(
            "material",
            "ammonia",
            "fert_techno_economic.xlsx",
        ),
        sheet_name="data_R12",
    )

    par_dict = {key: value for (key, value) in df.groupby("parameter")}
    for i in par_dict.keys():
        par_dict[i] = par_dict[i].dropna(axis=1)

    vtg_years = s_info.yv_ya[s_info.yv_ya.year_vtg > 2000]["year_vtg"]
    act_years = s_info.yv_ya[s_info.yv_ya.year_vtg > 2000]["year_act"]
    max_lt = par_dict["technical_lifetime"].value.max()
    vtg_years = vtg_years.drop_duplicates()
    act_years = act_years.drop_duplicates()

    for par_name in par_dict.keys():
        df = par_dict[par_name]
        # remove "default" node name to broadcast with all scenario regions later
        df["node_loc"] = df["node_loc"].apply(lambda x: None if x == "default" else x)
        df = df.to_dict()

        df_new = make_df(par_name, **df)
        # split df into df with default values and df with regionalized values
        df_new_no_reg = df_new[df_new["node_loc"].isna()]
        df_new_reg = df_new[~df_new["node_loc"].isna()]
        # broadcast regions to default parameter values
        df_new_no_reg = df_new_no_reg.pipe(broadcast, node_loc=nodes)
        df_new = pd.concat([df_new_reg, df_new_no_reg])

        # broadcast scenario years
        df_new = broadcast_years(df_new, max_lt, act_years, vtg_years)
        # set import/export node_dest/origin to GLB for input/output
        set_exp_imp_nodes(df_new)
        par_dict[par_name] = df_new

    if lower_costs:
        par_dict = experiment_lower_CPA_SAS_costs(par_dict)

    df_lifetime = par_dict.get("technical_lifetime")
    dict_lifetime = (
        df_lifetime.loc[:, ["technology", "value"]]
        .set_index("technology")
        .to_dict()["value"]
    )

    class missingdict(dict):
        def __missing__(self, key):
            return 1

    dict_lifetime = missingdict(dict_lifetime)
    for i in par_dict.keys():
        if ("year_vtg" in par_dict[i].columns) & ("year_act" in par_dict[i].columns):
            df_temp = par_dict[i]
            df_temp["lifetime"] = df_temp["technology"].map(dict_lifetime)
            df_temp = df_temp[
                (df_temp["year_act"] - df_temp["year_vtg"]) <= df_temp["lifetime"]
            ]
            par_dict[i] = df_temp.drop("lifetime", axis="columns")
    pars = ["inv_cost", "fix_cost"]
    tec_list = [
        "biomass_NH3",
        "electr_NH3",
        "gas_NH3",
        "coal_NH3",
        "fueloil_NH3",
        "biomass_NH3_ccs",
        "gas_NH3_ccs",
        "coal_NH3_ccs",
        "fueloil_NH3_ccs",
    ]
    cost_conv = pd.read_excel(
        package_data_path("material", "ammonia", "cost_conv_nh3.xlsx"),
        sheet_name="Sheet1",
        index_col=0,
    )
    for p in pars:
        conv_cost_df = pd.DataFrame()
        df = par_dict[p]
        for tec in tec_list:
            year_col = "year_vtg" if p == "inv_cost" else "year_act"

            df_tecs = df[df["technology"] == tec]
            df_tecs = df_tecs.merge(cost_conv, left_on=year_col, right_index=True)
            df_tecs_nam = df_tecs[df_tecs["node_loc"] == "R12_NAM"]
            df_tecs = df_tecs.merge(
                df_tecs_nam[[year_col, "value"]], left_on=year_col, right_on=year_col
            )
            df_tecs["diff"] = df_tecs["value_x"] - df_tecs["value_y"]
            df_tecs["diff"] = df_tecs["diff"] * (1 - df_tecs["convergence"])
            df_tecs["new_val"] = df_tecs["value_x"] - df_tecs["diff"]
            df_tecs["value"] = df_tecs["new_val"]
            conv_cost_df = pd.concat([conv_cost_df, make_df(p, **df_tecs)])
        par_dict[p] = pd.concat([df[~df["technology"].isin(tec_list)], conv_cost_df])

    # HACK: quick fix to enable compatibility with water build
    maybe_remove_water_tec(scenario, par_dict)

    return par_dict


def gen_data_rel(scenario, dry_run=False, add_ccs: bool = True):
    s_info = ScenarioInfo(scenario)
    # s_info.yv_ya
    nodes = s_info.N
    if "World" in nodes:
        nodes.pop(nodes.index("World"))
    if "R12_GLB" in nodes:
        nodes.pop(nodes.index("R12_GLB"))

    df = pd.read_excel(
        package_data_path(
            "material",
            "ammonia",
            "fert_techno_economic.xlsx",
        ),
        sheet_name="relations_R12",
    )
    df.groupby("parameter")
    par_dict = {key: value for (key, value) in df.groupby("parameter")}
    # for i in par_dict.keys():
    # par_dict[i] = par_dict[i].dropna(axis=1)

    act_years = s_info.yv_ya[s_info.yv_ya.year_vtg > 2000]["year_act"]
    act_years = act_years.drop_duplicates()

    for par_name in par_dict.keys():
        df = par_dict[par_name]
        # remove "default" node name to broadcast with all scenario regions later
        df["node_rel"] = df["node_rel"].apply(lambda x: None if x == "all" else x)
        df_dict = cast(dict[str, Any], df.to_dict())
        df = make_df(par_name, **df_dict)
        # split df into df with default values and df with regionalized values
        df_all_regs = df[df["node_rel"].isna()]
        df_single_regs = df.copy(deep=True).loc[~df["node_rel"].isna()]

        # broadcast regions to default parameter values
        df_all_regs = df_all_regs.pipe(broadcast, node_rel=nodes)

        def same_node_if_nan(df):
            if df["node_loc"] == "same":
                df["node_loc"] = df["node_rel"]
            return df

        if "node_loc" in df_all_regs.columns:
            df_all_regs = df_all_regs.apply(lambda x: same_node_if_nan(x), axis=1)

        if "node_loc" in df_single_regs.columns:
            df_single_regs["node_loc"] = df_single_regs["node_loc"].apply(
                lambda x: None if x == "all" else x
            )
            df_new_reg_all_regs = df_single_regs.copy(deep=True).loc[
                df_single_regs["node_loc"].isna()
            ]
            df_new_reg_all_regs = df_new_reg_all_regs.pipe(broadcast, node_loc=nodes)
            df_single_regs = pd.concat(
                [
                    df_single_regs[~df_single_regs["node_loc"].isna()],
                    df_new_reg_all_regs,
                ]
            )

        df = pd.concat([df_single_regs, df_all_regs])

        # broadcast scenario years
        if "year_rel" in df.columns:
            df = df.pipe(broadcast, year_rel=act_years)

        if "year_act" in df.columns:
            df["year_act"] = df["year_rel"]

        # set import/export node_dest/origin to GLB for input/output
        set_exp_imp_nodes(df)
        par_dict[par_name] = df

    return par_dict


def gen_data_ts(
    scenario: message_ix.Scenario, dry_run: bool = False, add_ccs: bool = True
) -> dict[str, pd.DataFrame]:
    s_info = ScenarioInfo(scenario)
    # s_info.yv_ya
    nodes = s_info.N
    if "World" in nodes:
        nodes.pop(nodes.index("World"))
    if "R12_GLB" in nodes:
        nodes.pop(nodes.index("R12_GLB"))

    df = pd.read_excel(
        package_data_path(
            "material",
            "ammonia",
            "fert_techno_economic.xlsx",
        ),
        sheet_name="timeseries_R12",
    )
    df["year_act"] = df["year_act"].astype("Int64")
    df["year_vtg"] = df["year_vtg"].astype("Int64")
    par_dict = {key: value for (key, value) in df.groupby("parameter")}
    for i in par_dict.keys():
        par_dict[i] = par_dict[i].dropna(axis=1)

    for par_name in par_dict.keys():
        df = par_dict[par_name]
        # remove "default" node name to broadcast with all scenario regions later
        df["node_loc"] = df["node_loc"].apply(lambda x: None if x == "default" else x)
        df = df.to_dict()

        df_new = make_df(par_name, **df)
        # split df into df with default values and df with regionalized values
        df_new_no_reg = df_new[df_new["node_loc"].isna()]
        df_new_reg = df_new[~df_new["node_loc"].isna()]
        # broadcast regions to default parameter values
        df_new_no_reg = df_new_no_reg.pipe(broadcast, node_loc=nodes)
        df_new = pd.concat([df_new_reg, df_new_no_reg])

        # set import/export node_dest/origin to GLB for input/output
        set_exp_imp_nodes(df_new)
        par_dict[par_name] = df_new

    # convert floats
    par_dict["historical_activity"] = par_dict["historical_activity"].astype(
        {"year_act": "int32"}
    )
    par_dict["historical_new_capacity"] = par_dict["historical_new_capacity"].astype(
        {"year_vtg": "int32"}
    )
    par_dict["bound_activity_lo"] = par_dict["bound_activity_lo"].astype(
        {"year_act": "int32"}
    )

    return par_dict


def set_exp_imp_nodes(df: pd.DataFrame) -> None:
    if "node_dest" in df.columns:
        df.loc[df["technology"].str.contains("export"), "node_dest"] = "R12_GLB"
    if "node_origin" in df.columns:
        df.loc[df["technology"].str.contains("import"), "node_origin"] = "R12_GLB"


def read_demand() -> dict[str, pd.DataFrame]:
    """Read and clean data from
    :file:`CD-Links SSP2 N-fertilizer demand.Global.xlsx`."""
    # Demand scenario [Mt N/year] from GLOBIOM

    N_demand_GLO = pd.read_excel(
        package_data_path(
            "material",
            "ammonia",
            "nh3_fertilizer_demand.xlsx",
        ),
        sheet_name="NFertilizer_demand",
    )

    # NH3 feedstock share by region in 2010 (from http://ietd.iipnetwork.org/content/ammonia#benchmarks)
    feedshare_GLO = pd.read_excel(
        package_data_path(
            "material",
            "ammonia",
            "nh3_fertilizer_demand.xlsx",
        ),
        sheet_name="NH3_feedstock_share",
        skiprows=14,
    )

    # Read parameters in xlsx
    te_params = pd.read_excel(
        package_data_path("material", "ammonia", "nh3_fertilizer_demand.xlsx"),
        sheet_name="old_TE_sheet",
        engine="openpyxl",
        nrows=72,
    )

    n_inputs_per_tech = 12  # Number of input params per technology

    input_fuel = te_params[2010][
        list(range(4, te_params.shape[0], n_inputs_per_tech))
    ].reset_index(drop=True)

    capacity_factor = te_params[2010][
        list(range(11, te_params.shape[0], n_inputs_per_tech))
    ].reset_index(drop=True)

    # Regional N demand in 2010
    ND = N_demand_GLO.loc[N_demand_GLO.Scenario == "NoPolicy", ["Region", 2010]]
    ND = ND[ND.Region != "World"]
    ND.Region = "R12_" + ND.Region
    ND = ND.set_index("Region")

    # Derive total energy (GWa) of NH3 production (based on demand 2010)
    N_energy = feedshare_GLO[feedshare_GLO.Region != "R12_GLB"].join(ND, on="Region")
    N_energy = pd.concat(
        [
            N_energy.Region,
            N_energy[["gas_pct", "coal_pct", "oil_pct"]].multiply(
                N_energy[2010], axis="index"
            ),
        ],
        axis=1,
    )
    N_energy.gas_pct *= input_fuel[2] * CONVERSION_FACTOR_NH3_N  # NH3 / N
    N_energy.coal_pct *= input_fuel[3] * CONVERSION_FACTOR_NH3_N
    N_energy.oil_pct *= input_fuel[4] * CONVERSION_FACTOR_NH3_N
    N_energy = pd.concat(
        [N_energy.Region, N_energy.sum(axis=1, numeric_only=True)], axis=1
    ).rename(columns={0: "totENE", "Region": "node"})  # GWa

    # N_trade_R12 = pd.read_csv(
    #    package_data_path("material", "ammonia", "trade.FAO.R12.csv"), index_col=0
    # )
    N_trade_R12 = pd.read_excel(
        package_data_path(
            "material",
            "ammonia",
            "nh3_fertilizer_demand.xlsx",
        ),
        sheet_name="NFertilizer_trade",
    )  # , index_col=0)

    N_trade_R12.region = "R12_" + N_trade_R12.region
    N_trade_R12.quantity = N_trade_R12.quantity
    N_trade_R12.unit = "t"
    N_trade_R12 = N_trade_R12.assign(time="year")
    N_trade_R12 = N_trade_R12.rename(
        columns={
            "quantity": "value",
            "region": "node_loc",
            "year": "year_act",
        }
    )

    df = N_trade_R12.loc[N_trade_R12.year_act == 2010,]
    df = df.pivot(index="node_loc", columns="type", values="value")
    NP = pd.DataFrame({"netimp": df["import"] - df.export, "demand": ND[2010]})
    NP["prod"] = NP.demand - NP.netimp

    # NH3_trade_R12 = pd.read_csv(
    #    package_data_path(
    #        "material", "ammonia", "NH3_trade_BACI_R12_aggregation.csv"
    #    )
    # )  # , index_col=0)
    NH3_trade_R12 = pd.read_excel(
        package_data_path(
            "material",
            "ammonia",
            "nh3_fertilizer_demand.xlsx",
        ),
        sheet_name="NH3_trade_R12_aggregated",
    )

    NH3_trade_R12.region = "R12_" + NH3_trade_R12.region
    NH3_trade_R12.quantity = NH3_trade_R12.quantity / 1e6
    NH3_trade_R12.unit = "t"
    NH3_trade_R12 = NH3_trade_R12.assign(time="year")
    NH3_trade_R12 = NH3_trade_R12.rename(
        columns={
            "quantity": "value",
            "region": "node_loc",
            "year": "year_act",
        }
    )

    # Derive total energy (GWa) of NH3 production (based on demand 2010)
    N_feed = feedshare_GLO[feedshare_GLO.Region != "R11_GLB"].join(NP, on="Region")
    N_feed = pd.concat(
        [
            N_feed.Region,
            N_feed[["gas_pct", "coal_pct", "oil_pct"]].multiply(
                N_feed["prod"], axis="index"
            ),
        ],
        axis=1,
    )
    N_feed.gas_pct *= input_fuel[2] * 17 / 14
    N_feed.coal_pct *= input_fuel[3] * 17 / 14
    N_feed.oil_pct *= input_fuel[4] * 17 / 14
    N_feed = pd.concat(
        [N_feed.Region, N_feed.sum(axis=1, numeric_only=True)], axis=1
    ).rename(columns={0: "totENE", "Region": "node"})

    # Process the regional historical activities

    fs_GLO = feedshare_GLO.copy()
    fs_GLO.insert(1, "bio_pct", 0.0)
    fs_GLO.insert(2, "elec_pct", 0.0)
    # 17/14 NH3:N ratio, to get NH3 activity based on N demand
    # => No NH3 loss assumed during production

    fs_GLO.iloc[:, 1:6] = input_fuel[5] * fs_GLO.iloc[:, 1:6]
    fs_GLO.insert(6, "NH3_to_N", 1)

    # Share of feedstocks for NH3 prodution
    # (based on 2010 => Assumed fixed for any past years)
    feedshare = fs_GLO.sort_values(["Region"]).set_index("Region").drop("R12_GLB")

    # Get historical N demand from SSP2-nopolicy (may need to vary for diff scenarios)
    N_demand_raw = N_demand_GLO[N_demand_GLO["Region"] != "World"].copy()
    N_demand_raw["Region"] = "R12_" + N_demand_raw["Region"]
    N_demand_raw = N_demand_raw.set_index("Region")
    N_demand = (
        N_demand_raw.loc[
            (N_demand_raw.Scenario == "NoPolicy")  # & (N_demand_raw.Region != "World")
        ]
        # .reset_index()
        .loc[:, 2010]
    )  # 2010 tot N demand
    # N_demand = N_demand.repeat(6)
    # act2010 = (feedshare.values.flatten() * N_demand).reset_index(drop=True)

    return {
        "act2010": feedshare.mul(N_demand, axis=0),
        "feedshare_GLO": feedshare_GLO,
        "ND": ND,
        "N_energy": N_energy,
        "feedshare": feedshare,
        # "act2010": act2010,
        "capacity_factor": capacity_factor,
        "N_feed": N_feed,
        "N_trade_R12": N_trade_R12,
        "NH3_trade_R12": NH3_trade_R12,
    }


def gen_demand() -> dict[str, pd.DataFrame]:
    N_energy = read_demand()["N_feed"]  # updated feed with imports accounted

    demand_fs_org = pd.read_excel(
        package_data_path("material", "ammonia", "nh3_fertilizer_demand.xlsx"),
        sheet_name="demand_i_feed_R12",
    )

    df = demand_fs_org.loc[demand_fs_org.year == 2010, :].join(
        N_energy.set_index("node"), on="node"
    )
    sh = pd.DataFrame(
        {
            "node": demand_fs_org.loc[demand_fs_org.year == 2010, "node"],
            "r_feed": df.totENE / df.value,
        }
    )  # share of NH3 energy among total feedstock (no trade assumed)
    df = demand_fs_org.join(sh.set_index("node"), on="node")
    df.value *= 1 - df.r_feed  # Carve out the same % from tot i_feed values
    df = df.drop("r_feed", axis=1)
    df = df.drop("Unnamed: 0", axis=1)
    # TODO: refactor with a more sophisticated solution to reduce i_feed
    df.loc[df["value"] < 0, "value"] = 0  # temporary solution to avoid negative values
    return {"demand": df}


def gen_resid_demand_NH3(scenario: message_ix.Scenario) -> dict[str, pd.DataFrame]:
    context = read_config()
    ssp = context["ssp"]

    default_gdp_elasticity_2020, default_gdp_elasticity_2030 = iea_elasticity_map[
        ssp_mode_map[ssp]
    ]
    df_residual = material_demand_calc.gen_demand_petro(
        scenario, "NH3", default_gdp_elasticity_2020, default_gdp_elasticity_2030
    )

    return {"demand": df_residual}


def gen_land_input(scenario: message_ix.Scenario) -> dict[str, pd.DataFrame]:
    df = scenario.par("land_output", {"commodity": "Fertilizer Use|Nitrogen"})
    df["level"] = "final_material"
    return {"land_input": df}


def experiment_lower_CPA_SAS_costs(par_dict: dict) -> dict[str, pd.DataFrame]:
    cost_list = ["inv_cost", "fix_cost"]
    scaler = {
        "R12_RCPA": [0.66 * 0.91, 0.75 * 0.9],
        "R12_CHN": [0.66 * 0.91, 0.75 * 0.9],
        "R12_SAS": [0.59, 1],
    }
    tec_list = ["fueloil_NH3", "coal_NH3"]
    for c in cost_list:
        df = par_dict[c]
        for k in scaler.keys():
            df_tmp = df.loc[df["node_loc"] == k]
            for e, t in enumerate(tec_list):
                df_tmp.loc[df_tmp["technology"] == t, "value"] = df_tmp.loc[
                    df_tmp["technology"] == t, "value"
                ].mul(scaler.get(k)[e])
                df_tmp.loc[df_tmp["technology"] == t + "_ccs", "value"] = df_tmp.loc[
                    df_tmp["technology"] == t + "_ccs", "value"
                ].mul(scaler.get(k)[e])
            df.loc[df["node_loc"] == k] = df_tmp
        par_dict[c] = df

    return par_dict
