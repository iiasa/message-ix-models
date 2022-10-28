import message_ix
import message_data
import ixmp as ix

import pandas as pd
import numpy as np

from message_ix_models import ScenarioInfo
from message_ix import make_df
from message_ix_models.util import broadcast, same_node
from .util import read_config

CONVERSION_FACTOR_NH3_N = 17 / 14
context = read_config()


def gen_all_NH3_fert(scenario, dry_run=False):
    return {
        **gen_data(scenario),
        **gen_data_rel(scenario),
        **gen_data_ts(scenario),
        **gen_demand(),
        **gen_land_input(scenario),
    }


def gen_data(scenario, dry_run=False, add_ccs: bool = True):
    s_info = ScenarioInfo(scenario)
    # s_info.yv_ya
    nodes = s_info.N
    if "World" in nodes:
        nodes.pop(nodes.index("World"))
    if "R12_GLB" in nodes:
        nodes.pop(nodes.index("R12_GLB"))

    df = pd.read_excel(
        context.get_local_path(
            "material",
            "ammonia",
            "new concise input files",
            "fert_techno_economic.xlsx",
        ),
        sheet_name="data_R12",
    )
    df.groupby("parameter")
    par_dict = {key: value for (key, value) in df.groupby("parameter")}
    for i in par_dict.keys():
        par_dict[i] = par_dict[i].dropna(axis=1)

    vtg_years = s_info.yv_ya[s_info.yv_ya.year_vtg > 2000]["year_vtg"]
    act_years = s_info.yv_ya[s_info.yv_ya.year_vtg > 2000]["year_act"]

    try:
        max_lt = par_dict["technical_lifetime"].value.max()
    except KeyError:
        print("lifetime not in xlsx")

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
                #remove years that are not in scenario set
                df_new = df_new[~df_new["year_vtg"].isin([2065, 2075, 2085, 2095, 2105])]
        else:
            if "year_vtg" in df_new.columns:
                df_new = df_new.pipe(same_node).pipe(broadcast, year_vtg=vtg_years)

        # set import/export node_dest/origin to GLB for input/output
        set_exp_imp_nodes(df_new)
        par_dict[par_name] = df_new

    df = par_dict.get("technical_lifetime")
    dict_lifetime = df.loc[:,["technology", "value"]].set_index("technology").to_dict()[
        "value"]
    for i in par_dict.keys():
        if ("year_vtg" in par_dict[i].columns) & ("year_act" in par_dict[i].columns):
            df_temp = par_dict[i]
            df_temp["lifetime"] = df_temp["technology"].map(dict_lifetime)
            df_temp = df_temp[(df_temp["year_act"] - df_temp["year_vtg"]) < df_temp["lifetime"]]
            par_dict[i] = df_temp.drop("lifetime", axis="columns")
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
        context.get_local_path(
            "material",
            "ammonia",
            "new concise input files",
            "fert_techno_economic.xlsx",
        ),
        sheet_name="relations_R12",
    )
    df.groupby("parameter")
    par_dict = {key: value for (key, value) in df.groupby("parameter")}
    for i in par_dict.keys():
        par_dict[i] = par_dict[i].dropna(axis=1)

    act_years = s_info.yv_ya[s_info.yv_ya.year_vtg > 2000]["year_act"]
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
        df_new_no_reg = df_new_no_reg.pipe(broadcast, node_rel=nodes)
        if "node_loc" in df_new_no_reg.columns:
            df_new_no_reg["node_loc"] = df_new_no_reg["node_rel"]
        df_new = pd.concat([df_new_reg, df_new_no_reg])

        # broadcast scenario years
        if "year_rel" in df_new.columns:
            df_new = df_new.pipe(same_node).pipe(broadcast, year_rel=act_years)

        if "year_act" in df_new.columns:
            df_new["year_act"] = df_new["year_rel"]

        # set import/export node_dest/origin to GLB for input/output
        set_exp_imp_nodes(df_new)
        par_dict[par_name] = df_new

    return par_dict


def gen_data_ts(scenario, dry_run=False, add_ccs: bool = True):
    s_info = ScenarioInfo(scenario)
    # s_info.yv_ya
    nodes = s_info.N
    if "World" in nodes:
        nodes.pop(nodes.index("World"))
    if "R12_GLB" in nodes:
        nodes.pop(nodes.index("R12_GLB"))

    df = pd.read_excel(
        context.get_local_path(
            "material",
            "ammonia",
            "new concise input files",
            "fert_techno_economic.xlsx",
        ),
        sheet_name="timeseries_R12",
    )
    df.groupby("parameter")
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

    #convert floats
    par_dict["historical_activity"] = par_dict["historical_activity"].astype({'year_act': 'int32'})
    par_dict["historical_new_capacity"] = par_dict["historical_new_capacity"].astype({'year_vtg': 'int32'})

    return par_dict


def set_exp_imp_nodes(df):
    if "node_dest" in df.columns:
        df.loc[df["technology"].str.contains("export"), "node_dest"] = "R12_GLB"
    if "node_origin" in df.columns:
        df.loc[df["technology"].str.contains("import"), "node_origin"] = "R12_GLB"


def read_demand():
    """Read and clean data from :file:`CD-Links SSP2 N-fertilizer demand.Global.xlsx`."""
    # Demand scenario [Mt N/year] from GLOBIOM
    context = read_config()

    N_demand_GLO = pd.read_excel(
        context.get_local_path(
            "material",
            "ammonia",
            "new concise input files",
            "nh3_fertilizer_demand.xlsx",
        ),
        sheet_name="NFertilizer_demand",
    )

    # NH3 feedstock share by region in 2010 (from http://ietd.iipnetwork.org/content/ammonia#benchmarks)
    feedshare_GLO = pd.read_excel(
        context.get_local_path(
            "material",
            "ammonia",
            "new concise input files",
            "nh3_fertilizer_demand.xlsx",
        ),
        sheet_name="NH3_feedstock_share",
        skiprows=14,
    )

    """# Read parameters in xlsx
    te_params = data = pd.read_excel(
        context.get_local_path(
            "material", "ammonia", "n-fertilizer_techno-economic_new.xlsx"
        ),
        sheet_name="Sheet1",
        engine="openpyxl",
        nrows=72,
    )

    n_inputs_per_tech = 12  # Number of input params per technology

    input_fuel = te_params[2010][
        list(range(4, te_params.shape[0], n_inputs_per_tech))
    ].reset_index(drop=True)"""
    # input_fuel[0:5] = input_fuel[0:5] * CONVERSION_FACTOR_PJ_GWa  # 0.0317 GWa/PJ, GJ/t = PJ/Mt NH3

    te_params_new = pd.read_excel(
        context.get_local_path(
            "material",
            "ammonia",
            "new concise input files",
            "fert_techno_economic.xlsx"),
            sheet_name="data_R12")

    tec_dict = [
        "biomass_NH3",
        "electr_NH3",
        "gas_NH3",
        "coal_NH3",
        "fueloil_NH3",
        "NH3_to_N_fertil",
    ]

    input_fuel = te_params_new[
        (te_params_new["parameter"] == "input") & ~(te_params_new["commodity"].isin(["freshwater_supply"])) & (
            te_params_new["technology"].isin(tec_dict))]
    input_fuel = input_fuel.iloc[[0, 2, 4, 5, 7, 9]].set_index("technology").loc[tec_dict, "value"]

    #capacity_factor = te_params[2010][
    #    list(range(11, te_params.shape[0], n_inputs_per_tech))
    #].reset_index(drop=True)

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
    N_energy = pd.concat([N_energy.Region, N_energy.sum(axis=1)], axis=1).rename(
        columns={0: "totENE", "Region": "node"}
    )  # GWa

    #N_trade_R12 = pd.read_csv(
    #    context.get_local_path("material", "ammonia", "trade.FAO.R12.csv"), index_col=0
    #)
    N_trade_R12 = pd.read_excel(
        context.get_local_path(
            "material",
            "ammonia",
            "new concise input files",
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

    df = N_trade_R12.loc[
        N_trade_R12.year_act == 2010,
    ]
    df = df.pivot(index="node_loc", columns="type", values="value")
    NP = pd.DataFrame({"netimp": df["import"] - df.export, "demand": ND[2010]})
    NP["prod"] = NP.demand - NP.netimp

    #NH3_trade_R12 = pd.read_csv(
    #    context.get_local_path(
    #        "material", "ammonia", "NH3_trade_BACI_R12_aggregation.csv"
    #    )
    #)  # , index_col=0)
    NH3_trade_R12 = pd.read_excel(
        context.get_local_path(
            "material",
            "ammonia",
            "new concise input files",
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
    N_feed = pd.concat([N_feed.Region, N_feed.sum(axis=1)], axis=1).rename(
        columns={0: "totENE", "Region": "node"}
    )

    # Process the regional historical activities

    fs_GLO = feedshare_GLO.copy()
    fs_GLO.insert(1, "bio_pct", 0)
    fs_GLO.insert(2, "elec_pct", 0)
    # 17/14 NH3:N ratio, to get NH3 activity based on N demand => No NH3 loss assumed during production
    fs_GLO.iloc[:, 1:6] = input_fuel[5] * fs_GLO.iloc[:, 1:6]
    fs_GLO.insert(6, "NH3_to_N", 1)

    # Share of feedstocks for NH3 prodution (based on 2010 => Assumed fixed for any past years)
    feedshare = fs_GLO.sort_values(["Region"]).set_index("Region").drop("R12_GLB")

    # Get historical N demand from SSP2-nopolicy (may need to vary for diff scenarios)
    N_demand_raw = N_demand_GLO[N_demand_GLO["Region"]!="World"].copy()
    N_demand_raw["Region"] = "R12_" + N_demand_raw["Region"]
    N_demand_raw = N_demand_raw.set_index("Region")
    N_demand = (
        N_demand_raw.loc[
            (N_demand_raw.Scenario == "NoPolicy")# & (N_demand_raw.Region != "World")
            ]
            #.reset_index()
            .loc[:, 2010]
    )  # 2010 tot N demand
    #N_demand = N_demand.repeat(6)
    #act2010 = (feedshare.values.flatten() * N_demand).reset_index(drop=True)

    return {
        "act2010": feedshare.mul(N_demand, axis=0),
        "feedshare_GLO": feedshare_GLO,
        "ND": ND,
        "N_energy": N_energy,
        "feedshare": feedshare,
        "act2010": act2010,
        #"capacity_factor": capacity_factor,
        "N_feed": N_feed,
        "N_trade_R12": N_trade_R12,
        "NH3_trade_R12": NH3_trade_R12,
    }


def gen_demand():
    context = read_config()

    N_energy = read_demand()["N_feed"]  # updated feed with imports accounted

    demand_fs_org = pd.read_excel(
        context.get_local_path("material", "ammonia",
                               "new concise input files",
                               "nh3_fertilizer_demand.xlsx"),
        sheet_name="demand_i_feed_R12"
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


def gen_land_input(scenario):
    df = scenario.par("land_output", {"commodity": "Fertilizer Use|Nitrogen"})
    df["level"] = "final_material"
    return {"land_input": df}
