import message_ix
import message_data
import ixmp as ix

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from message_ix_models import ScenarioInfo
from message_ix import make_df
from message_ix_models.util import broadcast, same_node
from .util import read_config

context = read_config()


def gen_data_methanol(scenario):
    df_pars = pd.read_excel(
        context.get_local_path("material", "methanol_sensitivity_pars.xlsx"),
        sheet_name="Sheet1",
        dtype=object,
    )
    pars = df_pars.set_index("par").to_dict()["value"]

    dict1 = gen_data_meth_h2()
    dict2 = gen_data_meth_bio()
    keys = set(list(dict1.keys()) + list(dict2.keys()))
    new_dict = {}
    for i in keys:
        if (i in dict2.keys()) & (i in dict1.keys()):
            new_dict[i] = pd.concat([dict1[i], dict2[i]])
        else:
            new_dict[i] = dict1[i]

    dict3 = pd.read_excel(
        context.get_local_path("material", "meth_t_d_material_pars.xlsx"),
        sheet_name=None,
    )

    keys = set(list(dict3.keys()) + list(new_dict.keys()))
    new_dict2 = {}
    for i in keys:
        if (i in dict3.keys()) & (i in new_dict.keys()):
            new_dict2[i] = pd.concat([new_dict[i], dict3[i]])
        if (i in dict3.keys()) & ~(i in new_dict.keys()):
            new_dict2[i] = dict3[i]
        if ~(i in dict3.keys()) & (i in new_dict.keys()):
            new_dict2[i] = new_dict[i]

    df_final = gen_meth_residual_demand(pars["methanol_elasticity"])
    df_final["value"] = df_final["value"].apply(lambda x: x * 0.5)
    new_dict2["demand"] = df_final

    # fix demand infeasibility
    act = scenario.par("historical_activity")
    row = (
        act[act["technology"].str.startswith("meth")]
        .sort_values("value", ascending=False)
        .iloc[0]
    )
    # china meth_coal production (90% coal share on 2015 47 Mt total; 1.348 = Mt to GWa )
    row["value"] = (47 / 1.3498) * 0.9
    new_dict2["historical_activity"] = pd.concat(
        [new_dict2["historical_activity"], pd.DataFrame(row).T]
    )
    # derived from graphic in "Methanol production statstics.xlsx/China demand split" diagram
    hist_cap = message_ix.make_df(
        "historical_new_capacity",
        node_loc="R12_CHN",
        technology="meth_coal",
        year_vtg=2015,
        value=9.6,
        unit="GW",
    )
    new_dict2["historical_new_capacity"] = hist_cap
    # fix demand infeasibility
    # act = scenario.par("historical_activity")
    # row = act[act["technology"].str.startswith("meth")].sort_values("value", ascending=False).iloc[0]
    # row["value"] = 0.0
    new_dict2["historical_activity"] = pd.concat(
        [new_dict2["historical_activity"], pd.DataFrame(row).T]
    )
    df_ng = pd.read_excel(
        context.get_local_path("material", "meth_ng_techno_economic.xlsx")
    )
    new_dict2["historical_activity"] = pd.concat([new_dict2["historical_activity"], df_ng])

    mto_dict = gen_data_meth_chemicals(scenario, "MTO")
    keys = set(list(new_dict2.keys()) + list(mto_dict.keys()))
    for i in keys:
        if (i in new_dict2.keys()) & (i in mto_dict.keys()):
            new_dict2[i] = pd.concat([new_dict2[i], mto_dict[i]])
        if ~(i in new_dict2.keys()) & (i in mto_dict.keys()):
            new_dict2[i] = mto_dict[i]

    ch2o_dict = gen_data_meth_chemicals(scenario, "Formaldehyde")
    keys = set(list(new_dict2.keys()) + list(ch2o_dict.keys()))
    for i in keys:
        if (i in new_dict2.keys()) & (i in ch2o_dict.keys()):
            new_dict2[i] = pd.concat([new_dict2[i], ch2o_dict[i]])
        if ~(i in new_dict2.keys()) & (i in ch2o_dict.keys()):
            new_dict2[i] = ch2o_dict[i]

    resin_dict = gen_data_meth_chemicals(scenario, "Resins")
    keys = set(list(new_dict2.keys()) + list(resin_dict.keys()))
    for i in keys:
        if (i in new_dict2.keys()) & (i in resin_dict.keys()):
            new_dict2[i] = pd.concat([new_dict2[i], resin_dict[i]])
        if ~(i in new_dict2.keys()) & (i in resin_dict.keys()):
            new_dict2[i] = resin_dict[i]

    df_comm = gen_resin_demand(scenario, pars["resin_share"], "comm", pars["wood_scenario"], pars["pathway"])
    df_resid = gen_resin_demand(scenario, pars["resin_share"], "residential", pars["wood_scenario"], pars["pathway"])
    df_resin_demand = df_comm.copy(deep=True)
    df_resin_demand["value"] = df_comm["value"] + df_resid["value"]
    new_dict2["demand"] = pd.concat([new_dict2["demand"], df_resin_demand])

    new_dict2["input"] = pd.concat([new_dict2["input"], add_methanol_trp_additives(scenario)])

    if pars["cbudget"]:
        emission_dict = {
            "node": "World",
            "type_emission": "TCE_CO2",
            "type_tec": "all",
            "type_year": "cumulative",
            "unit": "???",
        }
        new_dict2["bound_emission"] = make_df(
            "bound_emission", value=3667, **emission_dict
        )

    df = scenario.par("input", filters={"technology": "meth_t_d"})
    df["value"] = 1
    new_dict2["input"] = pd.concat([new_dict2["input"], df])

    return new_dict2


def gen_data_meth_h2():
    context = read_config()
    df_h2 = pd.read_excel(
        context.get_local_path("material", "meth_h2_techno_economic.xlsx"),
        sheet_name=None,
    )
    return df_h2


def gen_data_meth_bio():
    context = read_config()
    context.get_local_path("material", "meth_bio_techno_economic.xlsx")
    df_h2 = pd.read_excel(
        context.get_local_path("material", "meth_bio_techno_economic.xlsx"),
        sheet_name=None,
    )
    return df_h2


def gen_data_meth_chemicals(scenario, chemical):
    df = pd.read_excel(
        context.get_local_path("material", "MTO data collection.xlsx"),
        sheet_name=chemical,
        usecols=[1, 2, 3, 4, 6, 7],
    )
    # exclude emissions for now
    if chemical == "MTO":
        df = df.iloc[
            :10,
        ]
    if chemical == "Formaldehyde":
        df = df.iloc[
            :9,
        ]

    common = dict(
        # commodity="NH3",
        # level="secondary_material",
        mode="M1",
        time="year",
        time_dest="year",
        time_origin="year",
        emission="CO2_industry",  # confirm if correct
        relation="CO2_cc",
    )

    all_years = scenario.vintage_and_active_years()
    all_years = all_years[all_years["year_vtg"] > 1990]

    nodes = scenario.set("node")[1:]
    nodes = nodes.drop(5).reset_index(drop=True)

    par_dict = {k: pd.DataFrame() for k in (df["parameter"])}
    for i in df["parameter"]:
        for index, row in df[df["parameter"] == i].iterrows():
            par_dict[i] = pd.concat([par_dict[i],
                make_df(i, **all_years.to_dict(orient="list"), **row, **common)
                .pipe(broadcast, node_loc=nodes)
                .pipe(same_node)]
            )

            if i == "relation_activity":
                par_dict[i]["year_rel"] = par_dict[i]["year_act"]
                par_dict[i]["node_rel"] = par_dict[i]["node_loc"]

            if "unit" in par_dict[i].columns:
                par_dict[i]["unit"] = "???"

    hist_dict = {
        "node_loc": "R12_CHN",
        "technology": "MTO",
        "mode": "M1",
        "time": "year",
        "unit": "???",
    }
    if chemical == "MTO":
        par_dict["historical_activity"] = make_df(
            "historical_activity", value=4.5, year_act=2015, **hist_dict
        )
        # par_dict["historical_new_capacity"] = make_df("historical_new_capacity", value=[1.2, 1.2], year_vtg=[2015, 2020], **hist_dict)
        par_dict["historical_new_capacity"] = make_df(
            "historical_new_capacity", value=1.2, year_vtg=2015, **hist_dict
        )
        par_dict["bound_total_capacity_lo"] = make_df(
            "bound_total_capacity_lo", year_act=2020, value=9, **hist_dict
        )

    return par_dict


def add_methanol_trp_additives(scenario):
    df_loil = scenario.par("input")
    df_loil = df_loil[df_loil["technology"] == "loil_trp"]

    df_mtbe = pd.read_excel(
        context.get_local_path(
            "material", "Methanol production statistics (version 1).xlsx"
        ),
        # usecols=[1,2,3,4,6,7],
        skiprows=np.linspace(0, 65, 66),
        sheet_name="MTBE calc",
    )
    df_mtbe = df_mtbe.iloc[
        1:13,
    ]
    df_mtbe["node_loc"] = "R12_" + df_mtbe["node_loc"]
    df_mtbe = df_mtbe[["node_loc", "methanol energy%"]]
    df_biodiesel = pd.read_excel(
        context.get_local_path(
            "material", "Methanol production statistics (version 1).xlsx"
        ),
        skiprows=np.linspace(0, 37, 38),
        usecols=[1, 2],
        sheet_name="Biodiesel",
    )
    df_biodiesel["node_loc"] = "R12_" + df_biodiesel["node_loc"]
    df_total = df_biodiesel.merge(df_mtbe)
    df_total = df_total.assign(
        value=lambda x: x["methanol energy %"] + x["methanol energy%"]
    )

    def get_meth_share(df, node):
        return df[df["node_loc"] == node["node_loc"]]["value"].values[0]

    df_loil_meth = df_loil.copy(deep=True)
    df_loil_meth["value"] = df_loil.apply(lambda x: get_meth_share(df_total, x), axis=1)
    df_loil_meth["commodity"] = "methanol"
    df_loil["value"] = df_loil["value"] - df_loil_meth["value"]

    return pd.concat([df_loil, df_loil_meth])


def gen_resin_demand(scenario, resin_share, sector, buildings_scen, pathway="SHAPE"):
    df = pd.read_csv(
        context.get_local_path("material", "results_material_"+ pathway +"_" + sector + ".csv")
    )
    resin_intensity = resin_share
    df = df[df["scenario"] == buildings_scen]
    df = df[df["material"] == "wood"].assign(
        resin_demand=df["mat_demand_Mt"] * resin_intensity
    )
    df["R12"] = "R12_" + df["R12"]

    common = dict(
        mode="M1",
        time="year",
        time_dest="year",
        time_origin="year",
        emission="CO2_industry",  # confirm if correct,
        relation="CO2_cc",
        commodity="fcoh_resin",
        unit="???",
        level="final_material",
    )
    all_years = scenario.vintage_and_active_years()
    all_years = all_years[all_years["year_vtg"] > 1990]
    nodes = scenario.set("node")[1:]
    nodes = nodes.drop(5).reset_index(drop=True)

    df_demand = (
        make_df("demand", year=all_years["year_act"].unique()[:-1], **common)
        .pipe(broadcast, node=nodes)
        .merge(
            df[["R12", "year", "resin_demand"]],
            left_on=["node", "year"],
            right_on=["R12", "year"],
        )
    )
    df_demand["value"] = df_demand["resin_demand"]
    df_demand = make_df("demand", **df_demand)
    return df_demand


def gen_meth_residual_demand(gdp_elasticity):
    def get_demand_t1_with_income_elasticity(
        demand_t0, income_t0, income_t1, elasticity
    ):
        return (
            elasticity * demand_t0 * ((income_t1 - income_t0) / income_t0)
        ) + demand_t0

    df_gdp = pd.read_excel(
        context.get_local_path("material", "methanol demand.xlsx"),
        sheet_name="GDP_baseline",
    )

    df = df_gdp[(~df_gdp["Region"].isna()) & (df_gdp["Region"] != "World")]
    df = df.dropna(axis=1)

    df_demand_meth = pd.read_excel(
        context.get_local_path("material", "methanol demand.xlsx"),
        sheet_name="methanol_demand",
        skiprows=[12],
    )
    df_demand_meth = df_demand_meth[
        (~df_demand_meth["Region"].isna()) & (df_demand_meth["Region"] != "World")
    ]
    df_demand_meth = df_demand_meth.dropna(axis=1)

    df_demand = df.copy(deep=True)
    df_demand = df_demand.drop([2010, 2015, 2020], axis=1)

    years = list(df_demand_meth.columns[5:])
    dem_2020 = df_demand_meth[2020].values
    df_demand[2020] = dem_2020
    for i in range(len(years) - 1):
        income_year1 = years[i]
        income_year2 = years[i + 1]

        dem_2020 = get_demand_t1_with_income_elasticity(
            dem_2020, df[income_year1], df[income_year2], gdp_elasticity
        )
        df_demand[income_year2] = dem_2020

    df_melt = df_demand.melt(
        id_vars=["Region"], value_vars=df_demand.columns[5:], var_name="year"
    )
    return message_ix.make_df(
        "demand",
        unit="t",
        level="final_material",
        value=df_melt.value,
        time="year",
        commodity="methanol",
        year=df_melt.year,
        node=("R12_" + df_melt["Region"]),
    )


def get_meth_bio_cost_ratio(scenario, tec_name, cost_type):

    df = scenario.par(cost_type)
    df = df[df["technology"]==tec_name]
    df= df[df["year_vtg"]>=2020]
    if cost_type in ["fix_cost", "var_cost"]:
        df = df[df["year_vtg"]==df["year_act"]]

    df_nam = df[df["node_loc"]=="R12_NAM"].sort_values("year_vtg")
    df = df.merge(df_nam[["value", "year_vtg"]],left_on="year_vtg", right_on="year_vtg")
    df["ratio"] = df["value_x"]/df["value_y"]

    return df[["node_loc","year_vtg", "ratio"]]

def get_meth_bio_cost_ratio_2020(scenario, tec_name, cost_type):

    df = scenario.par(cost_type)
    df = df[df["technology"]==tec_name]
    df= df[df["year_vtg"]>=2020]
    if cost_type in ["fix_cost", "var_cost"]:
        df = df[df["year_vtg"]==df["year_act"]]

    val_nam_2020 = df.loc[(df["node_loc"]=="R12_NAM") & (df["year_vtg"]==2020), "value"].iloc[0]
    df["ratio"] = df["value"]/ val_nam_2020
    return df[["node_loc","year_vtg", "ratio"]]


def get_scaled_cost_from_proxy_tec(value, scenario, proxy_tec, cost_type, new_tec):
    df = get_meth_bio_cost_ratio_2020(scenario, proxy_tec, cost_type)
    df["value"] = value * df["ratio"]
    df["technology"] = new_tec
    df["unit"] = "-"
    return message_ix.make_df(cost_type, **df)