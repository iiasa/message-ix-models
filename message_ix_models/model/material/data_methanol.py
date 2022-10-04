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


def gen_data_methanol(scenario):
    dict1 = gen_data_meth_h2()
    dict2 = gen_data_meth_bio()
    keys = set(list(dict1.keys())+list(dict2.keys()))
    new_dict = {}
    for i in keys:
        if (i in dict2.keys()) & (i in dict1.keys()):
            new_dict[i] = pd.concat([dict1[i], dict2[i]])
        else:
            new_dict[i] = dict1[i]

    dict3 = pd.read_excel(context.get_local_path("material", "meth_bal_pars.xlsx"), sheet_name=None)

    keys = set(list(dict3.keys())+list(new_dict.keys()))
    new_dict2 = {}
    for i in keys:
        if (i in dict3.keys()) & (i in new_dict.keys()):
            new_dict2[i] = pd.concat([new_dict[i], dict3[i]])
        if (i in dict3.keys()) & ~(i in new_dict.keys()):
            new_dict2[i] = dict3[i]
        if ~(i in dict3.keys()) & (i in new_dict.keys()):
            new_dict2[i] = new_dict[i]
    # still contains ref_activity parameter! remove!
    df = pd.read_excel(context.get_local_path("material", "methanol demand.xlsx"), sheet_name="methanol_demand")
    df = df[(~df["Region"].isna()) & (df["Region"] != "World")]
    df = df.dropna(axis=1)
    df_melt = df.melt(id_vars=["Region"], value_vars=df.columns[5:], var_name="year")
    df_final = message_ix.make_df("demand", unit="t", level="final_material", value=df_melt.value, time="year",
                                  commodity="methanol", year=df_melt.year, node=("R12_"+df_melt["Region"]))
    df_final["value"] = df_final["value"].apply(lambda x: x * 0.5)
    new_dict2["demand"] = df_final

    # fix demand infeasibility
    act = scenario.par("historical_activity")
    row = act[act["technology"].str.startswith("meth")].sort_values("value", ascending=False).iloc[0]
    # china meth_coal production (90% coal share on 2015 47 Mt total; 1.348 = Mt to GWa )
    row["value"] = (47 / 1.3498) * 0.9
    new_dict2["historical_activity"] = pd.concat([new_dict2["historical_activity"], pd.DataFrame(row).T])
    # derived from graphic in "Methanol production statstics.xlsx/China demand split" diagram
    hist_cap = message_ix.make_df("historical_new_capacity", node_loc="R12_CHN", technology="meth_coal", year_vtg=2015, value=9.6,
                       unit="GW")
    new_dict2["historical_new_capacity"] = hist_cap
    # fix demand infeasibility
    #act = scenario.par("historical_activity")
    #row = act[act["technology"].str.startswith("meth")].sort_values("value", ascending=False).iloc[0]
    #row["value"] = 0.0
    new_dict2["historical_activity"] = pd.concat([new_dict2["historical_activity"], pd.DataFrame(row).T])
    df_ng = pd.read_excel(context.get_local_path("material", "meth_ng_techno_economic.xlsx"))
    new_dict2["historical_activity"] = new_dict2["historical_activity"].append(df_ng)

    mto_dict = gen_data_mto(scenario, "MTO")
    keys = set(list(new_dict2.keys()) + list(mto_dict.keys()))
    for i in keys:
        if (i in new_dict2.keys()) & (i in mto_dict.keys()):
            new_dict2[i] = pd.concat([new_dict2[i], mto_dict[i]])
        if ~(i in new_dict2.keys()) & (i in mto_dict.keys()):
            new_dict2[i] = mto_dict[i]

    ch2o_dict = gen_data_mto(scenario, "Formaldehyde")
    keys = set(list(new_dict2.keys()) + list(ch2o_dict.keys()))
    for i in keys:
        if (i in new_dict2.keys()) & (i in ch2o_dict.keys()):
            new_dict2[i] = pd.concat([new_dict2[i], ch2o_dict[i]])
        if ~(i in new_dict2.keys()) & (i in ch2o_dict.keys()):
            new_dict2[i] = ch2o_dict[i]

    resin_dict = gen_data_mto(scenario, "Resins")
    keys = set(list(new_dict2.keys()) + list(resin_dict.keys()))
    for i in keys:
        if (i in new_dict2.keys()) & (i in resin_dict.keys()):
            new_dict2[i] = pd.concat([new_dict2[i], resin_dict[i]])
        if ~(i in new_dict2.keys()) & (i in resin_dict.keys()):
            new_dict2[i] = resin_dict[i]

    new_dict2["demand"] = new_dict2["demand"].append(gen_resin_demand(scenario, 0.03, "residential"))
    new_dict2["demand"] = new_dict2["demand"].append(gen_resin_demand(scenario, 0.03, "comm"))
    new_dict2["input"].append(add_methanol_trp_additives(scenario))

    emission_dict = {
        "node": "World",
        "type_emission": "TCE_CO2",
        "type_tec": "all",
        "type_year": "cumulative",
        "unit": "???"
    }
    new_dict2["bound_emission"] = make_df("bound_emission", value=3667, **emission_dict)

    return new_dict2


def gen_data_meth_h2():
    context = read_config()
    df_h2 = pd.read_excel(context.get_local_path("material", "meth_h2_techno_economic.xlsx"), sheet_name=None)
    return df_h2


def gen_data_meth_bio():
    context = read_config()
    context.get_local_path("material", "meth_bio_techno_economic.xlsx")
    df_h2 = pd.read_excel(context.get_local_path("material", "meth_bio_techno_economic.xlsx"), sheet_name=None)
    return df_h2


def gen_data_mto(scenario, chemical):
    df = pd.read_excel(context.get_local_path("material", "MTO data collection.xlsx"),
                       sheet_name=chemical,
                       usecols=[1, 2, 3, 4, 6, 7])
    #exclude emissions for now
    if chemical == "MTO":
        df = df.iloc[:10, ]
    if chemical == "Formaldehyde":
        df = df.iloc[:9, ]

    common = dict(
        # commodity="NH3",
        # level="secondary_material",
        mode="M1",
        time="year",
        time_dest="year",
        time_origin="year",
        emission="CO2_industry",  # confirm if correct
        relation="CO2_cc"
    )

    all_years = scenario.vintage_and_active_years()
    all_years = all_years[all_years["year_vtg"] > 1990]

    nodes = scenario.set("node")[1:]
    nodes = nodes.drop(5).reset_index(drop=True)

    par_dict = {k: pd.DataFrame() for k in (df["parameter"])}
    for i in df["parameter"]:
        for index, row in df[df["parameter"] == i].iterrows():
            par_dict[i] = par_dict[i].append(
                make_df(i, **all_years.to_dict(orient="list"), **row, **common).pipe(broadcast,
                                                                                     node_loc=nodes).pipe(
                    same_node))

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
        "unit": "???"
    }
    if chemical == "MTO":
        par_dict["historical_activity"] = make_df("historical_activity", value=[4.5, 11], year_act=[2015, 2020], **hist_dict)
        #par_dict["historical_new_capacity"] = make_df("historical_new_capacity", value=[1.2, 1.2], year_vtg=[2015, 2020], **hist_dict)
        par_dict["historical_new_capacity"] = make_df("historical_new_capacity", value=1.2, year_vtg=2015,
                                                  **hist_dict)
    return par_dict


def add_methanol_trp_additives(scenario):
    df_loil = scenario.par("input")
    df_loil = df_loil[df_loil["technology"] == "loil_trp"]

    df_mtbe = pd.read_excel(
        context.get_local_path("material", "Methanol production statistics (version 1).xlsx"),
        # usecols=[1,2,3,4,6,7],
        skiprows=np.linspace(0, 65, 66), sheet_name="MTBE calc")
    df_mtbe = df_mtbe.iloc[1:13, ]
    df_mtbe["node_loc"] = "R12_" + df_mtbe["node_loc"]
    df_mtbe = df_mtbe[["node_loc", "methanol energy%"]]
    df_biodiesel = pd.read_excel(
        context.get_local_path("material", "Methanol production statistics (version 1).xlsx"),
        skiprows=np.linspace(0, 37, 38),
        usecols=[1, 2],
        sheet_name="Biodiesel")
    df_biodiesel["node_loc"] = "R12_" + df_biodiesel["node_loc"]
    df_total = df_biodiesel.merge(df_mtbe)
    df_total = df_total.assign(value=lambda x: x["methanol energy %"] + x["methanol energy%"])

    def get_meth_share(df, node):
        return df[df["node_loc"] == node["node_loc"]]["value"].values[0]

    df_loil_meth = df_loil.copy(deep=True)
    df_loil_meth["value"] = df_loil.apply(lambda x: get_meth_share(df_total, x), axis=1)
    df_loil_meth["commodity"] = "methanol"
    df_loil["value"] = df_loil["value"] - df_loil_meth["value"]

    return pd.concat([df_loil, df_loil_meth])


def gen_resin_demand(scenario, resin_share, sector):
    df = pd.read_csv(
        context.get_local_path("material", "results_material_SHAPE_"+sector+".csv"))
    resin_intensity = resin_share
    df = df[df["scenario"] == "SH2"]
    df = df[df["material"] == "wood"].assign(resin_demand=df["mat_demand_Mt"] * resin_intensity)
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
        level="final_material"
    )
    all_years = scenario.vintage_and_active_years()
    all_years = all_years[all_years["year_vtg"] > 1990]
    nodes = scenario.set("node")[1:]
    nodes = nodes.drop(5).reset_index(drop=True)

    df_demand = make_df("demand", year=all_years["year_act"].unique()[:-1], **common).pipe(broadcast,
                                                                                               node=nodes).merge(
        df[["R12", "year", "resin_demand"]], left_on=["node", "year"], right_on=["R12", "year"])
    df_demand["value"] = df_demand["resin_demand"]
    df_demand = make_df("demand", **df_demand)
    return df_demand