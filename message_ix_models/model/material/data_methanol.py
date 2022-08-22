import message_ix
import message_data
import ixmp as ix

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from message_ix_models import ScenarioInfo
from message_ix import make_df
from message_ix_models.util import broadcast, same_node
from util import read_config


def gen_data_methanol(scenario):
    dict1 = gen_data_meth_h2()
    dict2 = gen_data_meth_bio()
    keys = set(list(dict1.keys())+list(dict2.keys()))
    new_dict = {}
    for i in keys:
        if (i in dict2.keys()) & (i in dict1.keys()):
            new_dict[i] = dict1[i].append(dict2[i])
        else:
            new_dict[i] = dict1[i]

    context = read_config()
    dict3 = pd.read_excel(context.get_local_path("material", "meth_bal_pars.xlsx"), sheet_name=None)

    keys = set(list(dict3.keys())+list(new_dict.keys()))
    new_dict2 = {}
    for i in keys:
        if (i in dict3.keys()) & (i in new_dict.keys()):
            new_dict2[i] = new_dict[i].append(dict3[i])
        if (i in dict3.keys()) & ~(i in new_dict.keys()):
            new_dict2[i] = dict3[i]
        if ~(i in dict3.keys()) & (i in new_dict.keys()):
            new_dict2[i] = new_dict[i]
    # still contains ref_activity parameter! remove!
    df = pd.read_excel(context.get_local_path("material", "methanol demand.xlsx"), sheet_name="methanol_demand")
    df = df[(~df["Region"].isna()) & (df["Region"] != "World")]
    df = df.dropna(axis=1)
    df_melt = df.melt(id_vars=["Region"], value_vars=df.columns[5:], var_name="year")
    df_final = message_ix.make_df("demand", unit="t", level="demand", value=df_melt.value, time="year",
                                  commodity="methanol", year=df_melt.year, node=df_melt["Region"])
    df_final["value"] = df_final["value"].apply(lambda x: x * 0.75)
    new_dict2["demand"] = df_final
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
