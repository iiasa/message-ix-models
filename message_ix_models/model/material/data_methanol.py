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
    return {**gen_data_meth_h2(scenario), **gen_data_meth_bio(scenario)}


def gen_data_meth_h2(scenario):
    context = read_config()
    df_h2 = pd.read_excel(context.get_local_path("material", "meth_h2_techno_economic.xlsx"), sheet_name=None)
    return df_h2


def gen_data_meth_bio(scenario):
    context = read_config()
    context.get_local_path("material", "meth_bio_techno_economic.xlsx")
    df_h2 = pd.read_excel(context.get_local_path("material", "meth_bio_techno_economic.xlsx"), sheet_name=None)
    return df_h2
