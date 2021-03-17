from .data_util import read_sector_data, read_timeseries

import numpy as np
from collections import defaultdict
import logging

import pandas as pd

from .util import read_config
from message_data.tools import (
    ScenarioInfo,
    broadcast,
    make_df,
    make_io,
    make_matched_dfs,
    same_node,
    copy_column,
    add_par_data
)
from . import get_spec


def gen_data_power_sector(scenario, dry_run=False):
    """Generate data for materials representation of power industry.

    """
    # Load configuration
    context = read_config()
    config = context["material"]["power_sector"]

    # Information about scenario, e.g. node, year
    s_info = ScenarioInfo(scenario)

    modelyears = s_info.Y #s_info.Y is only for modeling years
    nodes = s_info.N
    tecs = s_info.set['technology']
    lvls = s_info.set['level']
    comms = s_info.set['commodity']

    params = ['input_cap_new', 'input_cap_ret', 'output_cap_ret']

    # List of data frames, to be concatenated together at end
    results = defaultdict(list)

    # Create external demand param
    for p in params:
        df = import_intensity_r(p, s_info)
        results[p].append(df)

    # Concatenate to one data frame per parameter
    results = {par_name: pd.concat(dfs) for par_name, dfs in results.items()}

    print(results)
    # return results


# Dummy definition - LINK THIS
def import_intensity_r(param_name, info):
    print("Get {} from R...".format(param_name))

    common = dict(
        node=info.N[0],
        node_loc=info.N[0],
        node_origin=info.N[0],
        node_dest=info.N[0],
        technology="dummy",
        year=info.Y,
        year_vtg=info.Y,
        year_act=info.Y,
        mode="all",
        # No subannual detail
        time="year",
        time_origin="year",
        time_dest="year",
    )

    # Dummy DF
    data = make_df(param_name,
        **common)

    return data
