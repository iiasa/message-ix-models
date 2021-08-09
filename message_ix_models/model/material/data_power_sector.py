from .data_util import read_sector_data, read_timeseries

import numpy as np
from collections import defaultdict
import logging
from pathlib import Path

# check Python and R environments (for debugging)
import rpy2.situation

for row in rpy2.situation.iter_info():
    print(row)

# load rpy2 modules
import rpy2.robjects as ro

# from rpy2.robjects.packages import importr
from rpy2.robjects import pandas2ri
from rpy2.robjects.conversion import localconverter

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
    add_par_data,
)
from . import get_spec


def gen_data_power_sector(scenario, dry_run=False):
    """Generate data for materials representation of power industry."""
    # Load configuration
    context = read_config()
    config = context["material"]["power_sector"]

    # paths to r code and lca data
    rcode_path = Path(__file__).parents[0] / "material_intensity"
    data_path = context.get_path("material")

    # Information about scenario, e.g. node, year
    s_info = ScenarioInfo(scenario)

    # read node, technology, commodity and level from existing scenario
    node = s_info.N
    year = s_info.set["year"]  # s_info.Y is only for modeling years
    technology = s_info.set["technology"]
    commodity = s_info.set["commodity"]
    level = s_info.set["level"]

    # read inv.cost data
    inv_cost = scenario.par("inv_cost")

    # source R code
    r = ro.r
    r.source(str(rcode_path / "ADVANCE_lca_coefficients_embedded.R"))

    param_name = ["input_cap_new", "input_cap_ret", "output_cap_ret"]

    # List of data frames, to be concatenated together at end
    results = defaultdict(list)

    # call R function with type conversion
    for p in set(param_name):
        with localconverter(ro.default_converter + pandas2ri.converter):
            df = r.read_material_intensities(
                p, str(data_path), node, year, technology, commodity, level, inv_cost
            )
            print("type df:", type(df))
            print(df.head())

        results[p].append(df)

    # import pdb; pdb.set_trace()

    # create new parameters input_cap_new, output_cap_new, input_cap_ret, output_cap_ret, input_cap and output_cap if they don't exist
    if not scenario.has_par("input_cap_new"):
        scenario.init_par(
            "input_cap_new",
            idx_sets=[
                "node",
                "technology",
                "year",
                "node",
                "commodity",
                "level",
                "time",
            ],
            idx_names=[
                "node_loc",
                "technology",
                "year_vtg",
                "node_origin",
                "commodity",
                "level",
                "time_origin",
            ],
        )
    if not scenario.has_par("output_cap_new"):
        scenario.init_par(
            "output_cap_new",
            idx_sets=[
                "node",
                "technology",
                "year",
                "node",
                "commodity",
                "level",
                "time",
            ],
            idx_names=[
                "node_loc",
                "technology",
                "year_vtg",
                "node_dest",
                "commodity",
                "level",
                "time_dest",
            ],
        )
    if not scenario.has_par("input_cap_ret"):
        scenario.init_par(
            "input_cap_ret",
            idx_sets=[
                "node",
                "technology",
                "year",
                "node",
                "commodity",
                "level",
                "time",
            ],
            idx_names=[
                "node_loc",
                "technology",
                "year_vtg",
                "node_origin",
                "commodity",
                "level",
                "time_origin",
            ],
        )
    if not scenario.has_par("output_cap_ret"):
        scenario.init_par(
            "output_cap_ret",
            idx_sets=[
                "node",
                "technology",
                "year",
                "node",
                "commodity",
                "level",
                "time",
            ],
            idx_names=[
                "node_loc",
                "technology",
                "year_vtg",
                "node_dest",
                "commodity",
                "level",
                "time_dest",
            ],
        )
    if not scenario.has_par("input_cap"):
        scenario.init_par(
            "input_cap",
            idx_sets=[
                "node",
                "technology",
                "year",
                "year",
                "node",
                "commodity",
                "level",
                "time",
            ],
            idx_names=[
                "node_loc",
                "technology",
                "year_vtg",
                "year_act",
                "node_origin",
                "commodity",
                "level",
                "time_origin",
            ],
        )
    if not scenario.has_par("output_cap"):
        scenario.init_par(
            "output_cap",
            idx_sets=[
                "node",
                "technology",
                "year",
                "year",
                "node",
                "commodity",
                "level",
                "time",
            ],
            idx_names=[
                "node_loc",
                "technology",
                "year_vtg",
                "year_act",
                "node_dest",
                "commodity",
                "level",
                "time_dest",
            ],
        )

    # Concatenate to one data frame per parameter
    results = {par_name: pd.concat(dfs) for par_name, dfs in results.items()}

    print(results)
    return results
