from collections import defaultdict
import logging

import pandas as pd

from .util import read_config
from message_ix_models import ScenarioInfo
from message_ix import make_df
from .data_util import read_timeseries
from message_ix_models.util import (
    broadcast,
    make_io,
    make_matched_dfs,
    same_node,
    add_par_data,
    package_data_path,
)


def read_data_generic(scenario):
    """Read and clean data from :file:`generic_furnace_boiler_techno_economic.xlsx`."""

    # Ensure config is loaded, get the context
    context = read_config()
    s_info = ScenarioInfo(scenario)

    # Shorter access to sets configuration
    # sets = context["material"]["generic"]

    # Read the file
    data_generic = pd.read_excel(package_data_path(
            "material", "other", "generic_furnace_boiler_techno_economic.xlsx"
        ), sheet_name="generic",)

    # Clean the data
    # Drop columns that don't contain useful information

    data_generic = data_generic.drop(["Region", "Source", "Description"], axis=1)
    data_generic_ts = read_timeseries(scenario, "other", "generic_furnace_boiler_techno_economic.xlsx")

    # Unit conversion

    # At the moment this is done in the excel file, can be also done here
    # To make sure we use the same units

    return data_generic, data_generic_ts


def gen_data_generic(scenario, dry_run=False):
    # Load configuration

    config = read_config()["material"]["generic"]

    # Information about scenario, e.g. node, year
    s_info = ScenarioInfo(scenario)

    # Techno-economic assumptions
    data_generic, data_generic_ts = read_data_generic(scenario)

    # List of data frames, to be concatenated together at end
    results = defaultdict(list)

    # For each technology there are differnet input and output combinations
    # Iterate over technologies

    allyears = s_info.set["year"]  # s_info.Y is only for modeling years
    modelyears = s_info.Y  # s_info.Y is only for modeling years
    nodes = s_info.N
    yv_ya = s_info.yv_ya
    fmy = s_info.y0


    # Do not parametrize GLB region the same way
    if "R11_GLB" in nodes:
        nodes.remove("R11_GLB")
        global_region = 'R11_GLB'
    if "R12_GLB" in nodes:
        nodes.remove("R12_GLB")
        global_region = "R12_GLB"

    # 'World' is included by default when creating a message_ix.Scenario().
    # Need to remove it for the China bare model
    nodes.remove("World")

    for t in config["technology"]["add"]:

        # years = s_info.Y
        params = data_generic.loc[
            (data_generic["technology"] == t), "parameter"
        ].values.tolist()

        # Availability year of the technology
        av = data_generic.loc[(data_generic["technology"] == t), "availability"].values[
            0
        ]
        modelyears = [year for year in modelyears if year >= av]
        yva = yv_ya.loc[
            yv_ya.year_vtg >= av,
        ]

        # Iterate over parameters
        for par in params:
            split = par.split("|")
            param_name = split[0]

            val = data_generic.loc[
                (
                    (data_generic["technology"] == t)
                    & (data_generic["parameter"] == par)
                ),
                "value",
            ].values[0]

            # Common parameters for all input and output tables
            # year_act is none at the moment
            # node_dest and node_origin are the same as node_loc

            common = dict(
                year_vtg=yva.year_vtg,
                year_act=yva.year_act,
                time="year",
                time_origin="year",
                time_dest="year",
            )

            if len(split) > 1:

                if (param_name == "input") | (param_name == "output"):

                    com = split[1]
                    lev = split[2]
                    mod = split[3]

                    df = (
                        make_df(
                            param_name,
                            technology=t,
                            commodity=com,
                            level=lev,
                            mode=mod,
                            value=val,
                            unit="t",
                            **common
                        )
                        .pipe(broadcast, node_loc=nodes)
                        .pipe(same_node)
                    )

                    results[param_name].append(df)

                elif param_name == "emission_factor":
                    emi = split[1]

                    # TODO: Now tentatively fixed to one mode. Have values for the other mode too
                    df_low = make_df(
                        param_name,
                        technology=t,
                        value=val,
                        emission=emi,
                        mode="low_temp",
                        unit="t",
                        **common
                    ).pipe(broadcast, node_loc=nodes)

                    df_high = make_df(
                        param_name,
                        technology=t,
                        value=val,
                        emission=emi,
                        mode="high_temp",
                        unit="t",
                        **common
                    ).pipe(broadcast, node_loc=nodes)

                    results[param_name].append(df_low)
                    results[param_name].append(df_high)

            # Rest of the parameters apart from input, output and emission_factor

            else:

                df = make_df(
                    param_name, technology=t, value=val, unit="t", **common
                ).pipe(broadcast, node_loc=nodes)

                results[param_name].append(df)

    # Special treatment for time-varying params

    tec_ts = set(data_generic_ts.technology)  # set of tecs in timeseries sheet

    for t in tec_ts:
        common = dict(
            time="year",
            time_origin="year",
            time_dest="year",
        )

        param_name = data_generic_ts.loc[
            (data_generic_ts["technology"] == t), "parameter"
        ]

        for p in set(param_name):
            val = data_generic_ts.loc[
                (data_generic_ts["technology"] == t)
                & (data_generic_ts["parameter"] == p),
                "value",
            ]
            regions = data_generic_ts.loc[
                (
                    (data_generic_ts["technology"] == t)
                    & (data_generic_ts["parameter"] == p)
                ),
                "region",
            ]
            units = data_generic_ts.loc[
                (data_generic_ts["technology"] == t)
                & (data_generic_ts["parameter"] == p),
                "units",
            ].values[0]
            mod = data_generic_ts.loc[
                (data_generic_ts["technology"] == t)
                & (data_generic_ts["parameter"] == p),
                "mode",
            ]
            yr = data_generic_ts.loc[
                (data_generic_ts["technology"] == t)
                & (data_generic_ts["parameter"] == p),
                "year",
            ]

            if p == "var_cost":
                df = make_df(
                    p,
                    technology=t,
                    value=val,
                    unit="t",
                    year_vtg=yr,
                    year_act=yr,
                    mode=mod,
                    **common
                ).pipe(broadcast, node_loc=nodes)
            else:
                rg = data_generic_ts.loc[
                    (data_generic_ts["technology"] == t)
                    & (data_generic_ts["parameter"] == p),
                    "region",
                ]
                df = make_df(
                    p,
                    technology=t,
                    value=val,
                    unit="t",
                    year_vtg=yr,
                    year_act=yr,
                    mode=mod,
                    node_loc=rg,
                    **common
                )

            # Copy parameters to all regions
            if (
                (len(set(regions)) == 1)
                and len(set(df["node_loc"])) == 1
                and list(set(df["node_loc"]))[0] != global_region
            ):
                df["node_loc"] = None
                df = df.pipe(broadcast, node_loc=nodes)

            results[p].append(df)


    results = {par_name: pd.concat(dfs) for par_name, dfs in results.items()}

    return results
