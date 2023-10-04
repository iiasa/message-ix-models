from .data_util import read_sector_data, read_timeseries

import numpy as np
from collections import defaultdict
import logging
from pathlib import Path

import pandas as pd

from .util import read_config
from .data_util import read_rel

# Get endogenous material demand from buildings interface
from .data_buildings import get_scen_mat_demand
from message_ix_models import ScenarioInfo
from message_ix import make_df
from message_ix_models.util import (
    broadcast,
    make_io,
    make_matched_dfs,
    same_node,
    copy_column,
    add_par_data,
    package_data_path,
)
from . import get_spec


# Generate a fake steel demand
def gen_mock_demand_steel(scenario):

    context = read_config()
    s_info = ScenarioInfo(scenario)
    modelyears = s_info.Y  # s_info.Y is only for modeling years
    fmy = s_info.y0
    nodes = s_info.N
    nodes.remove("World")

    # The order:
    # r = ['R12_AFR', 'R12_RCPA', 'R12_EEU', 'R12_FSU', 'R12_LAM', 'R12_MEA',\
    #'R12_NAM', 'R12_PAO', 'R12_PAS', 'R12_SAS', 'R12_WEU',"R12_CHN"]

    # Finished steel demand from: https://www.oecd.org/industry/ind/Item_4b_Worldsteel.pdf
    # For region definitions: https://worldsteel.org/wp-content/uploads/2021-World-Steel-in-Figures.pdf
    # For detailed assumptions and calculation see: steel_demand_calculation.xlsx
    # under https://iiasahub.sharepoint.com/sites/eceprog?cid=75ea8244-8757-44f1-83fd-d34f94ffd06a

    if "R12_CHN" in nodes:
        nodes.remove("R12_GLB")
        sheet_n = "data_R12"
        region_set = "R12_"
        d = [
            20.04,
            12.08,
            56.55,
            56.5,
            64.94,
            54.26,
            97.76,
            91.3,
            65.2,
            164.28,
            131.95,
            980.1,
        ]

    else:
        nodes.remove("R11_GLB")
        sheet_n = "data_R11"
        region_set = "R11_"
        d = [
            20.04,
            992.18,
            56.55,
            56.5,
            64.94,
            54.26,
            97.76,
            91.3,
            65.2,
            164.28,
            131.95,
        ]
        # MEA change from 39 to 9 to make it feasible (coal supply bound)

    # SSP2 R11 baseline GDP projection
    gdp_growth = pd.read_excel(
        package_data_path("material", "other", "iamc_db ENGAGE baseline GDP PPP.xlsx"),
        sheet_name=sheet_n,
    )

    gdp_growth = gdp_growth.loc[
        (gdp_growth["Scenario"] == "baseline") & (gdp_growth["Region"] != "World")
    ].drop(["Model", "Variable", "Unit", "Notes", 2000, 2005], axis=1)

    gdp_growth["Region"] = region_set + gdp_growth["Region"]

    demand2020_steel = (
        pd.DataFrame({"Region": nodes, "Val": d})
        .join(gdp_growth.set_index("Region"), on="Region")
        .rename(columns={"Region": "node"})
    )

    demand2020_steel.iloc[:, 3:] = (
        demand2020_steel.iloc[:, 3:]
        .div(demand2020_steel[2020], axis=0)
        .multiply(demand2020_steel["Val"], axis=0)
    )

    demand2020_steel = pd.melt(
        demand2020_steel.drop(["Val", "Scenario"], axis=1),
        id_vars=["node"],
        var_name="year",
        value_name="value",
    )

    return demand2020_steel


def gen_data_steel(scenario, dry_run=False):
    """Generate data for materials representation of steel industry."""
    # Load configuration
    context = read_config()
    config = context["material"]["steel"]

    # Information about scenario, e.g. node, year
    s_info = ScenarioInfo(scenario)

    # Techno-economic assumptions
    # TEMP: now add cement sector as well => Need to separate those since now I have get_data_steel and cement
    data_steel = read_sector_data(scenario, "steel")
    # Special treatment for time-dependent Parameters
    data_steel_ts = read_timeseries(scenario, "steel_cement", context.datafile)
    data_steel_rel = read_rel(scenario, "steel_cement", context.datafile)

    tec_ts = set(data_steel_ts.technology)  # set of tecs with var_cost

    # List of data frames, to be concatenated together at end
    results = defaultdict(list)

    # For each technology there are differnet input and output combinations
    # Iterate over technologies

    allyears = s_info.set["year"]  # s_info.Y is only for modeling years
    modelyears = s_info.Y  # s_info.Y is only for modeling years
    nodes = s_info.N
    yv_ya = s_info.yv_ya
    yv_ya = yv_ya.loc[yv_ya.year_vtg >= 1990]
    fmy = s_info.y0
    nodes.remove("World")

    # Do not parametrize GLB region the same way
    if "R11_GLB" in nodes:
        nodes.remove("R11_GLB")
        global_region = "R11_GLB"
    if "R12_GLB" in nodes:
        nodes.remove("R12_GLB")
        global_region = "R12_GLB"

    # for t in s_info.set['technology']:
    for t in config["technology"]["add"]:

        params = data_steel.loc[
            (data_steel["technology"] == t), "parameter"
        ].values.tolist()

        # Special treatment for time-varying params
        if t in tec_ts:
            common = dict(
                time="year",
                time_origin="year",
                time_dest="year",
            )

            param_name = data_steel_ts.loc[
                (data_steel_ts["technology"] == t), "parameter"
            ]

            for p in set(param_name):
                val = data_steel_ts.loc[
                    (data_steel_ts["technology"] == t)
                    & (data_steel_ts["parameter"] == p),
                    "value",
                ]
                units = data_steel_ts.loc[
                    (data_steel_ts["technology"] == t)
                    & (data_steel_ts["parameter"] == p),
                    "units",
                ].values[0]
                mod = data_steel_ts.loc[
                    (data_steel_ts["technology"] == t)
                    & (data_steel_ts["parameter"] == p),
                    "mode",
                ]
                yr = data_steel_ts.loc[
                    (data_steel_ts["technology"] == t)
                    & (data_steel_ts["parameter"] == p),
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
                if p == "output":
                    comm = data_steel_ts.loc[
                    (data_steel_ts["technology"] == t)
                    & (data_steel_ts["parameter"] == p),
                    "commodity",
                    ]

                    lev = data_steel_ts.loc[
                        (data_steel_ts["technology"] == t)
                        & (data_steel_ts["parameter"] == p),
                        "level",
                    ]

                    rg = data_steel_ts.loc[
                        (data_steel_ts["technology"] == t)
                        & (data_steel_ts["parameter"] == p),
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
                        node_dest=rg,
                        commodity = comm,
                        level = lev,
                        **common
                    )
                else:
                    rg = data_steel_ts.loc[
                        (data_steel_ts["technology"] == t)
                        & (data_steel_ts["parameter"] == p),
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

                results[p].append(df)

        # Iterate over parameters
        for par in params:
            # Obtain the parameter names, commodity,level,emission
            split = par.split("|")
            param_name = split[0]
            # Obtain the scalar value for the parameter
            val = data_steel.loc[
                ((data_steel["technology"] == t) & (data_steel["parameter"] == par)),
                "value",
            ]  # .values
            regions = data_steel.loc[
                ((data_steel["technology"] == t) & (data_steel["parameter"] == par)),
                "region",
            ]  # .values

            common = dict(
                year_vtg=yv_ya.year_vtg,
                year_act=yv_ya.year_act,
                # mode="M1",
                time="year",
                time_origin="year",
                time_dest="year",
            )

            for rg in regions:

                # For the parameters which inlcudes index names
                if len(split) > 1:
                    if (param_name == "input") | (param_name == "output"):

                        # Assign commodity and level names
                        com = split[1]
                        lev = split[2]
                        mod = split[3]
                        if (param_name == "input") and (lev == "import"):
                            df = make_df(
                                param_name,
                                technology=t,
                                commodity=com,
                                level=lev,
                                value=val[regions[regions == rg].index[0]],
                                mode=mod,
                                unit="t",
                                node_loc=rg,
                                node_origin=global_region,
                                **common
                            )

                        elif (param_name == "output") and (lev == "export"):
                            df = make_df(
                                param_name,
                                technology=t,
                                commodity=com,
                                level=lev,
                                value=val[regions[regions == rg].index[0]],
                                mode=mod,
                                unit="t",
                                node_loc=rg,
                                node_dest=global_region,
                                **common
                            )

                        else:
                            df = make_df(
                                param_name,
                                technology=t,
                                commodity=com,
                                level=lev,
                                value=val[regions[regions == rg].index[0]],
                                mode=mod,
                                unit="t",
                                node_loc=rg,
                                **common
                            ).pipe(same_node)

                        # Copy parameters to all regions, when node_loc is not GLB
                        if (len(regions) == 1) and (rg != global_region):
                            df["node_loc"] = None
                            df = df.pipe(broadcast, node_loc=nodes)
                            # Use same_node only for non-trade technologies
                            if (lev != "import") and (lev != "export"):
                                df = df.pipe(same_node)

                    elif param_name == "emission_factor":

                        # Assign the emisson type
                        emi = split[1]
                        mod = split[2]

                        df = make_df(
                            param_name,
                            technology=t,
                            value=val[regions[regions == rg].index[0]],
                            emission=emi,
                            mode=mod,
                            unit="t",
                            node_loc=rg,
                            **common
                        )

                    else:  # time-independent var_cost
                        mod = split[1]
                        df = make_df(
                            param_name,
                            technology=t,
                            value=val[regions[regions == rg].index[0]],
                            mode=mod,
                            unit="t",
                            node_loc=rg,
                            **common
                        )

                # Parameters with only parameter name
                else:
                    df = make_df(
                        param_name,
                        technology=t,
                        value=val[regions[regions == rg].index[0]],
                        unit="t",
                        node_loc=rg,
                        **common
                    )

                # Copy parameters to all regions
                if (
                    len(set(df["node_loc"])) == 1
                    and list(set(df["node_loc"]))[0] != global_region
                ):
                    df["node_loc"] = None
                    df = df.pipe(broadcast, node_loc=nodes)

                results[param_name].append(df)

    # Add relation for the maximum global scrap use in 2020

    df_max_recycling = pd.DataFrame({'relation': 'max_global_recycling_steel',
                       'node_rel': 'R12_GLB',
                       'year_rel': 2020,
                       'year_act': 2020,
                       'node_loc': nodes,
                       'technology': 'scrap_recovery_steel',
                       'mode': 'M1',
                       'unit': '???',
                       'value': data_steel_rel.loc[((data_steel_rel['relation'] \
                       == 'max_global_recycling_steel') & (data_steel_rel['parameter'] \
                       == 'relation_activity')), 'value'].values[0]})

    df_max_recycling_upper = pd.DataFrame({'relation': 'max_global_recycling_steel',
    'node_rel': 'R12_GLB',
    'year_rel': 2020,
    'unit': '???',
    'value': data_steel_rel.loc[((data_steel_rel['relation'] \
    == 'max_global_recycling_steel') & (data_steel_rel['parameter'] \
    == 'relation_upper')), 'value'].values[0]}, index = [0])
    df_max_recycling_lower = pd.DataFrame({'relation': 'max_global_recycling_steel',
    'node_rel': 'R12_GLB',
    'year_rel': 2020,
    'unit': '???',
    'value': data_steel_rel.loc[((data_steel_rel['relation'] \
    == 'max_global_recycling_steel') & (data_steel_rel['parameter'] \
    == 'relation_lower')), 'value'].values[0]}, index = [0])

    results['relation_activity'].append(df_max_recycling)
    results['relation_upper'].append(df_max_recycling_upper)
    results['relation_lower'].append(df_max_recycling_lower)

    # Add relations for scrap grades and availability
    regions = set(data_steel_rel["Region"].values)
    for reg in regions:
        for r in data_steel_rel["relation"]:
            model_years_rel = modelyears.copy()
            if r is None:
                break
            if r == 'max_global_recycling_steel':
                continue
            if r == 'minimum_recycling_steel':
                # Do not implement the minimum recycling rate for the year 2020
                model_years_rel.remove(2020)
            if r == 'max_regional_recycling_steel':
                # Do not implement the minimum recycling rate for the year 2020
                model_years_rel.remove(2020)

            params = set(
                data_steel_rel.loc[
                    (data_steel_rel["relation"] == r), "parameter"
                ].values
            )

            common_rel = dict(
                year_rel=model_years_rel,
                year_act=model_years_rel,
                mode="M1",
                relation=r,
            )

            for par_name in params:
                if par_name == "relation_activity":

                    tec_list = data_steel_rel.loc[
                        (
                            (data_steel_rel["relation"] == r)
                            & (data_steel_rel["parameter"] == par_name)
                        ),
                        "technology",
                    ]

                    for tec in tec_list.unique():
                        val = data_steel_rel.loc[
                            (
                                (data_steel_rel["relation"] == r)
                                & (data_steel_rel["parameter"] == par_name)
                                & (data_steel_rel["technology"] == tec)
                                & (data_steel_rel["Region"] == reg)
                            ),
                            "value",
                        ].values[0]

                        df = make_df(
                            par_name,
                            technology=tec,
                            value=val,
                            unit="-",
                            node_loc=reg,
                            node_rel=reg,
                            **common_rel
                        ).pipe(same_node)

                        results[par_name].append(df)

                elif (par_name == "relation_upper") | (par_name == "relation_lower"):
                    val = data_steel_rel.loc[
                        (
                            (data_steel_rel["relation"] == r)
                            & (data_steel_rel["parameter"] == par_name)
                            & (data_steel_rel["Region"] == reg)
                        ),
                        "value",
                    ].values[0]

                    df = make_df(
                        par_name, value=val, unit="-", node_rel=reg, **common_rel
                    )

                    results[par_name].append(df)

    # Create external demand param
    parname = "demand"
    demand = derive_steel_demand(scenario)

    df = make_df(
        parname,
        level="demand",
        commodity="steel",
        value=demand.value,
        unit="t",
        year=demand.year,
        time="year",
        node=demand.node,
    )  # .pipe(broadcast, node=nodes)
    results[parname].append(df)

    # Concatenate to one data frame per parameter
    results = {par_name: pd.concat(dfs) for par_name, dfs in results.items()}
    return results


# load rpy2 modules
import rpy2.robjects as ro
from rpy2.robjects import pandas2ri
from rpy2.robjects.conversion import localconverter


# This returns a df with columns ["region", "year", "demand.tot"]
def derive_steel_demand(scenario, dry_run=False):
    """Generate steel demand."""
    # paths to r code and lca data
    rcode_path = Path(__file__).parents[0] / "material_demand"
    context = read_config()

    # source R code
    r = ro.r
    r.source(str(rcode_path / "init_modularized.R"))
    package_data_path("material")
    # Read population and baseline demand for materials
    pop = scenario.par("bound_activity_up", {"technology": "Population"})
    pop = pop.loc[pop.year_act >= 2020].rename(
        columns={"year_act": "year", "value": "pop.mil", "node_loc": "region"}
    )

    # import pdb; pdb.set_trace()

    pop = pop[["region", "year", "pop.mil"]]

    base_demand = gen_mock_demand_steel(scenario)
    base_demand = base_demand.loc[base_demand.year == 2020].rename(
        columns={"value": "demand.tot.base", "node": "region"}
    )

    # call R function with type conversion
    with localconverter(ro.default_converter + pandas2ri.converter):
        # GDP is only in MER in scenario.
        # To get PPP GDP, it is read externally from the R side
        df = r.derive_steel_demand(
            pop, base_demand, str(package_data_path("material"))
        )
        df.year = df.year.astype(int)

    return df
