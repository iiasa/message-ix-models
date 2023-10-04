from .data_util import read_sector_data, read_timeseries

import numpy as np
from collections import defaultdict
import logging
from pathlib import Path

import pandas as pd

from .util import read_config
from message_ix_models import ScenarioInfo
from message_ix import make_df
from message_ix_models.util import (
    broadcast,
    make_io,
    make_matched_dfs,
    same_node,
    add_par_data,
    package_data_path,
)

# Get endogenous material demand from buildings interface
from .data_buildings import get_scen_mat_demand
from . import get_spec


# gdp_growth = [0.121448215899944, 0.0733079014579874, 0.0348154093342843, \
#     0.021827616787921, 0.0134425983942219, 0.0108320197485592, \
#     0.00884341208063,0.00829374133206562, 0.00649794573935969, 0.00649794573935969]
# gr = np.cumprod([(x+1) for x in gdp_growth])


# Generate a fake cement demand
def gen_mock_demand_cement(scenario):

    context = read_config()
    s_info = ScenarioInfo(scenario)
    modelyears = s_info.Y  # s_info.Y is only for modeling years
    fmy = s_info.y0
    nodes = s_info.N
    nodes.remove("World")

    # 2019 production by country (USGS)
    # p43 of https://pubs.usgs.gov/periodicals/mcs2020/mcs2020-cement.pdf

    # For R12: China and CPA demand divided by 0.1 and 0.9.

    # The order:
    # r = ['R12_AFR', 'R12_RCPA', 'R12_EEU', 'R12_FSU', 'R12_LAM', 'R12_MEA',\
    #'R12_NAM', 'R12_PAO', 'R12_PAS', 'R12_SAS', 'R12_WEU',"R12_CHN"]

    if "R12_CHN" in nodes:
        nodes.remove("R12_GLB")
        sheet_n = "data_R12"
        region_set = "R12_"

        demand2020_top = [76, 229.5, 0, 57, 55, 60, 89, 54, 129, 320, 51, 2065.5]
        # the rest (~900 Mt) allocated by % values in http://www.cembureau.eu/media/clkdda45/activity-report-2019.pdf
        demand2020_rest = [
            4100 * 0.051 - 76,
            (4100 * 0.14 - 155) * 0.2 * 0.1,
            4100 * 0.064 * 0.5,
            4100 * 0.026 - 57,
            4100 * 0.046 * 0.5 - 55,
            (4100 * 0.14 - 155) * 0.2,
            4100 * 0.046 * 0.5,
            12,
            4100 * 0.003,
            (4100 * 0.14 - 155) * 0.6,
            4100 * 0.064 * 0.5 - 51,
            (4100 * 0.14 - 155) * 0.2 * 0.9,
        ]
    else:
        nodes.remove("R11_GLB")
        sheet_n = "data_R11"
        region_set = "R11_"

        demand2020_top = [76, 2295, 0, 57, 55, 60, 89, 54, 129, 320, 51]
        # the rest (~900 Mt) allocated by % values in http://www.cembureau.eu/media/clkdda45/activity-report-2019.pdf
        demand2020_rest = [
            4100 * 0.051 - 76,
            (4100 * 0.14 - 155) * 0.2,
            4100 * 0.064 * 0.5,
            4100 * 0.026 - 57,
            4100 * 0.046 * 0.5 - 55,
            (4100 * 0.14 - 155) * 0.2,
            4100 * 0.046 * 0.5,
            12,
            4100 * 0.003,
            (4100 * 0.14 - 155) * 0.6,
            4100 * 0.064 * 0.5 - 51,
        ]

    # SSP2 R11 baseline GDP projection
    gdp_growth = pd.read_excel(
        package_data_path("material", "other", "iamc_db ENGAGE baseline GDP PPP.xlsx"),
        sheet_name=sheet_n,
    )

    gdp_growth = gdp_growth.loc[
        (gdp_growth["Scenario"] == "baseline") & (gdp_growth["Region"] != "World")
    ].drop(["Model", "Variable", "Unit", "Notes", 2000, 2005], axis=1)

    d = [a + b for a, b in zip(demand2020_top, demand2020_rest)]
    gdp_growth["Region"] = region_set + gdp_growth["Region"]

    # # Regions setting for IMAGE
    # region_cement = pd.read_excel(
    #     package_data_path("material",  "CEMENT.BvR2010.xlsx"),
    #     sheet_name="Timer_Regions", skiprows=range(0,3))[['Region #', 'Name']]\
    #     .drop_duplicates().sort_values(by='Region #')
    #
    # region_cement = region_cement.loc[region_cement['Region #'] < 999]
    # region_cement['node'] = \
    #     ['R11_NAM', 'R11_NAM',
    #      'R11_LAM', 'R11_LAM',
    #      'R11_LAM', 'R11_LAM',
    #      'R11_AFR', 'R11_AFR',
    #      'R11_AFR', 'R11_AFR',
    #      'R11_WEU', 'R11_EEU',
    #      'R11_EEU', 'R11_FSU',
    #      'R11_FSU', 'R11_FSU',
    #      'R11_MEA', 'R11_SAS',
    #      'R11_PAS', 'R11_CPA',
    #      'R11_PAS', 'R11_PAS',
    #      'R11_PAO', 'R11_PAO',
    #      'R11_SAS', 'R11_AFR']
    #
    # # Cement demand 2010 [Mt/year] (IMAGE)
    # demand2010_cement = pd.read_excel(
    #     package_data_path("material",  "CEMENT.BvR2010.xlsx"),
    #     sheet_name="Domestic Consumption", skiprows=range(0,3)).\
    #     groupby(by=["Region #"]).sum()[[2010]].\
    #     join(region_cement.set_index('Region #'), on='Region #').\
    #     rename(columns={2010:'value'})
    #
    # demand2010_cement = demand2010_cement.groupby(by=['node']).sum().reset_index()
    # demand2010_cement['value'] = demand2010_cement['value'] / 1e9 # kg to Mt

    # Directly assigned countries from the table on p43

    demand2020_cement = (
        pd.DataFrame({"Region": nodes, "value": d})
        .join(gdp_growth.set_index("Region"), on="Region")
        .rename(columns={"Region": "node"})
    )

    # demand2010_cement = demand2010_cement.\
    #     join(gdp_growth.rename(columns={'Region':'node'}).set_index('node'), on='node')

    demand2020_cement.iloc[:, 3:] = (
        demand2020_cement.iloc[:, 3:]
        .div(demand2020_cement[2020], axis=0)
        .multiply(demand2020_cement["value"], axis=0)
    )

    # Do this if we have 2020 demand values for buildings
    # sp = get_spec()
    # if 'buildings' in sp['add'].set['technology']:
    #     val = get_scen_mat_demand("cement",scenario) # Mt in 2020
    #     print("Base year demand of {}:".format("cement"), val)
    #     # demand2020_cement['value'] = demand2020_cement['value'] - val['value']
    #     # Scale down all years' demand values by the 2020 ratio
    #     demand2020_cement.iloc[:,3:] =  demand2020_cement.iloc[:,3:].\
    #         multiply(demand2020_cement[2020]- val['value'], axis=0).\
    #         div(demand2020_cement[2020], axis=0)
    #     print("UPDATE {} demand for 2020!".format("cement"))
    #
    demand2020_cement = pd.melt(
        demand2020_cement.drop(["value", "Scenario"], axis=1),
        id_vars=["node"],
        var_name="year",
        value_name="value",
    )

    return demand2020_cement


def gen_data_cement(scenario, dry_run=False):
    """Generate data for materials representation of cement industry."""
    # Load configuration
    context = read_config()
    config = read_config()["material"]["cement"]

    # Information about scenario, e.g. node, year
    s_info = ScenarioInfo(scenario)

    # Techno-economic assumptions
    # TEMP: now add cement sector as well
    data_cement = read_sector_data(scenario, "cement")
    # Special treatment for time-dependent Parameters
    data_cement_ts = read_timeseries(scenario, "steel_cement", context.datafile)
    tec_ts = set(data_cement_ts.technology)  # set of tecs with var_cost

    # List of data frames, to be concatenated together at end
    results = defaultdict(list)

    # For each technology there are differnet input and output combinations
    # Iterate over technologies

    allyears = s_info.set["year"]  # s_info.Y is only for modeling years
    modelyears = s_info.Y  # s_info.Y is only for modeling years
    nodes = s_info.N
    yv_ya = s_info.yv_ya
    yv_ya = yv_ya.loc[yv_ya.year_vtg >= 1980]
    fmy = s_info.y0
    nodes.remove("World")

    # Do not parametrize GLB region the same way
    if "R11_GLB" in nodes:
        nodes.remove("R11_GLB")
    if "R12_GLB" in nodes:
        nodes.remove("R12_GLB")

    # for t in s_info.set['technology']:
    for t in config["technology"]["add"]:

        params = data_cement.loc[
            (data_cement["technology"] == t), "parameter"
        ].values.tolist()

        # Special treatment for time-varying params
        if t in tec_ts:
            common = dict(
                time="year",
                time_origin="year",
                time_dest="year",
            )

            param_name = data_cement_ts.loc[
                (data_cement_ts["technology"] == t), "parameter"
            ]

            for p in set(param_name):
                val = data_cement_ts.loc[
                    (data_cement_ts["technology"] == t)
                    & (data_cement_ts["parameter"] == p),
                    "value",
                ]
                units = data_cement_ts.loc[
                    (data_cement_ts["technology"] == t)
                    & (data_cement_ts["parameter"] == p),
                    "units",
                ].values[0]
                mod = data_cement_ts.loc[
                    (data_cement_ts["technology"] == t)
                    & (data_cement_ts["parameter"] == p),
                    "mode",
                ]
                yr = data_cement_ts.loc[
                    (data_cement_ts["technology"] == t)
                    & (data_cement_ts["parameter"] == p),
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
                    rg = data_cement_ts.loc[
                        (data_cement_ts["technology"] == t)
                        & (data_cement_ts["parameter"] == p),
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
            val = data_cement.loc[
                ((data_cement["technology"] == t) & (data_cement["parameter"] == par)),
                "value",
            ]  # .values
            regions = data_cement.loc[
                ((data_cement["technology"] == t) & (data_cement["parameter"] == par)),
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
                        )  # .pipe(broadcast, \
                        # node_loc=nodes))

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
                        )  # .pipe(broadcast, node_loc=nodes))

                # Parameters with only parameter name
                else:
                    df = make_df(
                        param_name,
                        technology=t,
                        value=val[regions[regions == rg].index[0]],
                        unit="t",
                        node_loc=rg,
                        **common
                    )  # .pipe(broadcast, node_loc=nodes))

                if len(regions) == 1:
                    df["node_loc"] = None
                    df = df.pipe(broadcast, node_loc=nodes).pipe(same_node)

                results[param_name].append(df)

    # Create external demand param
    parname = "demand"
    # demand = gen_mock_demand_cement(scenario)
    demand = derive_cement_demand(scenario)
    df = make_df(
        parname,
        level="demand",
        commodity="cement",
        value=demand.value,
        unit="t",
        year=demand.year,
        time="year",
        node=demand.node,
    )
    results[parname].append(df)

    # Add CCS as addon
    parname = "addon_conversion"
    ccs_tec = ["clinker_wet_cement", "clinker_dry_cement"]
    df = make_df(
        parname, mode="M1", type_addon="ccs_cement", value=1, unit="-", **common
    ).pipe(broadcast, node=nodes, technology=ccs_tec)
    results[parname].append(df)

    # Test emission bound
    # parname = 'bound_emission'
    # df = (make_df(parname, type_tec='all', type_year='cumulative', \
    #     type_emission='CO2_industry', \
    #     value=200, unit='-').pipe(broadcast, node=nodes))
    # results[parname].append(df)

    # Concatenate to one data frame per parameter
    results = {par_name: pd.concat(dfs) for par_name, dfs in results.items()}

    return results


# load rpy2 modules
import rpy2.robjects as ro
from rpy2.robjects import pandas2ri
from rpy2.robjects.conversion import localconverter


# This returns a df with columns ["region", "year", "demand.tot"]
def derive_cement_demand(scenario, dry_run=False):
    """Generate cement demand."""
    # paths to r code and lca data
    rcode_path = Path(__file__).parents[0] / "material_demand"
    context = read_config()

    # source R code
    r = ro.r
    r.source(str(rcode_path / "init_modularized.R"))

    # Read population and baseline demand for materials
    pop = scenario.par("bound_activity_up", {"technology": "Population"})
    pop = pop.loc[pop.year_act >= 2020].rename(
        columns={"year_act": "year", "value": "pop.mil", "node_loc": "region"}
    )
    pop = pop[["region", "year", "pop.mil"]]

    base_demand = gen_mock_demand_cement(scenario)
    base_demand = base_demand.loc[base_demand.year == 2020].rename(
        columns={"value": "demand.tot.base", "node": "region"}
    )

    # import pdb; pdb.set_trace()

    # base_demand = scenario.par("demand", {"commodity": "steel", "year": 2020})
    # base_demand = base_demand.loc[base_demand.year >= 2020].rename(
    #     columns={"value": "demand.tot.base", "node": "region"}
    # )
    # base_demand = base_demand[["region", "year", "demand.tot.base"]]

    # call R function with type conversion
    with localconverter(ro.default_converter + pandas2ri.converter):
        # GDP is only in MER in scenario.
        # To get PPP GDP, it is read externally from the R side
        df = r.derive_cement_demand(
            pop, base_demand, str(package_data_path("material"))
        )
        df.year = df.year.astype(int)

    return df
