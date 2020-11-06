from .data_util import process_china_data_tec, read_timeseries

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
    add_par_data
)


gdp_growth = [0.121448215899944, 0.0733079014579874, 0.0348154093342843, \
    0.021827616787921, 0.0134425983942219, 0.0108320197485592, \
    0.00884341208063,0.00829374133206562, 0.00649794573935969, 0.00649794573935969]
gr = np.cumprod([(x+1) for x in gdp_growth])


# Generate a fake cement demand
def gen_mock_demand_cement(s_info):

    modelyears = s_info.Y
    fmy = s_info.y0

    # True cement use 2011 (China) = 2100 Mt/year (ADVANCE)
    demand2010_cement = 2100

    baseyear = list(range(2020, 2110+1, 10))

    demand = gr * demand2010_cement
    demand_interp = np.interp(modelyears, baseyear, demand)

    return demand_interp.tolist()


def gen_data_cement(scenario, dry_run=False):
    """Generate data for materials representation of steel industry.

    """
    # Load configuration
    config = read_config()["material"]["cement"]

    # Information about scenario, e.g. node, year
    s_info = ScenarioInfo(scenario)

    # Techno-economic assumptions
    # TEMP: now add cement sector as well
    data_cement = process_china_data_tec("cement")
    # Special treatment for time-dependent Parameters
    # data_cement_vc = read_timeseries()
    # tec_vc = set(data_cement_vc.technology) # set of tecs with var_cost

    # List of data frames, to be concatenated together at end
    results = defaultdict(list)

    # For each technology there are differnet input and output combinations
    # Iterate over technologies

    allyears = s_info.set['year'] #s_info.Y is only for modeling years
    modelyears = s_info.Y #s_info.Y is only for modeling years
    nodes = s_info.N
    yv_ya = s_info.yv_ya
    fmy = s_info.y0

    #print(allyears, modelyears, fmy)

    nodes.remove('World') # For the bare model

    # for t in s_info.set['technology']:
    for t in config['technology']['add']:

        params = data_cement.loc[(data_cement["technology"] == t),\
            "parameter"].values.tolist()

        # Iterate over parameters
        for par in params:

            # Obtain the parameter names, commodity,level,emission
            split = par.split("|")
            param_name = split[0]
            # Obtain the scalar value for the parameter
            val = data_cement.loc[((data_cement["technology"] == t) \
            & (data_cement["parameter"] == par)),'value'].values[0]

            common = dict(
                year_vtg= yv_ya.year_vtg,
                year_act= yv_ya.year_act,
                # mode="M1",
                time="year",
                time_origin="year",
                time_dest="year",)

            # For the parameters which inlcudes index names
            if len(split)> 1:

                #print('1.param_name:', param_name, t)
                if (param_name == "input")|(param_name == "output"):

                    # Assign commodity and level names
                    com = split[1]
                    lev = split[2]
                    mod = split[3]

                    df = (make_df(param_name, technology=t, commodity=com, \
                    level=lev, value=val, mode=mod, unit='t', **common)\
                    .pipe(broadcast, node_loc=nodes).pipe(same_node))

                elif param_name == "emission_factor":

                    # Assign the emisson type
                    emi = split[1]
                    mod = split[2]

                    df = (make_df(param_name, technology=t, value=val,\
                    emission=emi, mode=mod, unit='t', **common).pipe(broadcast, \
                    node_loc=nodes))

                else: # time-independent var_cost
                    mod = split[1]
                    df = (make_df(param_name, technology=t, value=val, \
                    mode=mod, unit='t', \
                    **common).pipe(broadcast, node_loc=nodes))

                results[param_name].append(df)

            # Parameters with only parameter name
            else:
                #print('2.param_name:', param_name)
                df = (make_df(param_name, technology=t, value=val, unit='t', \
                **common).pipe(broadcast, node_loc=nodes))

                results[param_name].append(df)

    # Create external demand param
    parname = 'demand'
    demand = gen_mock_demand_cement(s_info)
    df = (make_df(parname, level='demand', commodity='cement', value=demand, \
        unit='t', year=modelyears, **common).pipe(broadcast, node=nodes))
    results[parname].append(df)

    # Add CCS as addon
    parname = 'addon_conversion'
    ccs_tec = ['clinker_wet_cement', 'clinker_dry_cement']
    df = (make_df(parname, mode='M1', \
        type_addon='ccs_cement', \
        value=1, unit='-', **common).pipe(broadcast, node=nodes, technology=ccs_tec))
    results[parname].append(df)

    # Test emission bound
    parname = 'bound_emission'
    df = (make_df(parname, type_tec='all', type_year='cumulative', \
        type_emission='CO2_industry', \
        value=200, unit='-').pipe(broadcast, node=nodes))
    results[parname].append(df)

    # Concatenate to one data frame per parameter
    results = {par_name: pd.concat(dfs) for par_name, dfs in results.items()}

    return results
