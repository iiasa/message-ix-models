# -*- coding: utf-8 -*-
"""
Created on Wed Aug 26 14:43:41 2020
Generate techno economic aluminum data based on aluminum_techno_economic.xlsx

@author: unlu
"""
import pandas as pd
import numpy as np
from collections import defaultdict
from .data_util import read_timeseries

import message_ix
import ixmp

from .util import read_config
from .data_util import read_rel
from message_data.tools import (
    ScenarioInfo,
    broadcast,
    make_df,
    make_io,
    make_matched_dfs,
    same_node,
    add_par_data
)
# Get endogenous material demand from buildings interface
from .data_buildings import get_scen_mat_demand
from . import get_spec

def read_data_aluminum(scenario):
    """Read and clean data from :file:`aluminum_techno_economic.xlsx`."""

    # Ensure config is loaded, get the context
    context = read_config()
    s_info = ScenarioInfo(scenario)

    # Shorter access to sets configuration
    # sets = context["material"]["generic"]

    fname = "aluminum_techno_economic.xlsx"

    if "R11_CHN" in s_info.N:
        sheet_n = "data_R12"
        sheet_n_relations = "relations_R12"
    else:
        sheet_n = "data_R11"
        sheet_n_relations = "relations_R11"

    # Read the file
    data_alu = pd.read_excel(context.get_path("material", fname),
               sheet_name=sheet_n,)

    # Drop columns that don't contain useful information
    data_alu= data_alu.drop(["Source", 'Description'], axis = 1)

    data_alu_rel = read_rel(scenario, "aluminum_techno_economic.xlsx")

    data_aluminum_ts = read_timeseries(scenario,"aluminum_techno_economic.xlsx")

    # Unit conversion

    # At the moment this is done in the excel file, can be also done here
    # To make sure we use the same units

    return data_alu, data_alu_rel, data_aluminum_ts

def print_full(x):
    pd.set_option('display.max_rows', len(x))
    print(x)
    pd.reset_option('display.max_rows')

def gen_data_aluminum(scenario, dry_run=False):

    config = read_config()["material"]["aluminum"]

    # Information about scenario, e.g. node, year
    s_info = ScenarioInfo(scenario)

    # Techno-economic assumptions
    data_aluminum, data_aluminum_rel, data_aluminum_ts = read_data_aluminum(scenario)
    # List of data frames, to be concatenated together at end
    results = defaultdict(list)

    # For each technology there are differnet input and output combinations
    # Iterate over technologies

    allyears = s_info.set['year'] #s_info.Y is only for modeling years
    modelyears = s_info.Y #s_info.Y is only for modeling years
    nodes = s_info.N
    yv_ya = s_info.yv_ya
    fmy = s_info.y0
    nodes.remove('World')

    # Do not parametrize GLB region the same way
    if "R11_GLB" in nodes:
        nodes.remove("R11_GLB")

    for t in config["technology"]["add"]:

        params = data_aluminum.loc[(data_aluminum["technology"] == t),"parameter"]\
        .values.tolist()

        # Obtain the active and vintage years
        av = data_aluminum.loc[(data_aluminum["technology"] == t),
                               'availability'].values[0]
        modelyears = [year for year in modelyears if year >= av]
        yva = yv_ya.loc[yv_ya.year_vtg >= av]

        # Iterate over parameters
        for par in params:
            # Obtain the parameter names, commodity,level,emission

            split = par.split("|")
            param_name = split[0]

            # Obtain the scalar value for the parameter

            val = data_aluminum.loc[((data_aluminum["technology"] == t) \
            & (data_aluminum["parameter"] == par)),'value']

            regions = data_aluminum.loc[((data_aluminum["technology"] == t) \
                & (data_aluminum["parameter"] == par)),'region']

            common = dict(
            year_vtg= yv_ya.year_vtg,
            year_act= yv_ya.year_act,
            mode="M1",
            time="year",
            time_origin="year",
            time_dest="year",)

            for rg in regions:
                # For the parameters which inlcudes index names
                if len(split)> 1:

                    if (param_name == "input")|(param_name == "output"):

                        # Assign commodity and level names
                        # Later mod can be added
                        com = split[1]
                        lev = split[2]

                        if (param_name == "input") and (lev == "import"):
                            df = make_df(param_name, technology=t, commodity=com, \
                            level=lev, value=val[regions[regions==rg].index[0]], unit='t', \
                            node_loc=rg, node_origin="R11_GLB", **common)

                        elif (param_name == "output") and (lev == "export"):
                            df = make_df(param_name, technology=t, commodity=com, \
                            level=lev, value=val[regions[regions==rg].index[0]], unit='t', \
                            node_loc=rg, node_dest="R11_GLB", **common)

                        # Assign higher efficiency to younger plants
                        elif (((t == "soderberg_aluminum") or (t == "prebake_aluminum")) \
                         & (com == "electr") & (param_name == "input")):
                              # All the vıntage years
                             year_vtg = sorted(set(yv_ya.year_vtg.values))
                             # Collect the values for the combination of vintage and
                             # active years.
                             input_values_all = []
                             for yr_v in year_vtg:
                                 # The initial year efficiency value
                                 input_values_temp = [val[regions[regions==rg].index[0]]]
                                 # Reduction after the vintage year
                                 year_vtg_filtered = list(filter(lambda op: op>=yr_v, year_vtg))
                                 # Filter the active model years
                                 year_act = yv_ya.loc[yv_ya["year_vtg"]== yr_v,"year_act"].values
                                 for i in range(len(year_vtg_filtered) - 1):
                                     input_values_temp.append(input_values_temp[i] * 1.1)

                                 act_year_no = len(year_act)
                                 input_values_temp = input_values_temp[-act_year_no:]
                                 input_values_all = input_values_all + input_values_temp

                             df = (make_df(param_name, technology=t, commodity=com, \
                             level=lev, value= input_values_all, unit='t', \
                             node_loc=rg, **common).pipe(same_node))

                        else:
                            df = (make_df(param_name, technology=t, commodity=com, \
                            level=lev, value=val[regions[regions==rg].index[0]], unit='t', \
                            node_loc=rg, **common).pipe(same_node))

                        # Copy parameters to all regions, when node_loc is not GLB
                        if (len(regions) == 1) and (rg != "R11_GLB"):
                            df['node_loc'] = None
                            df = df.pipe(broadcast, node_loc=nodes)#.pipe(same_node)
                            # Use same_node only for non-trade technologies
                            if (lev != "import") and (lev != "export"):
                                df = df.pipe(same_node)

                    elif param_name == "emission_factor":

                        # Assign the emisson type
                        emi = split[1]

                        df = make_df(param_name, technology=t, \
                        value=val[regions[regions==rg].index[0]],emission=emi,\
                        unit='t', node_loc=rg, **common)

                # Parameters with only parameter name
                else:
                    df = make_df(param_name, technology=t, \
                    value=val[regions[regions==rg].index[0]], unit='t', \
                    node_loc=rg, **common)

                # Copy parameters to all regions
                if (len(regions) == 1)  and len(set(df['node_loc'])) == 1 and list(set(df['node_loc']))[0]!='R11_GLB':
                    df['node_loc'] = None
                    df = df.pipe(broadcast, node_loc=nodes)

                results[param_name].append(df)

    # Create external demand param
    parname = 'demand'
    demand = gen_mock_demand_aluminum(scenario)
    df = make_df(parname, level='demand', commodity='aluminum', value=demand.value, unit='t', \
        year=demand.year, time='year', node=demand.node)#.pipe(broadcast, node=nodes)
    results[parname].append(df)

    # Special treatment for time-varying params

    tec_ts = set(data_aluminum_ts.technology) # set of tecs in timeseries sheet

    for t in tec_ts:
        common = dict(
            time="year",
            time_origin="year",
            time_dest="year",)

        param_name = data_aluminum_ts.loc[(data_aluminum_ts["technology"] == t), 'parameter']

        for p in set(param_name):
            val = data_aluminum_ts.loc[(data_aluminum_ts["technology"] == t) \
                & (data_aluminum_ts["parameter"] == p), 'value']
            units = data_aluminum_ts.loc[(data_aluminum_ts["technology"] == t) \
                & (data_aluminum_ts["parameter"] == p), 'units'].values[0]
            mod = data_aluminum_ts.loc[(data_aluminum_ts["technology"] == t) \
                & (data_aluminum_ts["parameter"] == p), 'mode']
            yr = data_aluminum_ts.loc[(data_aluminum_ts["technology"] == t) \
                & (data_aluminum_ts["parameter"] == p), 'year']

            if p=="var_cost":
                df = (make_df(p, technology=t, value=val,\
                unit='t', year_vtg=yr, year_act=yr, mode=mod, **common).pipe(broadcast, \
                node_loc=nodes))
            else:
                rg = data_aluminum_ts.loc[(data_aluminum_ts["technology"] == t) \
                    & (data_aluminum_ts["parameter"] == p), 'region']
                df = make_df(p, technology=t, value=val,\
                unit='t', year_vtg=yr, year_act=yr, mode=mod, node_loc=rg, **common)

            results[p].append(df)

    # Add relations for scrap grades and availability

    regions = set(data_aluminum_rel["Region"].values)

    for reg in regions:
        for r in data_aluminum_rel["relation"]:
            if r is None:
                break

            params = set(data_aluminum_rel.loc[(data_aluminum_rel["relation"] == r),\
                "parameter"].values)

            # This relation should start from 2020...
            if r == "minimum_recycling_aluminum":
                modelyears_copy = modelyears[:]
                modelyears_copy.remove(2020)

                common_rel = dict(
                    year_rel = modelyears_copy,
                    year_act = modelyears_copy,
                    mode = 'M1',
                    relation = r,)
            else:

                # Use all the model years for other relations...
                common_rel = dict(
                    year_rel = modelyears,
                    year_act = modelyears,
                    mode = 'M1',
                    relation = r,)

            for par_name in params:
                if par_name == "relation_activity":

                    tec_list = data_aluminum_rel.loc[((data_aluminum_rel["relation"] == r) \
                        & (data_aluminum_rel["parameter"] == par_name)) ,'technology']

                    for tec in tec_list.unique():

                        val = data_aluminum_rel.loc[((data_aluminum_rel["relation"] == r) \
                            & (data_aluminum_rel["parameter"] == par_name) & \
                            (data_aluminum_rel["technology"]==tec) & \
                            (data_aluminum_rel["Region"]==reg)),'value'].values[0]

                        df = (make_df(par_name, technology=tec, value=val, unit='-',\
                        node_loc = reg, node_rel= reg, **common_rel).pipe(same_node))

                        results[par_name].append(df)

                elif (par_name == "relation_upper") | (par_name == "relation_lower"):
                    val = data_aluminum_rel.loc[((data_aluminum_rel["relation"] == r) \
                        & (data_aluminum_rel["parameter"] == par_name) & \
                        (data_aluminum_rel["Region"]==reg)),'value'].values[0]

                    df = (make_df(par_name, value=val, unit='-',node_rel=reg,
                    **common_rel))

                    results[par_name].append(df)

    results_aluminum = {par_name: pd.concat(dfs) for par_name,
                        dfs in results.items()}
    return results_aluminum

def gen_mock_demand_aluminum(scenario):

    context = read_config()
    s_info = ScenarioInfo(scenario)
    modelyears = s_info.Y #s_info.Y is only for modeling years
    fmy = s_info.y0
    nodes = s_info.N

    # Demand at product level (IAI Global Aluminum Cycle 2018)
    # Globally: 82.4 Mt
    # Domestic production + Import
    # AFR: No Data
    # CPA - China: 28.2 Mt
    # EEU / 2 + WEU / 2 = Europe 12.5 Mt
    # FSU: No data
    # LAM: South America: 2.5 Mt
    # MEA: Middle East: 2
    # NAM: North America: 14.1
    # PAO: Japan: 3
    # PAS/2 + SAS /2: Other Asia: 11.5 Mt
    # Remaining 8.612 Mt shared between AFR and FSU
    # This is used as 2020 data.

    # For R12: China and CPA demand divided by 0.1 and 0.9.

    if "R11_CHN" in s_info.N:
        sheet_n = "data_R11"

        r = ['R11_AFR', 'R11_CPA', 'R11_EEU', 'R11_FSU', 'R11_LAM', \
        'R11_MEA', 'R11_NAM', 'R11_PAO', 'R11_PAS', 'R11_SAS', 'R11_WEU']

        d = [3,28, 6,5,2.5,2,13.6,3,4.8,4.8,6]

    else:
        sheet_n = "data_R12"

        r = ['R11_AFR', 'R11_CPA', 'R11_EEU', 'R11_FSU', 'R11_LAM', 'R11_MEA',\
            'R11_NAM', 'R11_PAO', 'R11_PAS', 'R11_SAS', 'R11_WEU',"R11_CHN"]

        d = [3,2,6,5,2.5,2,13.6,3,4.8,4.8,6,26]

    # SSP2 R11 baseline GDP projection
    gdp_growth = pd.read_excel(
        context.get_path("material", "iamc_db ENGAGE baseline GDP PPP.xlsx"),
        sheet_name=sheet_n,)

    gdp_growth = gdp_growth.loc[(gdp_growth['Scenario']=='baseline') & \
    (gdp_growth['Region']!='World')].drop(['Model', 'Variable', 'Unit', 'Notes',\
     2000, 2005], axis = 1)

    gdp_growth['Region'] = 'R11_'+ gdp_growth['Region']

    demand2020_al = pd.DataFrame({'Region':r, 'Val':d}).\
        join(gdp_growth.set_index('Region'), on='Region').\
        rename(columns={'Region':'node'})

    demand2020_al.iloc[:,3:] = demand2020_al.iloc[:,3:].\
        div(demand2020_al[2020], axis=0).\
        multiply(demand2020_al["Val"], axis=0)

    # Do this if we have 2020 demand values for buildings
    sp = get_spec()
    if 'buildings' in sp['add'].set['technology']:
        val = get_scen_mat_demand("aluminum")
        print("Base year demand of {}:".format("aluminum"), val)
        # d = d - val.value
        # Scale down all years' demand values by the 2020 ratio
        demand2020_al.iloc[:,3:] =  demand2020_al.iloc[:,3:].\
            multiply(demand2020_al[2020]- val['value'], axis=0).\
            div(demand2020_al[2020], axis=0)
        print("UPDATE {} demand for 2020!".format("aluminum"))

    demand2020_al = pd.melt(demand2020_al.drop(['Val', 'Scenario'], axis=1),\
        id_vars=['node'], var_name='year', value_name = 'value')

    return demand2020_al
