from .data_util import read_sector_data, read_timeseries

import numpy as np
from collections import defaultdict
import logging

import pandas as pd

from .util import read_config
from .data_util import read_rel
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

# annual average growth rate by decade (2020-2110)
# gdp_growth = [0.121448215899944, 0.0733079014579874,
#             0.0348154093342843, 0.021827616787921, \
#             0.0134425983942219,  0.0108320197485592, \
#             0.00884341208063,  0.00829374133206562, \
#             0.00649794573935969, 0.00649794573935969]



# Generate a fake steel demand
def gen_mock_demand_steel(scenario):

    context = read_config()
    s_info = ScenarioInfo(scenario)
    modelyears = s_info.Y #s_info.Y is only for modeling years
    fmy = s_info.y0

    # SSP2 R11 baseline GDP projection
    gdp_growth = pd.read_excel(
        context.get_path("material", "iamc_db ENGAGE baseline GDP PPP.xlsx"),
        sheet_name="data",
    )

    gdp_growth = gdp_growth.loc[(gdp_growth['Scenario']=='baseline') & (gdp_growth['Region']!='World')].\
        drop(['Model', 'Variable', 'Unit', 'Notes', 2000, 2005, 2010, 2015], axis = 1)

    gdp_growth['Region'] = 'R11_'+ gdp_growth['Region']

    # True steel use 2010 [Mt/year]
    # https://www.worldsteel.org/en/dam/jcr:0474d208-9108-4927-ace8-4ac5445c5df8/World+Steel+in+Figures+2017.pdf
    r = ['R11_AFR', 'R11_CPA', 'R11_EEU', 'R11_FSU', 'R11_LAM', \
        'R11_MEA', 'R11_NAM', 'R11_PAO', 'R11_PAS', 'R11_SAS', 'R11_WEU']
    d = [35, 537, 70, 53, 49, \
        39, 130, 80, 45, 96, 100]  # MEA change from 39 to 9 to make it feasible (coal supply bound)

    demand2010_steel = pd.DataFrame({'Region':r, 'Val':d}).\
        join(gdp_growth.set_index('Region'), on='Region').rename(columns={'Region':'node'})

    demand2010_steel.iloc[:,3:] = demand2010_steel.iloc[:,3:].\
        div(demand2010_steel[2020], axis=0).\
        multiply(demand2010_steel["Val"], axis=0)

    demand2010_steel = pd.melt(demand2010_steel.drop(['Val', 'Scenario'], axis=1),\
        id_vars=['node'], \
        var_name='year', value_name = 'value')
    #
    # baseyear = list(range(2020, 2110+1, 10))
    # gdp_growth_interp = np.interp(modelyears, baseyear, gdp_growth)
    #
    # i = 0
    # values = []
    #
    # # Assume 5 year duration at the beginning
    # duration_period = (pd.Series(modelyears) - \
    #     pd.Series(modelyears).shift(1)).tolist()
    # duration_period[0] = 5
    #
    # val = (demand2010_steel.val * (1+ 0.147718884937996/2) ** duration_period[i])
    # values.append(val)
    #
    # for element in gdp_growth_interp:
    #     i = i + 1
    #     if i < len(modelyears):
    #         val = (val * (1+ element/2) ** duration_period[i])
    #         values.append(val)

    return demand2010_steel



def gen_data_steel(scenario, dry_run=False):
    """Generate data for materials representation of steel industry.

    """
    # Load configuration
    context = read_config()
    config = context["material"]["steel"]

    # Information about scenario, e.g. node, year
    s_info = ScenarioInfo(scenario)

    # Techno-economic assumptions
    # TEMP: now add cement sector as well => Need to separate those since now I have get_data_steel and cement
    data_steel = read_sector_data("steel")
    # Special treatment for time-dependent Parameters
    data_steel_ts = read_timeseries(context.datafile)
    data_steel_rel = read_rel(context.datafile)

    tec_ts = set(data_steel_ts.technology) # set of tecs with var_cost

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

    # for t in s_info.set['technology']:
    for t in config['technology']['add']:

        params = data_steel.loc[(data_steel["technology"] == t),\
            "parameter"].values.tolist()

        # Special treatment for time-varying params
        if t in tec_ts:
            common = dict(
                time="year",
                time_origin="year",
                time_dest="year",)

            param_name = data_steel_ts.loc[(data_steel_ts["technology"] == t), 'parameter']

            for p in set(param_name):
                val = data_steel_ts.loc[(data_steel_ts["technology"] == t) \
                    & (data_steel_ts["parameter"] == p), 'value']
                units = data_steel_ts.loc[(data_steel_ts["technology"] == t) \
                    & (data_steel_ts["parameter"] == p), 'units'].values[0]
                mod = data_steel_ts.loc[(data_steel_ts["technology"] == t) \
                    & (data_steel_ts["parameter"] == p), 'mode']
                yr = data_steel_ts.loc[(data_steel_ts["technology"] == t) \
                    & (data_steel_ts["parameter"] == p), 'year']

                df = (make_df(p, technology=t, value=val,\
                unit='t', year_vtg=yr, year_act=yr, mode=mod, **common).pipe(broadcast, \
                node_loc=nodes))

                #print("time-dependent::", p, df)
                results[p].append(df)

        # Iterate over parameters
        for par in params:

            # Obtain the parameter names, commodity,level,emission
            split = par.split("|")
            print(split)
            param_name = split[0]
            # Obtain the scalar value for the parameter
            val = data_steel.loc[((data_steel["technology"] == t) \
                & (data_steel["parameter"] == par)),'value']#.values
            regions = data_steel.loc[((data_steel["technology"] == t) \
                & (data_steel["parameter"] == par)),'region']#.values

            common = dict(
                year_vtg= yv_ya.year_vtg,
                year_act= yv_ya.year_act,
                # mode="M1",
                time="year",
                time_origin="year",
                time_dest="year",)

            for rg in regions:

                # For the parameters which inlcudes index names
                if len(split)> 1:

                    print('1.param_name:', param_name, t)
                    if (param_name == "input")|(param_name == "output"):

                        # Assign commodity and level names
                        com = split[1]
                        lev = split[2]
                        mod = split[3]
                        print(rg, par, lev)

                        if (param_name == "input") and (lev == "import"):
                            df = make_df(param_name, technology=t, commodity=com, \
                            level=lev, \
                            value=val[regions[regions==rg].index[0]], mode=mod, unit='t', \
                            node_loc=rg, node_origin="R11_GLB", **common)
                        elif (param_name == "output") and (lev == "export"):
                            df = make_df(param_name, technology=t, commodity=com, \
                            level=lev, \
                            value=val[regions[regions==rg].index[0]], mode=mod, unit='t', \
                            node_loc=rg, node_dest="R11_GLB", **common)
                        else:
                            df = (make_df(param_name, technology=t, commodity=com, \
                            level=lev, \
                            value=val[regions[regions==rg].index[0]], mode=mod, unit='t', \
                            node_loc=rg, **common)\
                            .pipe(same_node))

                        # Copy parameters to all regions, when node_loc is not GLB
                        if (len(regions) == 1) and (rg != "R11_GLB"):
                            print("copying to all R11", rg, lev)
                            df['node_loc'] = None
                            df = df.pipe(broadcast, node_loc=nodes)#.pipe(same_node)
                            # Use same_node only for non-trade technologies
                            if (lev != "import") and (lev != "export"):
                                df = df.pipe(same_node)

                    elif param_name == "emission_factor":

                        # Assign the emisson type
                        emi = split[1]
                        mod = split[2]

                        df = make_df(param_name, technology=t, \
                        value=val[regions[regions==rg].index[0]],\
                        emission=emi, mode=mod, unit='t', \
                        node_loc=rg, **common)

                    else: # time-independent var_cost
                        mod = split[1]
                        df = make_df(param_name, technology=t, \
                        value=val[regions[regions==rg].index[0]], \
                        mode=mod, unit='t', node_loc=rg, \
                        **common)

                # Parameters with only parameter name
                else:
                    print('2.param_name:', param_name)
                    df = make_df(param_name, technology=t, \
                    value=val[regions[regions==rg].index[0]], unit='t', \
                    node_loc=rg, **common)

                # Copy parameters to all regions
                if len(set(df['node_loc'])) == 1 and list(set(df['node_loc']))[0]!='R11_GLB':
                    print("Copying to all R11")
                    df['node_loc'] = None
                    df = df.pipe(broadcast, node_loc=nodes)

                results[param_name].append(df)

    # Add relations for scrap grades and availability

    for r in config['relation']['add']:

        params = set(data_steel_rel.loc[(data_steel_rel["relation"] == r),\
            "parameter"].values)

        common_rel = dict(
            year_rel = modelyears,
            year_act = modelyears,
            mode = 'M1',
            relation = r,)

        for par_name in params:
            if par_name == "relation_activity":

                val = data_steel_rel.loc[((data_steel_rel["relation"] == r) \
                    & (data_steel_rel["parameter"] == par_name)),'value'].values
                tec = data_steel_rel.loc[((data_steel_rel["relation"] == r) \
                    & (data_steel_rel["parameter"] == par_name)),'technology'].values

                print(par_name, "val", val, "tec", tec)

                df = (make_df(par_name, technology=tec, \
                            value=val, unit='-', mode = 'M1', relation = r)\
                    .pipe(broadcast, node_rel=nodes, \
                            node_loc=nodes, year_rel = modelyears))\
                    .assign(year_act=copy_column('year_rel'))

                results[par_name].append(df)

            elif par_name == "relation_upper":

                val = data_steel_rel.loc[((data_steel_rel["relation"] == r) \
                    & (data_steel_rel["parameter"] == par_name)),'value'].values[0]

                df = (make_df(par_name, value=val, unit='-',\
                **common_rel).pipe(broadcast, node_rel=nodes))

                results[par_name].append(df)

    # Create external demand param
    parname = 'demand'
    demand = gen_mock_demand_steel(scenario)
    df = make_df(parname, level='demand', commodity='steel', value=demand.value, unit='t', \
        year=demand.year, time='year', node=demand.node)#.pipe(broadcast, node=nodes)
    results[parname].append(df)

    # Concatenate to one data frame per parameter
    results = {par_name: pd.concat(dfs) for par_name, dfs in results.items()}

    return results
