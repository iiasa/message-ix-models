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

def read_data_petrochemicals(scenario):
    """Read and clean data from :file:`petrochemicals_techno_economic.xlsx`."""

    # Ensure config is loaded, get the context
    context = read_config()
    s_info = ScenarioInfo(scenario)
    fname =  "petrochemicals_techno_economic.xlsx"

    if "R11_CHN" in s_info.N:
        sheet_n = "data_R12"
    else:
        sheet_n = "data_R11"

    # Read the file
    data_petro = pd.read_excel(
        context.get_path("material", fname),sheet_name=sheet_n)
    # Clean the data

    data_petro= data_petro.drop(['Source', 'Description'], axis = 1)

    return data_petro


def gen_mock_demand_petro(scenario):

    context = read_config()
    s_info = ScenarioInfo(scenario)
    modelyears = s_info.Y #s_info.Y is only for modeling years
    fmy = s_info.y0
    nodes = s_info.N

    # 2018 production
    # Use as 2020
    # The Future of Petrochemicals Methodological Annex
    # Projections here do not show too much growth until 2050 for some regions.
    # For division of some regions assumptions made:
    # PAO, PAS, SAS, EEU,WEU

    # For R12: China and CPA demand divided by 0.1 and 0.9.

    if "R11_CHN" in s_info.N:
        sheet_n = "data_R12"

        r = ['R11_AFR', 'R11_CPA', 'R11_EEU', 'R11_FSU', 'R11_LAM', 'R11_MEA',\
                'R11_NAM', 'R11_PAO', 'R11_PAS', 'R11_SAS', 'R11_WEU',"R11_CHN"]

        d_ethylene = [0.853064, 3.2, 2.88788, 8.780442, 8.831229,21.58509,
        32.54942, 8.94036, 28.84327, 28.12818, 16.65209, 28.84327]
        d_propylene = [0.426532, 3.2, 2.88788, 1.463407, 5.298738, 10.79255,
        16.27471, 7.4503, 7.497867, 17.30965, 11.1014,28.84327]
        d_BTX = [0.426532, 3.2, 2.88788, 1.463407, 5.298738, 12.95105, 16.27471,
        7.4503, 7.497867, 17.30965, 11.1014, 28.84327]

    else:

        r = ['R11_AFR', 'R11_CPA', 'R11_EEU', 'R11_FSU', 'R11_LAM', \
        'R11_MEA', 'R11_NAM', 'R11_PAO', 'R11_PAS', 'R11_SAS', 'R11_WEU']

        d_ethylene = [0.853064, 32.04327, 2.88788, 8.780442, 8.831229,21.58509,
        32.54942, 8.94036, 7.497867, 28.12818, 16.65209]
        d_propylene = [0.426532, 32.04327, 2.88788, 1.463407, 5.298738, 10.79255,
        16.27471, 7.4503, 7.497867, 17.30965, 11.1014]
        d_BTX = [0.426532, 32.04327, 2.88788, 1.463407, 5.298738, 12.95105, 16.27471,
        7.4503, 7.497867, 17.30965, 11.1014]


    # SSP2 R11 baseline GDP projection
    gdp_growth = pd.read_excel(
        context.get_path("material", "iamc_db ENGAGE baseline GDP PPP.xlsx"),
        sheet_name=sheet_n,
    )

    gdp_growth = gdp_growth.loc[(gdp_growth['Scenario']=='baseline') & \
    (gdp_growth['Region']!='World')].drop(['Model', 'Variable', 'Unit', \
    'Notes', 2000, 2005], axis = 1)

    gdp_growth['Region'] = 'R11_'+ gdp_growth['Region']

    list = []

    for e in ["ethylene","propylene","BTX"]:
        if e == "ethylene":
            demand2020 = pd.DataFrame({'Region':r, 'Val':d_ethylene}).\
            join(gdp_growth.set_index('Region'), on='Region').\
            rename(columns={'Region':'node'})

        if e == "propylene":
            demand2020 = pd.DataFrame({'Region':r, 'Val':d_propylene}).\
            join(gdp_growth.set_index('Region'), on='Region').\
            rename(columns={'Region':'node'})

        if e == "BTX":
            demand2020 = pd.DataFrame({'Region':r, 'Val':d_BTX}).\
            join(gdp_growth.set_index('Region'), on='Region').\
            rename(columns={'Region':'node'})

        demand2020.iloc[:,3:] = demand2020.iloc[:,3:].div(demand2020[2020], axis=0).\
        multiply(demand2020["Val"], axis=0)

        demand2020 = pd.melt(demand2020.drop(['Val', 'Scenario'], axis=1),\
            id_vars=['node'], var_name='year', value_name = 'value')

        list.append(demand2020)

    return list[0], list[1], list[2]


    # China 2006: 22 kg/cap HVC demand. 2006 population: 1.311 billion
    # This makes 28.842 Mt. (IEA Energy Technology Transitions for Industry)
    # In 2010: 43.263 Mt (1.5 times of 2006)
    # In 2015 72.105 Mt (1.56 times of 2010)
    # Grwoth rates are from CECDATA (assuming same growth rate as ethylene).
    # Distribution in 2015 for China: 6:6:5 (ethylene,propylene,BTX)
    # Future of Petrochemicals Methodological Annex
    # This makes 25 Mt ethylene, 25 Mt propylene, 21 Mt BTX
    # This can be verified by other sources.

def gen_data_petro_chemicals(scenario, dry_run=False):
    # Load configuration

    config = read_config()["material"]["petro_chemicals"]

    # Information about scenario, e.g. node, year
    s_info = ScenarioInfo(scenario)

    # Techno-economic assumptions
    data_petro = read_data_petrochemicals(scenario)
    data_petro_ts = read_timeseries(scenario,"petrochemicals_techno_economic.xlsx")
    # List of data frames, to be concatenated together at end
    results = defaultdict(list)

    # For each technology there are differnet input and output combinations
    # Iterate over technologies

    allyears = s_info.set['year']
    modelyears = s_info.Y #s_info.Y is only for modeling years
    nodes = s_info.N
    yv_ya = s_info.yv_ya
    fmy = s_info.y0
    nodes.remove('World')
    nodes.remove("R11_RCPA")

    # Do not parametrize GLB region the same way
    if "R11_GLB" in nodes:
        nodes.remove("R11_GLB")

    for t in config["technology"]["add"]:

        # years = s_info.Y
        params = data_petro.loc[(data_petro["technology"] == t),"parameter"]\
        .values.tolist()

        # Availability year of the technology
        av = data_petro.loc[(data_petro["technology"] == t),'availability'].\
        values[0]
        modelyears = [year for year in modelyears if year >= av]
        yva = yv_ya.loc[yv_ya.year_vtg >= av, ]

        # Iterate over parameters
        for par in params:
            split = par.split("|")
            param_name = par.split("|")[0]

            val = data_petro.loc[((data_petro["technology"] == t) & \
            (data_petro["parameter"] == par)),'value']

            regions = data_petro.loc[((data_petro["technology"] == t) \
                & (data_petro["parameter"] == par)),'Region']


            # Common parameters for all input and output tables
            # node_dest and node_origin are the same as node_loc

            common = dict(
            year_vtg= yva.year_vtg,
            year_act= yva.year_act,
            time="year",
            time_origin="year",
            time_dest="year",)

            for rg in regions:
                if len(split)> 1:

                    if (param_name == "input")|(param_name == "output"):

                        com = split[1]
                        lev = split[2]
                        mod = split[3]

                        if (param_name == "input") and (lev == "import"):
                            df = make_df(param_name, technology=t, commodity=com, \
                            level=lev, mode= mod, \
                            value=val[regions[regions==rg].index[0]], unit='t', \
                            node_loc=rg, node_origin="R11_GLB", **common)
                        elif (param_name == "output") and (lev == "export"):
                            df = make_df(param_name, technology=t, commodity=com, \
                            level=lev, mode=mod, \
                            value=val[regions[regions==rg].index[0]], unit='t', \
                            node_loc=rg, node_dest="R11_GLB", **common)
                        else:
                            df = (make_df(param_name, technology=t, commodity=com, \
                            level=lev, mode=mod, \
                            value=val[regions[regions==rg].index[0]], unit='t', \
                            node_loc=rg, **common).pipe(same_node))

                        # Copy parameters to all regions, when node_loc is not GLB
                        if (len(regions) == 1) and (rg != "R11_GLB"):
                            # print("copying to all R11", rg, lev)
                            df['node_loc'] = None
                            df = df.pipe(broadcast, node_loc=nodes)#.pipe(same_node)
                            # Use same_node only for non-trade technologies
                            if (lev != "import") and (lev != "export"):
                                df = df.pipe(same_node)



                    elif param_name == "emission_factor":
                        emi = split[1]
                        mod = split[2]

                        df = make_df(param_name, technology=t, \
                        value=val[regions[regions==rg].index[0]],emission=emi,\
                        mode=mod, unit='t', node_loc=rg, **common)

                    elif param_name == "var_cost":
                        mod = split[1]

                        df = (make_df(param_name, technology=t, commodity=com, \
                        level=lev,mode=mod, value=val[regions[regions==rg].index[0]],\
                        unit='t', **common).pipe(broadcast, node_loc=nodes).pipe(same_node))

                # Rest of the parameters apart from inpput, output and emission_factor

                else:

                    df = make_df(param_name, technology=t, \
                    value=val[regions[regions==rg].index[0]], unit='t', \
                    node_loc=rg, **common)

                # Copy parameters to all regions
                if (len(regions) == 1) and (rg != "R11_GLB"):
                    if len(set(df['node_loc'])) == 1 and list(set(df['node_loc']))[0]!='R11_GLB':
                        # print("Copying to all R11")
                        df['node_loc'] = None
                        df = df.pipe(broadcast, node_loc=nodes)

                results[param_name].append(df)

    # Add demand
    # Create external demand param

    demand_e,demand_p,demand_BTX = gen_mock_demand_petro(scenario)
    paramname = "demand"

    df_e = make_df(paramname, level='final_material', commodity="ethylene", \
    value=demand_e.value, unit='t',year=demand_e.year, time='year', \
    node=demand_e.node)#.pipe(broadcast, node=nodes)
    results["demand"].append(df_e)

    df_p = make_df(paramname, level='final_material', commodity="propylene", \
    value=demand_p.value, unit='t',year=demand_p.year, time='year', \
    node=demand_p.node)#.pipe(broadcast, node=nodes)
    results["demand"].append(df_p)

    df_BTX = make_df(paramname, level='final_material', commodity="BTX", \
    value=demand_BTX.value, unit='t',year=demand_BTX.year, time='year', \
    node=demand_BTX.node)#.pipe(broadcast, node=nodes)
    results["demand"].append(df_BTX)

    # Special treatment for time-varying params

    tec_ts = set(data_petro_ts.technology) # set of tecs in timeseries sheet

    for t in tec_ts:
        common = dict(
            time="year",
            time_origin="year",
            time_dest="year",)

        param_name = data_petro_ts.loc[(data_petro_ts["technology"] == t), 'parameter']

        for p in set(param_name):
            val = data_petro_ts.loc[(data_petro_ts["technology"] == t) \
                & (data_petro_ts["parameter"] == p), 'value']
            units = data_petro_ts.loc[(data_petro_ts["technology"] == t) \
                & (data_petro_ts["parameter"] == p), 'units'].values[0]
            mod = data_petro_ts.loc[(data_petro_ts["technology"] == t) \
                & (data_petro_ts["parameter"] == p), 'mode']
            yr = data_petro_ts.loc[(data_petro_ts["technology"] == t) \
                & (data_petro_ts["parameter"] == p), 'year']

            if p=="var_cost":
                df = (make_df(p, technology=t, value=val,\
                unit='t', year_vtg=yr, year_act=yr, mode=mod, **common).pipe(broadcast, \
                node_loc=nodes))
            else:
                rg = data_petro_ts.loc[(data_petro_ts["technology"] == t) \
                    & (data_petro_ts["parameter"] == p), 'region']
                df = make_df(p, technology=t, value=val,\
                unit='t', year_vtg=yr, year_act=yr, mode=mod, node_loc=rg, **common)

            results[p].append(df)

    results = {par_name: pd.concat(dfs) for par_name, dfs in results.items()}

    return results
