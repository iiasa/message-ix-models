import pandas as pd
import numpy as np
from collections import defaultdict

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

def read_data_petrochemicals():
    """Read and clean data from :file:`petrochemicals_techno_economic.xlsx`."""

    # Ensure config is loaded, get the context
    context = read_config()

    # Read the file
    data_petro = pd.read_excel(
        context.get_path("material", "petrochemicals_techno_economic.xlsx"),
        sheet_name="data")
    # Clean the data

    data_petro= data_petro.drop(['Region', 'Source', 'Description'], axis = 1)

    data_petro_hist = pd.read_excel(context.get_path("material", \
    "petrochemicals_techno_economic.xlsx"),sheet_name="data_historical", \
    usecols = "A:G", nrows= 16)
    print("data petro hist")
    print(data_petro_hist)
    return data_petro,data_petro_hist


def gen_mock_demand_petro(scenario):

    context = read_config()
    s_info = ScenarioInfo(scenario)
    modelyears = s_info.Y #s_info.Y is only for modeling years
    fmy = s_info.y0

    # SSP2 R11 baseline GDP projection
    gdp_growth = pd.read_excel(
        context.get_path("material", "iamc_db ENGAGE baseline GDP PPP.xlsx"),
        sheet_name="data",
    )

    gdp_growth = gdp_growth.loc[(gdp_growth['Scenario']=='baseline') & \
    (gdp_growth['Region']!='World')].drop(['Model', 'Variable', 'Unit', \
    'Notes', 2000, 2005, 2010, 2015], axis = 1)

    gdp_growth['Region'] = 'R11_'+ gdp_growth['Region']

    # 2015 demand
    # The Future of Petrochemicals Methodological Annex
    # Projections here do not show too much growth until 2050 for some regions.
    # For division of some regions assumptions made:
    # PAO, PAS, SAS, EEU,WEU

    r = ['R11_AFR', 'R11_CPA', 'R11_EEU', 'R11_FSU', 'R11_LAM', \
        'R11_MEA', 'R11_NAM', 'R11_PAO', 'R11_PAS', 'R11_SAS', 'R11_WEU']
    d_ethylene = [1,25,10,3,5,20,25,12,10,13,15]
    d_propylene = [0.5,25, 10, 0.5, 3, 10, 15, 10,7,8, 10]
    d_BTX = [0.5,25, 10, 0.5, 3, 10, 15, 10,7,8, 10]
    list = []

    for e in ["ethylene","propylene","BTX"]:
        print(e)
        if e == "ethylene":
            demand2015 = pd.DataFrame({'Region':r, 'Val':d_ethylene}).\
            join(gdp_growth.set_index('Region'), on='Region').\
            rename(columns={'Region':'node'})

        if e == "propylene":
            demand2015 = pd.DataFrame({'Region':r, 'Val':d_propylene}).\
            join(gdp_growth.set_index('Region'), on='Region').\
            rename(columns={'Region':'node'})

        if e == "BTX":
            demand2015 = pd.DataFrame({'Region':r, 'Val':d_BTX}).\
            join(gdp_growth.set_index('Region'), on='Region').\
            rename(columns={'Region':'node'})

        demand2015.iloc[:,3:] = demand2015.iloc[:,3:].div(demand2015[2020], axis=0).\
        multiply(demand2015["Val"], axis=0)

        demand2015 = pd.melt(demand2015.drop(['Val', 'Scenario'], axis=1),\
            id_vars=['node'], var_name='year', value_name = 'value')

        print(demand2015)

        list.append(demand2015)
        print("list")
        print(list)

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

    # The future projection of the demand: Increases by half of the GDP growth rate.
    # Starting from 2020.
    # context = read_config()
    # s_info = ScenarioInfo(scenario)
    # modelyears = s_info.Y #s_info.Y is only for modeling years
    #
    # gdp_growth = [0.121448215899944, 0.0733079014579874,
    #                         0.0348154093342843, 0.021827616787921,
    #                         0.0134425983942219, 0.0108320197485592,
    #                         0.00884341208063, 0.00829374133206562,
    #                         # Add one more element since model is until 2110 normally
    #                         0.00649794573935969, 0.00649794573935969]
    # baseyear = list(range(2020, 2110+1, 10)) # Index for above vector
    # gdp_growth_interp = np.interp(modelyears, baseyear, gdp_growth)
    # print(gdp_growth_interp)

    # i = 0
    # values_e = []
    # values_p = []
    # values_BTX = []
    #
    # # Assume 5 year duration at the beginning
    # duration_period = (pd.Series(modelyears) - \
    #     pd.Series(modelyears).shift(1)).tolist()
    # duration_period[0] = 5
    #
    # val_e = (25 * (1+ 0.147718884937996/2) ** duration_period[i])
    # print("val_e")
    # print(val_e)
    # values_e.append(val_e)
    # print(values_e)
    #
    # val_p = (25 * (1+ 0.147718884937996/2) ** duration_period[i])
    # print("val_p")
    # print(val_p)
    # values_p.append(val_p)
    # print(values_p)
    #
    # val_BTX = (21 * (1+ 0.147718884937996/2) ** duration_period[i])
    # print("val_BTX")
    # print(val_BTX)
    # values_BTX.append(val_BTX)
    # print(values_BTX)
    #
    # for element in gdp_growth_interp:
    #     i = i + 1
    #     if i < len(modelyears):
    #         val_e = (val_e * (1+ element/2) ** duration_period[i])
    #         values_e.append(val_e)
    #
    #         val_p = (val_p * (1+ element/2) ** duration_period[i])
    #         values_p.append(val_p)
    #
    #         val_BTX = (val_BTX * (1+ element/2) ** duration_period[i])
    #         values_BTX.append(val_BTX)
    #
    # return values_e, values_p, values_BTX

def gen_data_petro_chemicals(scenario, dry_run=False):
    # Load configuration

    config = read_config()["material"]["petro_chemicals"]

    # Information about scenario, e.g. node, year
    s_info = ScenarioInfo(scenario)

    # Techno-economic assumptions
    data_petro, data_petro_hist = read_data_petrochemicals()
    # List of data frames, to be concatenated together at end
    results = defaultdict(list)

    # For each technology there are differnet input and output combinations
    # Iterate over technologies

    allyears = s_info.set['year']
    modelyears = s_info.Y #s_info.Y is only for modeling years
    nodes = s_info.N
    yv_ya = s_info.yv_ya
    fmy = s_info.y0

    # Do not parametrize GLB region the same way
    if "R11_GLB" in nodes:
        nodes.remove("R11_GLB")

    # 'World' is included by default when creating a message_ix.Scenario().
    # Need to remove it for the China bare model
    nodes.remove('World')

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
            (data_petro["parameter"] == par)),'value'].values[0]

            # Common parameters for all input and output tables
            # year_act is none at the moment
            # node_dest and node_origin are the same as node_loc

            common = dict(
            year_vtg= yva.year_vtg,
            year_act= yva.year_act,
            time="year",
            time_origin="year",
            time_dest="year",)

            if len(split)> 1:

                if (param_name == "input")|(param_name == "output"):

                    com = split[1]
                    lev = split[2]
                    mod = split[3]

                    df = (make_df(param_name, technology=t, commodity=com, \
                    level=lev,mode=mod, value=val, unit='t', **common).\
                    pipe(broadcast, node_loc=nodes).pipe(same_node))

                    results[param_name].append(df)

                elif param_name == "emission_factor":
                    emi = split[1]
                    mod = split[2]

                    df = (make_df(param_name, technology=t,value=val,\
                    emission=emi, mode=mod, unit='t', **common).pipe(broadcast, \
                    node_loc=nodes))

                elif param_name == "var_cost":
                    mod = split[1]

                    df = (make_df(param_name, technology=t, commodity=com, \
                    level=lev,mode=mod, value=val, unit='t', **common).\
                    pipe(broadcast, node_loc=nodes).pipe(same_node))



                    results[param_name].append(df)

            # Rest of the parameters apart from inpput, output and emission_factor

            else:

                df = (make_df(param_name, technology=t, value=val,unit='t', \
                **common).pipe(broadcast, node_loc=nodes))

                results[param_name].append(df)

    # Add demand
    # Create external demand param

    demand_e,demand_p,demand_BTX = gen_mock_demand_petro(scenario)
    paramname = "demand"

    df_e = make_df(paramname, level='demand', commodity="ethylene", \
    value=demand_e.value, unit='t',year=demand_e.year, time='year', \
    node=demand_e.node)#.pipe(broadcast, node=nodes)
    print("demand ethylene")
    print(demand_e.node)
    results["demand"].append(df_e)

    df_p = make_df(paramname, level='demand', commodity="propylene", \
    value=demand_p.value, unit='t',year=demand_p.year, time='year', \
    node=demand_p.node)#.pipe(broadcast, node=nodes)
    print("demand propylene")
    print(demand_p.node)
    results["demand"].append(df_p)

    df_BTX = make_df(paramname, level='demand', commodity="BTX", \
    value=demand_BTX.value, unit='t',year=demand_BTX.year, time='year', \
    node=demand_BTX.node)#.pipe(broadcast, node=nodes)
    print("demand BTX")
    print(demand_BTX.node)
    results["demand"].append(df_BTX)


    # # Add historical data
    #
    # print(data_petro_hist["technology"].unique())
    # for tec in data_petro_hist["technology"].unique():
    #     print(tec)
    #
    #     y_hist = [1980,1985,1990,1995,2000,2005,2010,2015] #length need to match what's in the xls
    #     common_hist = dict(
    #         year_vtg= y_hist,
    #         year_act= y_hist,
    #         mode="M1",
    #         time="year",)
    #
    #     print("historical years")
    #     print(y_hist)
    #     val_act = data_petro_hist.\
    #     loc[(data_petro_hist["technology"]== tec), "production"]
    #     print("value activity")
    #     print(val_act)
    #
    #     df_hist_act = (make_df("historical_activity", technology=tec, \
    #     value=val_act, unit='Mt', **common_hist).pipe(broadcast, node_loc=nodes))
    #
    #     results["historical_activity"].append(df_hist_act)
    #
    #     c_factor = data_petro.loc[((data_petro["technology"]== tec) \
    #                 & (data_petro["parameter"]=="capacity_factor")), "value"].values
    #
    #     val_cap = data_petro_hist.loc[(data_petro_hist["technology"]== tec), \
    #                                     "new_production"] / c_factor
    #
    #     print("This is capacity value")
    #     print(val_cap)
    #
    #     df_hist_cap = (make_df("historical_new_capacity", technology=tec, \
    #     value=val_cap, unit='Mt', **common_hist).pipe(broadcast, node_loc=nodes))
    #
    #     results["historical_new_capacity"].append(df_hist_cap)

    results = {par_name: pd.concat(dfs) for par_name, dfs in results.items()}

    return results
