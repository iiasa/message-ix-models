# -*- coding: utf-8 -*-
"""
Created on Wed Aug 26 14:43:41 2020
Generate techno economic aluminum data based on aluminum_techno_economic.xlsx

@author: unlu
"""
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



def read_data_aluminum():
    """Read and clean data from :file:`aluminum_techno_economic.xlsx`."""

    # Ensure config is loaded, get the context
    context = read_config()

    # Shorter access to sets configuration
    # sets = context["material"]["generic"]

    fname = "aluminum_techno_economic.xlsx"
    # Read the file
    data_alu = pd.read_excel(
        context.get_path("material", fname),
        sheet_name="data",
    )

    # Drop columns that don't contain useful information
    data_alu= data_alu.drop(['Region', 'Source', 'Description'], axis = 1)

    data_alu_hist = pd.read_excel(
        context.get_path("material", fname),
        sheet_name="data_historical",
        usecols = "A:G")

    data_alu_rel = read_rel(fname)
    # Unit conversion

    # At the moment this is done in the excel file, can be also done here
    # To make sure we use the same units

    return data_alu, data_alu_hist, data_alu_rel

def gen_data_aluminum(scenario, dry_run=False):

    config = read_config()["material"]["aluminum"]

    # Information about scenario, e.g. node, year
    s_info = ScenarioInfo(scenario)

    # Techno-economic assumptions
    data_aluminum, data_aluminum_hist, data_aluminum_rel= read_data_aluminum()
    tec_tv = set(data_aluminum_hist.technology) # set of tecs with time-varying values

    # List of data frames, to be concatenated together at end
    results = defaultdict(list)

    # For each technology there are differnet input and output combinations
    # Iterate over technologies

    allyears = s_info.set['year'] #s_info.Y is only for modeling years
    modelyears = s_info.Y #s_info.Y is only for modeling years
    nodes = s_info.N
    yv_ya = s_info.yv_ya
    fmy = s_info.y0

    # 'World' is included by default when creating a message_ix.Scenario().
    # Need to remove it for the China bare model
    nodes.remove('World')

    for t in config["technology"]["add"]:

        # years = s_info.Y
        params = data_aluminum.loc[(data_aluminum["technology"] == t),"parameter"]\
        .values.tolist()

        # Special treatment for time-varying params
        if t in tec_tv:
            common = dict(
                time="year",
                time_origin="year",
                time_dest="year",)

            param_name = data_aluminum_hist.loc[(data_aluminum_hist["technology"] == t), 'parameter']

            for p in set(param_name):
                val = data_aluminum_hist.loc[(data_aluminum_hist["technology"] == t) \
                    & (data_aluminum_hist["parameter"] == p), 'value']
                units = data_aluminum_hist.loc[(data_aluminum_hist["technology"] == t) \
                    & (data_aluminum_hist["parameter"] == p), 'unit'].values[0]
                mod = data_aluminum_hist.loc[(data_aluminum_hist["technology"] == t) \
                    & (data_aluminum_hist["parameter"] == p), 'mode']
                yr = data_aluminum_hist.loc[(data_aluminum_hist["technology"] == t) \
                    & (data_aluminum_hist["parameter"] == p), 'year']

                df = (make_df(p, technology=t, value=val,\
                unit='t', year_vtg=yr, year_act=yr, mode=mod, **common).pipe(broadcast, \
                node_loc=nodes))

                #print("time-dependent::", p, df)
                results[p].append(df)

        # Obtain the active and vintage years
        print("aluminum", t)
        av = data_aluminum.loc[(data_aluminum["technology"] == t),
                               'availability'].values[0]
        if "technical_lifetime" in data_aluminum.loc[
                (data_aluminum["technology"] == t)]["parameter"].values:
            lifetime = data_aluminum.loc[(data_aluminum["technology"] == t)
                                        & (data_aluminum["parameter"] ==
                                           "technical_lifetime"), 'value'].\
                                           values[0]
            years_df = scenario.vintage_and_active_years()
            years_df = years_df.loc[years_df["year_vtg"]>= av]
            # Empty data_frame
            years_df_final = pd.DataFrame(columns=["year_vtg","year_act"])

        # For each vintage adjsut the active years according to technical
        # lifetime
        for vtg in years_df["year_vtg"].unique():
            years_df_temp = years_df.loc[years_df["year_vtg"]== vtg]
            years_df_temp = years_df_temp.loc[years_df["year_act"]
                                              < vtg + lifetime]
            years_df_final = pd.concat([years_df_temp, years_df_final],
                                       ignore_index=True)
        vintage_years, act_years = years_df_final['year_vtg'], \
        years_df_final['year_act']

        params = data_aluminum.loc[(data_aluminum["technology"] == t),\
                                   "parameter"].values.tolist()
        # Iterate over parameters
        for par in params:
            split = par.split("|")
            param_name = par.split("|")[0]

            val = data_aluminum.loc[((data_aluminum["technology"] == t) &
                                     (data_aluminum["parameter"] == par)),\
                                'value'].values[0]

            # Common parameters for all input and output tables
            # node_dest and node_origin are the same as node_loc

            common = dict(
            year_vtg= vintage_years,
            year_act= act_years,
            mode="M1",
            time="year",
            time_origin="year",
            time_dest="year",)

            if len(split)> 1:

                if (param_name == "input")|(param_name == "output"):

                    com = split[1]
                    lev = split[2]

                    df = (make_df(param_name, technology=t, commodity=com,
                                  level=lev, value=val, unit='t', **common)
                    .pipe(broadcast, node_loc=nodes).pipe(same_node))

                    results[param_name].append(df)

                elif param_name == "emission_factor":
                    emi = split[1]

                    df = (make_df(param_name, technology=t,value=val,
                                  emission=emi, unit='t', **common)
                    .pipe(broadcast, node_loc=nodes))
                    results[param_name].append(df)

            # Rest of the parameters apart from inpput, output and
            # emission_factor
            else:
                df = (make_df(param_name, technology=t, value=val,unit='t',
                              **common).pipe(broadcast, node_loc=nodes))
                results[param_name].append(df)

    for r in config['relation']['add']:

        params = data_aluminum_rel.loc[(data_aluminum_rel["relation"] == r),\
            "parameter"].values.tolist()

        common_rel = dict(
            year_rel = modelyears,
            year_act = modelyears,
            mode = 'M1',
            relation = r,)

        for par_name in params:
            if par_name == "relation_activity":

                val = data_aluminum_rel.loc[((data_aluminum_rel["relation"] == r) \
                    & (data_aluminum_rel["parameter"] == par_name)),'value'].values[0]
                tec = data_aluminum_rel.loc[((data_aluminum_rel["relation"] == r) \
                    & (data_aluminum_rel["parameter"] == par_name)),'technology'].values[0]

                df = (make_df(par_name, technology=tec, value=val, unit='-',\
                **common_rel).pipe(broadcast, node_rel=nodes, node_loc=nodes))

                results[par_name].append(df)

            elif par_name == "relation_upper":

                val = data_aluminum_rel.loc[((data_aluminum_rel["relation"] == r) \
                    & (data_aluminum_rel["parameter"] == par_name)),'value'].values[0]

                df = (make_df(par_name, value=val, unit='-',\
                **common_rel).pipe(broadcast, node_rel=nodes))

                results[par_name].append(df)

            # print(par_name, df)

    results_aluminum = {par_name: pd.concat(dfs) for par_name,
                        dfs in results.items()}

    return results_aluminum#,  data_aluminum_hist


def add_other_data_aluminum(scenario, dry_run=False):
    # # Adding a new unit to the library
    # mp.add_unit('Mt')
    #
    # # New model
    # scenario = message_ix.Scenario(mp, model='MESSAGE_material',
    #                                scenario='baseline', version='new')
    # # Addition of basics
    #
    # history = [1980, 1985, 1990, 1995, 2000, 2005, 2010, 2015]
    # model_horizon = [2020, 2030, 2040, 2050, 2060, 2070, 2080, 2090, 2100]
    # scenario.add_horizon({'year': history + model_horizon,
    #                       'firstmodelyear': model_horizon[0]})
    # country = 'CHN'
    # scenario.add_spatial_sets({'country': country})
    #
    # # These will come from the yaml file
    #
    # commodities = ['ht_heat', 'lt_heat', 'aluminum', 'd_heat', "electr",
    #                "coal", "fueloil", "ethanol", "biomass", "gas", "hydrogen",
    #                "methanol", "lightoil"]
    #
    # scenario.add_set("commodity", commodities)
    #
    # levels = ['useful_aluminum', 'new_scrap', 'old_scrap', 'final_material',
    #           'useful_material', 'product', "secondary_material", "final",
    #           "demand"]
    # scenario.add_set("level", levels)
    #
    # technologies = ['soderberg_aluminum', 'prebake_aluminum', 'secondary_aluminum',
    #                 'prep_secondary_aluminum', 'finishing_aluminum',
    #                 'manuf_aluminum', 'scrap_recovery_aluminum',
    #                 'furnace_coal_aluminum', 'furnace_foil_aluminum',
    #                 'furnace_methanol_aluminum', 'furnace_biomass_aluminum',
    #                 'furnace_ethanol_aluminum', 'furnace_gas_aluminum',
    #                 'furnace_elec_aluminum', 'furnace_h2_aluminum',
    #                 'hp_gas_aluminum', 'hp_elec_aluminum', 'fc_h2_aluminum',
    #                 'solar_aluminum', 'dheat_aluminum', 'furnace_loil_aluminum',
    #                 "alumina_supply"]
    #
    # scenario.add_set("technology", technologies)
    # scenario.add_set("mode", ['standard', 'low_temp', 'high_temp'])
    #
    # # Create duration period
    #
    # val = [j-i for i, j in zip(model_horizon[:-1], model_horizon[1:])]
    # val.append(val[0])
    #
    # duration_period = pd.DataFrame({
    #         'year': model_horizon,
    #         'value': val,
    #         'unit': "y",
    #     })
    #
    # scenario.add_par("duration_period", duration_period)
    # duration_period = duration_period["value"].values
    #
    # # Energy system: Unlimited supply of the commodities.
    # # Fuel costs are obtained from the PRICE_COMMODITY baseline SSP2 global model.
    # # PRICE_COMMODTY * input -> (2005USD/Gwa-coal)*(Gwa-coal / ACT of furnace)
    # # = (2005USD/ACT of furnace)
    #
    # years_df = scenario.vintage_and_active_years()
    # vintage_years, act_years = years_df['year_vtg'], years_df['year_act']
    #
    # # Choose the prices in excel (baseline vs. NPi400)
    # data_var_cost = pd.read_excel("variable_costs.xlsx", sheet_name="data")
    #
    # for row in data_var_cost.index:
    #     data = data_var_cost.iloc[row]
    #     values = []
    #     for yr in act_years:
    #         values.append(data[yr])
    #     base_var_cost = pd.DataFrame({
    #         'node_loc': country,
    #         'year_vtg': vintage_years.values,
    #         'year_act': act_years.values,
    #         'mode': data["mode"],
    #         'time': 'year',
    #         'unit': 'USD/GWa',
    #         "technology": data["technology"],
    #         "value": values
    #     })
    #
    #     scenario.add_par("var_cost",base_var_cost)
    #
    # # Add dummy technologies to represent energy system
    #
    # dummy_tech = ["electr_gen", "dist_heating", "coal_gen", "foil_gen", "eth_gen",
    #               "biomass_gen", "gas_gen", "hydrogen_gen", "meth_gen", "loil_gen"]
    # scenario.add_set("technology", dummy_tech)
    #
    # commodity_tec = ["electr", "d_heat", "coal", "fueloil", "ethanol", "biomass",
    #                  "gas", "hydrogen", "methanol", "lightoil"]
    #
    # # Add output to dummy tech.
    #
    # year_df = scenario.vintage_and_active_years()
    # vintage_years, act_years = year_df['year_vtg'], year_df['year_act']
    #
    # base = {
    #     'node_loc': country,
    #     "node_dest": country,
    #     "time_dest": "year",
    #     'year_vtg': vintage_years,
    #     'year_act': act_years,
    #     'mode': 'standard',
    #     'time': 'year',
    #     "level": "final",
    #     'unit': '-',
    #     "value": 1.0
    # }
    #
    # t = 0
    #
    # for tec in dummy_tech:
    #     out = make_df("output", technology= tec, commodity = commodity_tec[t],
    #                   **base)
    #     t = t + 1
    #     scenario.add_par("output", out)
    #
    # # Introduce emissions
    # scenario.add_set('emission', 'CO2')
    # scenario.add_cat('emission', 'GHG', 'CO2')
    #
    # # Run read data aluminum
    #
    # scenario.commit("changes added")
    #
    # results_al, data_aluminum_hist = gen_data_aluminum("aluminum_techno_economic.xlsx")
    #
    # scenario.check_out()
    #
    # for k, v in results_al.items():
    #     scenario.add_par(k,v)
    #
    # scenario.commit("aluminum_techno_economic added")
    # results_generic = gen_data_generic()
    #
    # scenario.check_out()
    # for k, v in results_generic.items():
    #     scenario.add_par(k,v)

    # Information about scenario, e.g. node, year
    s_info = ScenarioInfo(scenario)

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

    # Add temporary exogenous demand: 17.3 Mt in 2010 (IAI)
    demand2010_aluminum = 17.3

    # The future projection of the demand: Increases by half of the GDP growth rate
    # gdp_growth rate: SSP2 global model. Starting from 2020.
    gdp_growth = [0.121448215899944, 0.0733079014579874,
                            0.0348154093342843, 0.021827616787921,
                            0.0134425983942219, 0.0108320197485592,
                            0.00884341208063, 0.00829374133206562,
                            # Add one more element since model is until 2110 normally
                            0.00649794573935969, 0.00649794573935969]
    baseyear = list(range(2020, 2110+1, 10)) # Index for above vector
    gdp_growth_interp = np.interp(modelyears, baseyear, gdp_growth)

    fin_to_useful = scenario.par("output", filters = {"technology":
        "finishing_aluminum","year_act":2020})["value"][0]
    useful_to_product = scenario.par("output", filters = {"technology":
        "manuf_aluminum","year_act":2020})["value"][0]
    i = 0
    values = []

    # Assume 5 year duration at the beginning
    duration_period = (pd.Series(modelyears) - \
        pd.Series(modelyears).shift(1)).tolist()
    duration_period[0] = 5

    # print('modelyears', modelyears)
    # print('duration_period', duration_period)

    val = (demand2010_aluminum * (1+ 0.147718884937996/2) ** duration_period[i])
    values.append(val)

    for element in gdp_growth_interp:
        i = i + 1
        if i < len(modelyears):
            val = (val * (1+ element/2) ** duration_period[i])
            values.append(val)
        # print('val', val)
    # Adjust the demand to product level.

    values = [x * fin_to_useful * useful_to_product for x in values]

    aluminum_demand = (make_df('demand', level='demand', commodity='aluminum', value=values, \
        unit='Mt', time = 'year', year=modelyears).pipe(broadcast, node=nodes))
    # results[parname].append(df)

    # print('demand', aluminum_demand)
    # aluminum_demand = pd.DataFrame({
    #         'node': nodes,
    #         'commodity': 'aluminum',
    #         'level': 'demand',
    #         'year': modelyears,
    #         'time': 'year',
    #         'value': values,
    #         'unit': 'Mt',
    #     })

    results['demand'].append(aluminum_demand)

    # scenario.add_par("demand", aluminum_demand)

    # # Interest rate
    # scenario.add_par("interestrate", model_horizon, value=0.05, unit='-')

    # Add historical production and capacity
    #
    # for tec in data_aluminum_hist["technology"].unique():
    #     hist_activity = pd.DataFrame({
    #     'node_loc': country,
    #     'year_act': history,
    #     'mode': data_aluminum_hist.loc[(data_aluminum_hist["technology"]== tec),
    #                                                        "mode"],
    #     'time': 'year',
    #     'unit': 'Mt',
    #     "technology": tec,
    #     "value": data_aluminum_hist.loc[(data_aluminum_hist["technology"]== tec),
    #                                     "production"]
    #     })
    #     scenario.add_par('historical_activity', hist_activity)

    # for tec in data_aluminum_hist["technology"].unique():
    #     c_factor = scenario.par("capacity_factor", filters = {"technology": tec})\
    #     ["value"].values[0]
    #     value = data_aluminum_hist.loc[(data_aluminum_hist["technology"]== tec),
    #                                    "new_production"] / c_factor
    #     hist_capacity = pd.DataFrame({
    #     'node_loc': country,
    #     'year_vtg': history,
    #     'unit': 'Mt',
    #     "technology": tec,
    #     "value": value })
    #     scenario.add_par('historical_new_capacity', hist_capacity)
    #
    # Historical thermal demand depending on the historical aluminum production
    # This section can be revised to make shorter and generic to other materials

    scrap_recovery = scenario.par("output", filters = {"technology":
        "scrap_recovery_aluminum","level":"old_scrap","year_act":2020})["value"][0]
    high_th = scenario.par("input", filters = {"technology":
        "secondary_aluminum","year_act":2020})["value"][0]
    low_th = scenario.par("input", filters = {"technology":
        "prep_secondary_aluminum_1","year_act":2020})["value"][0] #JM: tec name?

    # What is this? Aggregate activity of alu tecs?
    historic_generation = scenario.par("historical_activity").\
    groupby("year_act").sum()

    for yr in historic_generation.index:

        total_scrap = ((historic_generation.loc[yr].value * fin_to_useful * \
                        (1- useful_to_product)) + (historic_generation.loc[yr] \
                        * fin_to_useful * useful_to_product * scrap_recovery))
        old_scrap = (historic_generation.loc[yr] * fin_to_useful * \
                     useful_to_product * scrap_recovery)
        total_hist_act = total_scrap * high_th + old_scrap * low_th

        # hist_activity_h = pd.DataFrame({
        #     'node_loc': nodes,
        #     'year_act': yr,
        #     'mode': 'high_temp',
        #     'time': 'year',
        #     'unit': 'GWa',
        #     "technology": "furnace_gas_aluminum",
        #     "value": total_scrap * high_th
        # })

        hist_activity_h = make_df('historical_activity', year_act= yr, mode= 'high_temp', \
            time= 'year', unit= 'GWa', \
            technology= "furnace_gas_aluminum", \
            value= total_scrap * high_th).pipe(broadcast, node_loc=nodes)

        print('historical_activity 1', hist_activity_h)
        # scenario.add_par('historical_activity', hist_activity_h)
        results['historical_activity'].append(hist_activity_h)

        # hist_activity_l = pd.DataFrame({
        # 'node_loc': nodes,
        # 'year_act': yr,
        # 'mode': 'low_temp',
        # 'time': 'year',
        # 'unit': 'GWa',
        # "technology": "furnace_gas_aluminum",
        # "value": old_scrap * low_th
        # })
        hist_activity_l = make_df('historical_activity', year_act= yr, mode= 'low_temp', \
            time= 'year', unit= 'GWa', \
            technology= 'furnace_gas_aluminum', \
            value= old_scrap * low_th).pipe(broadcast, node_loc=nodes)

        print('historical_activity 2', hist_activity_l)
        # scenario.add_par('historical_activity', hist_activity_l)
        results['historical_activity'].append(hist_activity_l)

        c_fac_furnace_gas = scenario.par("capacity_factor", filters =
                                         {"technology": "furnace_gas_aluminum"})\
                                         ["value"].values[0]

        # hist_capacity_gas = pd.DataFrame({
        # 'node_loc': nodes,
        # 'year_vtg': yr,
        # 'unit': 'GW',
        # "technology": "furnace_gas_aluminum",
        # "value": total_hist_act / c_fac_furnace_gas  })
        hist_capacity_gas = make_df('historical_new_capacity', year_vtg = yr, unit = 'GW', \
            technology = "furnace_gas_aluminum", \
            value = total_hist_act / c_fac_furnace_gas).pipe(broadcast, node_loc=nodes)
        print('historical_new_capacity', hist_capacity_gas)
        # scenario.add_par('historical_new_capacity', hist_capacity_gas)
        results['historical_new_capacity'].append(hist_capacity_gas)

        # scenario.commit('aluminum other data added')

    # Concatenate to one data frame per parameter
    results = {par_name: pd.concat(dfs) for par_name, dfs in results.items()}

    return results
    #
    # scenario.commit("changes")
    #
    # scenario.solve()
    #
    # # Be aware plots produce the same color for some technologies.
    #
    # p = Plots(scenario, country, firstyear=model_horizon[0])
    #
    # p.plot_activity(baseyear=True, subset=['soderberg_aluminum',
    #                                        'prebake_aluminum',"secondary_aluminum"])
    # p.plot_capacity(baseyear=True, subset=['soderberg_aluminum', 'prebake_aluminum'])
    # p.plot_activity(baseyear=True, subset=["prep_secondary_aluminum"])
    # p.plot_activity(baseyear=True, subset=['furnace_coal_aluminum',
    #                                        'furnace_foil_aluminum',
    #                                        'furnace_methanol_aluminum',
    #                                        'furnace_biomass_aluminum',
    #                                        'furnace_ethanol_aluminum',
    #                                        'furnace_gas_aluminum',
    #                                        'furnace_elec_aluminum',
    #                                        'furnace_h2_aluminum',
    #                                        'hp_gas_aluminum',
    #                                        'hp_elec_aluminum','fc_h2_aluminum',
    #                                        'solar_aluminum', 'dheat_aluminum',
    #                                        'furnace_loil_aluminum'])
    # p.plot_capacity(baseyear=True, subset=['furnace_coal_aluminum',
    #                                        'furnace_foil_aluminum',
    #                                        'furnace_methanol_aluminum',
    #                                        'furnace_biomass_aluminum',
    #                                        'furnace_ethanol_aluminum',
    #                                        'furnace_gas_aluminum',
    #                                        'furnace_elec_aluminum',
    #                                        'furnace_h2_aluminum',
    #                                        'hp_gas_aluminum',
    #                                        'hp_elec_aluminum','fc_h2_aluminum',
    #                                        'solar_aluminum', 'dheat_aluminum',
    #                                        'furnace_loil_aluminum'])
    # p.plot_prices(subset=['aluminum'], baseyear=True)
