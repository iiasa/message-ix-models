# -*- coding: utf-8 -*-
"""
Created on Wed Aug 26 13:41:29 2020

"Aluminum stand-alone run"
Solve and plot
@author: unlu
"""
import message_ix
import ixmp
import pandas as pd
from message_data.tools import make_df
from generate_data_AL import gen_data_aluminum as gen_data_aluminum
from generate_data_generic import gen_data_generic as gen_data_generic
from tools import Plots

mp = ixmp.Platform()

# Adding a new unit to the library
mp.add_unit('Mt')

# New model
scenario = message_ix.Scenario(mp, model='MESSAGE_material',
                               scenario='baseline', version='new')
# Addition of basics

history = [1980, 1985, 1990, 1995, 2000, 2005, 2010, 2015]
model_horizon = [2020, 2030, 2040, 2050, 2060, 2070, 2080, 2090, 2100]
scenario.add_horizon({'year': history + model_horizon,
                      'firstmodelyear': model_horizon[0]})
country = 'CHN'
scenario.add_spatial_sets({'country': country})

# These will come from the yaml file

commodities = ['ht_heat', 'lt_heat', 'aluminum', 'd_heat', "electr",
               "coal", "fueloil", "ethanol", "biomass", "gas", "hydrogen",
               "methanol", "lightoil"]

scenario.add_set("commodity", commodities)

levels = ['useful_aluminum', 'new_scrap', 'old_scrap', 'final_material',
          'useful_material', 'product', "secondary_material", "final",
          "demand"]
scenario.add_set("level", levels)

technologies = ['soderberg_aluminum', 'prebake_aluminum', 'secondary_aluminum',
                'prep_secondary_aluminum', 'finishing_aluminum',
                'manuf_aluminum', 'scrap_recovery_aluminum',
                'furnace_coal_aluminum', 'furnace_foil_aluminum',
                'furnace_methanol_aluminum', 'furnace_biomass_aluminum',
                'furnace_ethanol_aluminum', 'furnace_gas_aluminum',
                'furnace_elec_aluminum', 'furnace_h2_aluminum',
                'hp_gas_aluminum', 'hp_elec_aluminum', 'fc_h2_aluminum',
                'solar_aluminum', 'dheat_aluminum', 'furnace_loil_aluminum',
                "alumina_supply"]

scenario.add_set("technology", technologies)
scenario.add_set("mode", ['standard', 'low_temp', 'high_temp'])

# Create duration period

val = [j-i for i, j in zip(model_horizon[:-1], model_horizon[1:])]
val.append(val[0])

duration_period = pd.DataFrame({
        'year': model_horizon,
        'value': val,
        'unit': "y",
    })

scenario.add_par("duration_period", duration_period)
duration_period = duration_period["value"].values

# Energy system: Unlimited supply of the commodities. 
# Fuel costs are obtained from the PRICE_COMMODITY baseline SSP2 global model. 
# PRICE_COMMODTY * input -> (2005USD/Gwa-coal)*(Gwa-coal / ACT of furnace) 
# = (2005USD/ACT of furnace)

years_df = scenario.vintage_and_active_years()
vintage_years, act_years = years_df['year_vtg'], years_df['year_act']

# Choose the prices in excel (baseline vs. NPi400)
data_var_cost = pd.read_excel("variable_costs.xlsx", sheet_name="data")

for row in data_var_cost.index:
    data = data_var_cost.iloc[row]
    values = []
    for yr in act_years:
        values.append(data[yr])
    base_var_cost = pd.DataFrame({
        'node_loc': country,
        'year_vtg': vintage_years.values,
        'year_act': act_years.values,
        'mode': data["mode"],
        'time': 'year',
        'unit': 'USD/GWa',
        "technology": data["technology"],
        "value": values
    })
    
    scenario.add_par("var_cost",base_var_cost)

# Add dummy technologies to represent energy system 

dummy_tech = ["electr_gen", "dist_heating", "coal_gen", "foil_gen", "eth_gen",
              "biomass_gen", "gas_gen", "hydrogen_gen", "meth_gen", "loil_gen"]
scenario.add_set("technology", dummy_tech)  

commodity_tec = ["electr", "d_heat", "coal", "fueloil", "ethanol", "biomass",
                 "gas", "hydrogen", "methanol", "lightoil"]

# Add output to dummy tech.

year_df = scenario.vintage_and_active_years()
vintage_years, act_years = year_df['year_vtg'], year_df['year_act']

base = {
    'node_loc': country,
    "node_dest": country,
    "time_dest": "year",
    'year_vtg': vintage_years,
    'year_act': act_years,
    'mode': 'standard',
    'time': 'year',
    "level": "final",
    'unit': '-',
    "value": 1.0
}

t = 0

for tec in dummy_tech:
    out = make_df("output", technology= tec, commodity = commodity_tec[t],
                  **base)
    t = t + 1
    scenario.add_par("output", out)
    
# Introduce emissions
scenario.add_set('emission', 'CO2')
scenario.add_cat('emission', 'GHG', 'CO2')

# Run read data aluminum 

scenario.commit("changes added")

results_al, data_aluminum_hist = gen_data_aluminum("aluminum_techno_economic.xlsx")

scenario.check_out()

for k, v in results_al.items():
    scenario.add_par(k,v)
    
scenario.commit("aluminum_techno_economic added")
results_generic = gen_data_generic() 

scenario.check_out()
for k, v in results_generic.items():
    scenario.add_par(k,v)
    
# Add temporary exogenous demand: 17.3 Mt in 2010 (IAI)
# The future projection of the demand: Increases by half of the GDP growth rate
# gdp_growth rate: SSP2 global model. Starting from 2020.
gdp_growth = pd.Series([0.121448215899944, 0.0733079014579874,
                        0.0348154093342843, 0.021827616787921,
                        0.0134425983942219, 0.0108320197485592,
                        0.00884341208063, 0.00829374133206562,
                        0.00649794573935969],
                       index=pd.Index(model_horizon, name='Time'))

fin_to_useful = scenario.par("output", filters = {"technology": 
    "finishing_aluminum","year_act":2020})["value"][0]
useful_to_product = scenario.par("output", filters = {"technology": 
    "manuf_aluminum","year_act":2020})["value"][0]
i = 0
values = []
val = (17.3 * (1+ 0.147718884937996/2) ** duration_period[i])
values.append(val)

for element in gdp_growth:
    i = i + 1 
    if i < len(model_horizon):
        val = (val * (1+ element/2) ** duration_period[i]) 
        values.append(val)
        
# Adjust the demand to product level. 

values = [x * fin_to_useful * useful_to_product for x in values]
aluminum_demand = pd.DataFrame({
        'node': country,
        'commodity': 'aluminum',
        'level': 'demand',
        'year': model_horizon,
        'time': 'year',
        'value': values ,
        'unit': 'Mt',
    })
    
scenario.add_par("demand", aluminum_demand)

# Interest rate
scenario.add_par("interestrate", model_horizon, value=0.05, unit='-')

# Add historical production and capacity

for tec in data_aluminum_hist["technology"].unique():
    hist_activity = pd.DataFrame({
    'node_loc': country,
    'year_act': history,
    'mode': data_aluminum_hist.loc[(data_aluminum_hist["technology"]== tec), 
                                                       "mode"],
    'time': 'year',
    'unit': 'Mt',
    "technology": tec,
    "value": data_aluminum_hist.loc[(data_aluminum_hist["technology"]== tec), 
                                    "production"]
    })
    scenario.add_par('historical_activity', hist_activity)
    
for tec in data_aluminum_hist["technology"].unique():
    c_factor = scenario.par("capacity_factor", filters = {"technology": tec})\
    ["value"].values[0]
    value = data_aluminum_hist.loc[(data_aluminum_hist["technology"]== tec), 
                                   "new_production"] / c_factor
    hist_capacity = pd.DataFrame({
    'node_loc': country,
    'year_vtg': history,
    'unit': 'Mt',
    "technology": tec,
    "value": value })
    scenario.add_par('historical_new_capacity', hist_capacity)
    
# Historical thermal demand depending on the historical aluminum production
# This section can be revised to make shorter and generic to other materials

scrap_recovery = scenario.par("output", filters = {"technology": 
    "scrap_recovery_aluminum","level":"old_scrap","year_act":2020})["value"][0]
high_th = scenario.par("input", filters = {"technology": 
    "secondary_aluminum","year_act":2020})["value"][0]
low_th = scenario.par("input", filters = {"technology": 
    "prep_secondary_aluminum","year_act":2020})["value"][0]

historic_generation = scenario.par("historical_activity").\
groupby("year_act").sum()

for yr in historic_generation.index: 

    total_scrap = ((historic_generation.loc[yr].value * fin_to_useful * \
                    (1- useful_to_product)) + (historic_generation.loc[yr] \
                    * fin_to_useful * useful_to_product * scrap_recovery)) 
    old_scrap = (historic_generation.loc[yr] * fin_to_useful * \
                 useful_to_product * scrap_recovery)
    total_hist_act = total_scrap * high_th + old_scrap * low_th 
    
    hist_activity_h = pd.DataFrame({
        'node_loc': country,
        'year_act': yr,
        'mode': 'high_temp',
        'time': 'year',
        'unit': 'GWa',
        "technology": "furnace_gas_aluminum",
        "value": total_scrap * high_th
    })
    
    scenario.add_par('historical_activity', hist_activity_h)
    
    hist_activity_l = pd.DataFrame({
    'node_loc': country,
    'year_act': yr,
    'mode': 'low_temp',
    'time': 'year',
    'unit': 'GWa',
    "technology": "furnace_gas_aluminum",
    "value": old_scrap * low_th
    })
    
    scenario.add_par('historical_activity', hist_activity_l)

    c_fac_furnace_gas = scenario.par("capacity_factor", filters = 
                                     {"technology": "furnace_gas_aluminum"})\
                                     ["value"].values[0]

    hist_capacity_gas = pd.DataFrame({
    'node_loc': country,
    'year_vtg': yr,
    'unit': 'GW',
    "technology": "furnace_gas_aluminum",
    "value": total_hist_act / c_fac_furnace_gas  })

    scenario.add_par('historical_new_capacity', hist_capacity_gas)
    
scenario.commit("changes")

scenario.solve()

# Be aware plots produce the same color for some technologies. 

p = Plots(scenario, country, firstyear=model_horizon[0])

p.plot_activity(baseyear=True, subset=['soderberg_aluminum', 
                                       'prebake_aluminum',"secondary_aluminum"])
p.plot_capacity(baseyear=True, subset=['soderberg_aluminum', 'prebake_aluminum'])
p.plot_activity(baseyear=True, subset=["prep_secondary_aluminum"])
p.plot_activity(baseyear=True, subset=['furnace_coal_aluminum',
                                       'furnace_foil_aluminum',
                                       'furnace_methanol_aluminum',
                                       'furnace_biomass_aluminum',
                                       'furnace_ethanol_aluminum',
                                       'furnace_gas_aluminum',
                                       'furnace_elec_aluminum',
                                       'furnace_h2_aluminum',
                                       'hp_gas_aluminum', 
                                       'hp_elec_aluminum','fc_h2_aluminum',
                                       'solar_aluminum', 'dheat_aluminum',
                                       'furnace_loil_aluminum'])
p.plot_capacity(baseyear=True, subset=['furnace_coal_aluminum',
                                       'furnace_foil_aluminum',
                                       'furnace_methanol_aluminum',
                                       'furnace_biomass_aluminum',
                                       'furnace_ethanol_aluminum',
                                       'furnace_gas_aluminum',
                                       'furnace_elec_aluminum',
                                       'furnace_h2_aluminum',
                                       'hp_gas_aluminum', 
                                       'hp_elec_aluminum','fc_h2_aluminum',
                                       'solar_aluminum', 'dheat_aluminum',
                                       'furnace_loil_aluminum'])
p.plot_prices(subset=['aluminum'], baseyear=True)










