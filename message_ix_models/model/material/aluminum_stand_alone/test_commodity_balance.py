# -*- coding: utf-8 -*-
"""
Created on Thu Aug 27 11:00:06 2020

Test new commodity balance equation
Run stand_alone_AL before runing this script

@author: unlu
"""
import message_ix
import ixmp
from generate_data_AL import gen_data_aluminum as gen_data_aluminum
import pandas as pd
from tools import Plots

mp = ixmp.Platform()

base = message_ix.Scenario(mp, model="MESSAGE_material", scenario='baseline')
scen = base.clone("MESSAGE_material", 'test','test commodity balance equation',
                  keep_solution=False)
scen.check_out()

country = 'CHN'
model_horizon = [2020, 2030, 2040, 2050, 2060, 2070, 2080, 2090, 2100]

# Add a new demand side technology and related commodity
 
new_tec = "bulb"
scen.add_set("technology",new_tec)

new_commodity = "light"
scen.add_set("commodity",new_commodity)

# Add input, output,technical_lifetime, inv_cost etc. 
tec_data, tec_data_hist = gen_data_aluminum("demand_technology.xlsx")

for k, v in tec_data.items():
    scen.add_par(k,v)
    
# Add dummy demand. 1 GWa initially.  

duration_period = scen.par("duration_period")
duration_period = duration_period["value"].values
    
gdp_growth = pd.Series([0.121448215899944, 0.0733079014579874,
                        0.0348154093342843, 0.021827616787921,
                        0.0134425983942219, 0.0108320197485592,
                        0.00884341208063, 0.00829374133206562,
                        0.00649794573935969],
                       index=pd.Index(model_horizon, name='Time'))

i = 0
values = []
val = (1 * (1+ 0.147718884937996/2) ** duration_period[i])
values.append(val)

for element in gdp_growth:
    i = i + 1 
    if i < len(model_horizon):
        val = (val * (1+ element/2) ** duration_period[i]) 
        values.append(val)
        
bulb_demand = pd.DataFrame({
        'node': country,
        'commodity': 'light',
        'level': 'useful_aluminum',
        'year': model_horizon,
        'time': 'year',
        'value': values ,
        'unit': 'GWa',
    })
        
scen.add_par("demand", bulb_demand)
    
# Adjust the active years. 

tec_lifetime = tec_data.get("technical_lifetime")

lifetime = tec_lifetime['value'].values[0]
years_df = scen.vintage_and_active_years()
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

# **** WORKS UNTIL HERE ****** 

# Add the new parameters: input_cap_ret, output_cap_ret, input_cap, output_cap
# input_cap_new, output_cap_new

# Material release after retirement (e.g. old scrap)

output_cap_ret = pd.DataFrame({"node_loc":country,
                               "technology":"bulb",
                               "year_vtg": vintage_years,
                               "node_dest":country,
                               "commodity":"aluminum",
                               "level": "useful_material",
                               "time":"year",
                               "time_dest":"year",
                               "value":0.15,
                               "unit":"-"})
print(output_cap_ret)   
scen.add_par("output_cap_ret", output_cap_ret)
    
# Material need during lifetime (e.g. retrofit)
    
input_cap = pd.DataFrame({"node_loc":country,
                               "technology":"bulb",
                               "year_vtg": vintage_years,
                               "year_act": act_years,
                               "node_origin":country,
                               "commodity":"aluminum",
                               "level": "useful_material",
                               "time":"year",
                               "time_origin":"year",
                               "value":0.01,
                               "unit":"-"})
print(input_cap)
scen.add_par("input_cap", input_cap)

# Material need for the construction

input_cap_new = pd.DataFrame({"node_loc":country,
                           "technology":"bulb",
                           "year_vtg": vintage_years,
                           "node_origin":country,
                           "commodity":"aluminum",
                           "level": "useful_material",
                           "time":"year",
                           "time_origin":"year",
                           "value":0.25,
                           "unit":"-"})
print(input_cap_new)    
scen.add_par("input_cap_new",input_cap_new)

# Material release during construction (e.g. new scrap)
    
output_cap_new = pd.DataFrame({"node_loc":country,
                               "technology":"bulb",
                               "year_vtg": vintage_years,
                               "node_dest":country,
                               "commodity":"aluminum",
                               "level": "useful_material",
                               "time":"year",
                               "time_dest":"year",
                               "value":0.1,
                               "unit":"-"})
print(output_cap_new)
scen.add_par("output_cap_new",output_cap_new)

scen.commit()

#scen.solve()

# Be aware plots produce the same color for some technologies. 

p = Plots(scen, country, firstyear=model_horizon[0])

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





