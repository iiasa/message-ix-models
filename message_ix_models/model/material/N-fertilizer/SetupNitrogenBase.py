# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

# load required packages 
# import itertools
import pandas as pd
import numpy as np

import matplotlib.pyplot as plt
plt.style.use('ggplot')

import ixmp as ix
import message_ix

import os
import time, re

#os.chdir(r'H:\MyDocuments\MESSAGE\message_data')
##from tools.post_processing.iamc_report_hackathon import report as reporting
#
#from message_ix.reporting import Reporter

os.chdir(r'H:\MyDocuments\MESSAGE\N-fertilizer\code.Global')

#import LoadParams # Load techno-economic param values from the excel file
#exec(open(r'LoadParams.py').read())
import importlib
#import LoadParams
importlib.reload(LoadParams)

#%% Set up scope

scen_names = {"baseline" : "NoPolicy",
              "NPi2020-con-prim-dir-ncr" : "NPi",
              "NPi2020_1000-con-prim-dir-ncr" : "NPi2020_1000",
              "NPi2020_400-con-prim-dir-ncr" : "NPi2020_400"}

run_scen = "baseline" # "NPiREF-con-prim-dir-ncr"

# details for existing datastructure to be updated and annotation log in database 
modelName = "CD_Links_SSP2" 
scenarioName = run_scen

# new model name in ix platform
newmodelName = "JM_GLB_NITRO"
newscenarioName = scen_names[run_scen]

comment = "CD_Links_SSP2 test for new representation of nitrogen cycle"

REGIONS = [
    'R11_AFR',
    'R11_CPA',
    'R11_EEU',
    'R11_FSU',
    'R11_LAM',
    'R11_MEA',
    'R11_NAM',
    'R11_PAO',
    'R11_PAS',
    'R11_SAS',
    'R11_WEU',
    'R11_GLB' ]


#%% Load scenario

# launch the IX modeling platform using the local default database                                                                                                                       
mp = ix.Platform(dbprops=r'H:\MyDocuments\MESSAGE\message_ix\config\default.properties')

# Reference scenario
Sc_ref = message_ix.Scenario(mp, modelName, scenarioName)
paramList_tec = [x for x in Sc_ref.par_list() if 'technology' in Sc_ref.idx_sets(x)]
params_src = [x for x in paramList_tec if 'solar_i' in set(Sc_ref.par(x)["technology"].tolist())]

#%% Clone
Sc_nitro = Sc_ref.clone(newmodelName, newscenarioName)
#Sc_nitro = message_ix.Scenario(mp, newmodelName, newscenarioName)

Sc_nitro.remove_solution()
Sc_nitro.check_out()

 
#%% Add new technologies & commodities

newtechnames = ['biomass_NH3', 'electr_NH3', 'gas_NH3', 'coal_NH3', 'fueloil_NH3', 'NH3_to_N_fertil']
newcommnames = ['NH3', 'Fertilizer Use|Nitrogen'] #'N-fertilizer'
newlevelnames = ['material_interim', 'material_final']

Sc_nitro.add_set("technology", newtechnames)
Sc_nitro.add_set("commodity", newcommnames)
Sc_nitro.add_set("level", newlevelnames)
Sc_nitro.add_set("type_tec", 'industry')

cat_add = pd.DataFrame({
        'type_tec': 'industry', #'non_energy' not in Russia model
        'technology': newtechnames
})

Sc_nitro.add_set("cat_tec", cat_add)


#%% Connect input & output

### NH3 production process
for t in newtechnames[0:5]:
    # output
    df = Sc_nitro.par("output", {"technology":["solar_i"]}) # lifetime = 15 year
    df['technology'] = t
    df['commodity'] = newcommnames[0]
    df['level'] = newlevelnames[0]
    Sc_nitro.add_par("output", df) 
    df['commodity'] = 'd_heat'
    df['level'] = 'secondary'      
    df['value'] = LoadParams.output_heat[newtechnames.index(t)] 
    Sc_nitro.add_par("output", df) 
      
    # Fuel input
    df = df.rename(columns={'time_dest':'time_origin', 'node_dest':'node_origin'})
    if t=='biomass_NH3':
        lvl = 'primary'
    else:
        lvl = 'secondary'
    df['level'] = lvl
    # Non-elec fuels
    if t[:-4]!='electr': # electr has only electr input (no other fuel)
        df['commodity'] = t[:-4] # removing '_NH3'    
        df['value'] = LoadParams.input_fuel[newtechnames.index(t)] 
        Sc_nitro.add_par("input", df)         
    # Electricity input (for any fuels)
    df['commodity'] = 'electr' # All have electricity input    
    df['value'] = LoadParams.input_elec[newtechnames.index(t)] 
    df['level'] = 'secondary'
    Sc_nitro.add_par("input", df)        
    
    # Water input # Not exist in Russia model - CHECK for global model
    df['level'] = 'water_supply' # final for feedstock input     
    df['commodity'] = 'freshwater_supply' # All have electricity input    
    df['value'] = LoadParams.input_water[newtechnames.index(t)] 
    Sc_nitro.add_par("input", df)            
    
    df = Sc_nitro.par("technical_lifetime", {"technology":["solar_i"]}) # lifetime = 15 year
    df['technology'] = t
    Sc_nitro.add_par("technical_lifetime", df)   
    
    # Costs
    df = Sc_nitro.par("inv_cost", {"technology":["solar_i"]})
    df['technology'] = t
    df['value'] = LoadParams.inv_cost[newtechnames.index(t)] 
    Sc_nitro.add_par("inv_cost", df) 
    
    df = Sc_nitro.par("fix_cost", {"technology":["solar_i"]})
    df['technology'] = t
    df['value'] = LoadParams.fix_cost[newtechnames.index(t)] 
    Sc_nitro.add_par("fix_cost", df)  
    
    df = Sc_nitro.par("var_cost", {"technology":["solar_i"]})
    df['technology'] = t
    df['value'] = LoadParams.var_cost[newtechnames.index(t)] 
    Sc_nitro.add_par("var_cost", df)   
    
    # Emission factor
    df = Sc_nitro.par("output", {"technology":["solar_i"]})
    df = df.drop(columns=['node_dest', 'commodity', 'level', 'time'])
    df = df.rename(columns={'time_dest':'emission'})
    df['emission'] = 'CO2_transformation' # Check out what it is
    df['value'] = LoadParams.emissions[newtechnames.index(t)] 
    df['technology'] = t
    df['unit'] = '???'
    Sc_nitro.add_par("emission_factor", df)   
    df['emission'] = 'CO2' # Check out what it is
    df['value'] = 0 # Set the same as CO2_transformation
    Sc_nitro.add_par("emission_factor", df) 
    
    # Emission factors in relation (Currently these are more correct values than emission_factor)
    df = Sc_nitro.par("relation_activity",  {"relation":["CO2_cc"], "technology":["h2_smr"]})
    df['value'] = LoadParams.emissions[newtechnames.index(t)] 
    df['technology'] = t
    df['unit'] = '???'
    Sc_nitro.add_par("relation_activity", df)   
#    df = Sc_nitro.par("relation_activity",  {"relation":["CO2_cc"], "technology":t})
#    Sc_nitro.remove_par("relation_activity",  df)

    # Capacity factor
    df = Sc_nitro.par("capacity_factor", {"technology":["solar_i"]})
    df['technology'] = t
    df['value'] = LoadParams.capacity_factor[newtechnames.index(t)] 
    Sc_nitro.add_par("capacity_factor", df)   


### N-fertilizer from NH3 (generic)
comm = newcommnames[-1]
tech = newtechnames[-1]

# output  
df = Sc_nitro.par("output", {"technology":["solar_i"]}) 
df['technology'] = tech
df['commodity'] = comm #'N-fertilizer'  
df['level'] = newlevelnames[-1] #'land_use_reporting'
Sc_nitro.add_par("output", df)     
 
# input
df = Sc_nitro.par("output", {"technology":["solar_i"]}) 
df = df.rename(columns={'time_dest':'time_origin', 'node_dest':'node_origin'})
df['technology'] = tech
df['level'] = newlevelnames[0] 
df['commodity'] = newcommnames[0] #'NH3'     
df['value'] = LoadParams.input_fuel[newtechnames.index(tech)] # NH3/N = 17/14
Sc_nitro.add_par("input", df)  
    
df = Sc_nitro.par("technical_lifetime", {"technology":["solar_i"]}) # lifetime = 15 year
df['technology'] = tech
Sc_nitro.add_par("technical_lifetime", df)   

# Costs
df = Sc_nitro.par("inv_cost", {"technology":["solar_i"]})
df['value'] = LoadParams.inv_cost[newtechnames.index(tech)]
df['technology'] = tech
Sc_nitro.add_par("inv_cost", df) 

df = Sc_nitro.par("fix_cost", {"technology":["solar_i"]})
df['technology'] = tech
df['value'] = LoadParams.fix_cost[newtechnames.index(tech)]
Sc_nitro.add_par("fix_cost", df)  

df = Sc_nitro.par("var_cost", {"technology":["solar_i"]})
df['technology'] = tech
df['value'] = LoadParams.var_cost[newtechnames.index(tech)]
Sc_nitro.add_par("var_cost", df)   

# Emission factor (<0 for this)
"""
Urea applied in the field will emit all CO2 back, so we don't need this.
Source: https://ammoniaindustry.com/urea-production-is-not-carbon-sequestration/#targetText=To%20make%20urea%2C%20fertilizer%20producers,through%20the%20production%20of%20urea.
"""
#df = Sc_nitro.par("output", {"technology":["solar_i"]})
#df = df.drop(columns=['node_dest', 'commodity', 'level', 'time'])
#df = df.rename(columns={'time_dest':'emission'})
#df['emission'] = 'CO2_transformation' # Check out what it is
#df['value'] = LoadParams.emissions[newtechnames.index(tech)] 
#df['technology'] = tech
#df['unit'] = '???'
#Sc_nitro.add_par("emission_factor", df)   
#df['emission'] = 'CO2' # Check out what it is
#Sc_nitro.add_par("emission_factor", df) 


#%% Copy some background parameters# 

par_bgnd = [x for x in params_src if '_up' in x] + [x for x in params_src if '_lo' in x] 
par_bgnd = list(set(par_bgnd) - set(['level_cost_activity_soft_lo', 'level_cost_activity_soft_up', 'growth_activity_lo'])) #, 'soft_activity_lo', 'soft_activity_up']))
for t in par_bgnd[:-1]:
    df = Sc_nitro.par(t, {"technology":["solar_i"]}) # lifetime = 15 year
    for q in newtechnames:
        df['technology'] = q
        Sc_nitro.add_par(t, df)   
        
df = Sc_nitro.par('initial_activity_lo', {"technology":["gas_extr_mpen"]})
for q in newtechnames:
    df['technology'] = q
    Sc_nitro.add_par('initial_activity_lo', df)   
           
df = Sc_nitro.par('growth_activity_lo', {"technology":["gas_extr_mpen"]})
for q in newtechnames:
    df['technology'] = q
    Sc_nitro.add_par('growth_activity_lo', df)      

#%% Process the regional historical activities
   
fs_GLO = LoadParams.feedshare_GLO.copy()     
fs_GLO.insert(1, "bio_pct", 0)
fs_GLO.insert(2, "elec_pct", 0)
# 17/14 NH3:N ratio, to get NH3 activity based on N demand => No NH3 loss assumed during production
fs_GLO.iloc[:,1:6] = LoadParams.input_fuel[5] * fs_GLO.iloc[:,1:6] 
fs_GLO.insert(6, "NH3_to_N", 1)

# Share of feedstocks for NH3 prodution (based on 2010 => Assumed fixed for any past years)
feedshare = fs_GLO.sort_values(['Region']).set_index('Region').drop('R11_GLB')
        
# Get historical N demand from SSP2-nopolicy (may need to vary for diff scenarios)
N_demand_raw = LoadParams.N_demand_GLO.copy()
N_demand = N_demand_raw[N_demand_raw.Scenario=="NoPolicy"].reset_index().loc[0:10,2010] # 2010 tot N demand
N_demand = N_demand.repeat(len(newtechnames))

act2010 = (feedshare.values.flatten() * N_demand).reset_index(drop=True)



#%% Historical activities/capacities - Region specific

df = Sc_nitro.par("historical_activity").iloc[0:len(newtechnames)*(len(REGIONS)-1),] # Choose whatever same number of rows
df['technology'] = newtechnames * (len(REGIONS)-1)
df['value'] = act2010 # total NH3 or N in Mt 2010 FAO Russia 
df['year_act'] = 2010
df['node_loc'] = [y for x in REGIONS[:-1] for y in (x,)*len(newtechnames)]
df['unit'] = 'Tg N/yr' # Separate unit needed for NH3?
Sc_nitro.add_par("historical_activity", df)

# 2015 activity necessary if this is 5-year step scenario
#df['value'] = act2015 # total NH3 or N in Mt 2010 FAO Russia 
#df['year_act'] = 2015
#Sc_nitro.add_par("historical_activity", df)

life = Sc_nitro.par("technical_lifetime", {"technology":["gas_NH3"]}).value[0]

df = Sc_nitro.par("historical_new_capacity").iloc[0:len(newtechnames)*(len(REGIONS)-1),] # whatever
df['technology'] = newtechnames * (len(REGIONS)-1)
df['value'] = [x * 1/life/LoadParams.capacity_factor[0] for x in act2010] # Assume 1/lifetime (=15yr) is built each year
df['year_vtg'] = 2010
df['node_loc'] = [y for x in REGIONS[:-1] for y in (x,)*len(newtechnames)]
df['unit'] = 'Tg N/yr'
Sc_nitro.add_par("historical_new_capacity", df)


    
#%% Secure feedstock balance (foil_fs, gas_fs, coal_fs)  loil_fs?

# Select only the model years
years = set(map(int, list(Sc_nitro.set('year')))) & set(N_demand_raw) # get intersection 

#scenarios = N_demand_FSU.Scenario # Scenario names (SSP2)
N_demand = N_demand_raw.loc[N_demand_raw.Scenario=="NoPolicy",].drop(35)
N_demand = N_demand[N_demand.columns.intersection(years)]
N_demand[2110] = N_demand[2100] # Make up 2110 data (for now) in Mt/year

# Adjust i_feed demand 
demand_fs_org = Sc_nitro.par('demand', {"commodity":["i_feed"]})
#demand_fs_org['value'] = demand_fs_org['value'] * 0.9   # (10% of total feedstock for Ammonia assumed) - REFINE
df = demand_fs_org.loc[demand_fs_org.year==2010,:].join(LoadParams.N_energy.set_index('node'), on='node')
sh = pd.DataFrame( {'node': demand_fs_org.loc[demand_fs_org.year==2010, 'node'], 
                    'r_feed': df.totENE / df.value})    # share of NH3 energy among total feedstock (no trade assumed)
df = demand_fs_org.join(sh.set_index('node'), on='node')
df.value *= 1 - df.r_feed # Carve out the same % from tot i_feed values
df = df.drop('r_feed', axis=1)
Sc_nitro.add_par("demand", df)


# Now link the GLOBIOM input (now endogenous)
df = Sc_nitro.par("land_output", {"commodity":newcommnames[-1]})
df['level'] = newlevelnames[-1]
Sc_nitro.add_par("land_input", df)   


#%% Add CCS tecs

#%% Add tecs to set
tec_for_ccs = list(newtechnames[i] for i in [0,2,3,4])
newtechnames_ccs = list(map(lambda x:str(x)+'_ccs', tec_for_ccs)) #include biomass in CCS, newtechnames[2:5]))
Sc_nitro.add_set("technology", newtechnames_ccs)

cat_add = pd.DataFrame({
        'type_tec': 'industry', #'non_energy' not in Russia model
        'technology': newtechnames_ccs
})

Sc_nitro.add_set("cat_tec", cat_add)



#%% Implement technologies - only for non-elec NH3 tecs

# input and output
# additional electricity input for CCS operation
df = Sc_nitro.par("input")
df = df[df['technology'].isin(tec_for_ccs)]
df['technology'] = df['technology'] + '_ccs'  # Rename the technologies
df.loc[df['commodity']=='electr', ['value']] = df.loc[df['commodity']=='electr', ['value']] + 0.005 # TUNE THIS # Add electricity input for CCS
Sc_nitro.add_par("input", df)   

df = Sc_nitro.par("output")
df = df[df['technology'].isin(tec_for_ccs)]
df['technology'] = df['technology'] + '_ccs'  # Rename the technologies
Sc_nitro.add_par("output", df)   


# Emission factors (emission_factor)

df = Sc_nitro.par("emission_factor")
biomass_ef = 0.942 # MtC/GWa from 109.6 kgCO2/GJ biomass (https://www.rvo.nl/sites/default/files/2013/10/Vreuls%202005%20NL%20Energiedragerlijst%20-%20Update.pdf)

# extract vent vs. storage ratio from h2_smr tec
h2_smr_vent_ratio = -Sc_nitro.par("relation_activity", {"relation":["CO2_cc"], "technology":["h2_smr_ccs"]}).value[0] \
                    / Sc_nitro.par("relation_activity", {"relation":["CO2_Emission"], "technology":["h2_smr_ccs"]}).value[0]
h2_coal_vent_ratio = -Sc_nitro.par("relation_activity", {"relation":["CO2_cc"], "technology":["h2_coal_ccs"]}).value[0] \
                    / Sc_nitro.par("relation_activity", {"relation":["CO2_Emission"], "technology":["h2_coal_ccs"]}).value[0]
h2_bio_vent_ratio = 1 + Sc_nitro.par("relation_activity", {"relation":["CO2_cc"], "technology":["h2_bio_ccs"]}).value[0] \
                    / (Sc_nitro.par("input", {"technology":["h2_bio_ccs"], "commodity":["biomass"]}).value[0] * biomass_ef)

# ef for NG
df_gas = df[df['technology']=='gas_NH3']
ef_org = np.asarray(df_gas.loc[df_gas['emission']=='CO2_transformation', ['value']])
df_gas = df_gas.assign(technology = df_gas.technology + '_ccs')
df_gas.loc[(df.emission=='CO2_transformation'), 'value'] = ef_org*h2_smr_vent_ratio 
df_gas.loc[(df.emission=='CO2'), 'value'] = ef_org*(h2_smr_vent_ratio-1)
Sc_nitro.add_par("emission_factor", df_gas)   

# ef for oil/coal
df_coal = df[df['technology'].isin(tec_for_ccs[2:])]
ef_org = np.asarray(df_coal.loc[df_coal['emission']=='CO2_transformation', ['value']])
df_coal = df_coal.assign(technology = df_coal.technology + '_ccs')
df_coal.loc[(df.emission=='CO2_transformation'), 'value'] = ef_org*h2_coal_vent_ratio 
df_coal.loc[(df.emission=='CO2'), 'value'] = ef_org*(h2_coal_vent_ratio-1)
Sc_nitro.add_par("emission_factor", df_coal)   

# ef for biomass
df_bio = df[df['technology']=='biomass_NH3']
biomass_input = Sc_nitro.par("input", {"technology":["biomass_NH3"], "commodity":["biomass"]}).value[0]
df_bio = df_bio.assign(technology = df_bio.technology + '_ccs')
df_bio['value'] = biomass_input*(h2_bio_vent_ratio-1)*biomass_ef 
Sc_nitro.add_par("emission_factor", df_bio)   




# Investment cost

df = Sc_nitro.par("inv_cost")

# Get inv_cost ratio between std and ccs for different h2 feedstocks
a = df[df['technology'].str.contains("h2_smr")].sort_values(["technology", "year_vtg"])  # To get cost ratio for std vs CCS
r_ccs_cost_gas = a.loc[(a.technology=='h2_smr') & (a.year_vtg >=2020)]['value'].values / a.loc[(a['technology']=='h2_smr_ccs') & (a.year_vtg >=2020)]['value'].values
r_ccs_cost_gas = r_ccs_cost_gas.mean()

a = df[df['technology'].str.contains("h2_coal")].sort_values(["technology", "year_vtg"])  # To get cost ratio for std vs CCS
r_ccs_cost_coal = a.loc[(a.technology=='h2_coal') & (a.year_vtg >=2020)]['value'].values / a.loc[(a['technology']=='h2_coal_ccs') & (a.year_vtg >=2020)]['value'].values
r_ccs_cost_coal = r_ccs_cost_coal.mean()

a = df[df['technology'].str.contains("h2_bio")].sort_values(["technology", "year_vtg"])  # To get cost ratio for std vs CCS
a = a[a.year_vtg > 2025] # h2_bio_ccs only available from 2030
r_ccs_cost_bio = a.loc[(a.technology=='h2_bio') & (a.year_vtg >=2020)]['value'].values / a.loc[(a['technology']=='h2_bio_ccs') & (a.year_vtg >=2020)]['value'].values
r_ccs_cost_bio = r_ccs_cost_bio.mean()

df_gas = df[df['technology']=='gas_NH3']
df_gas['technology'] = df_gas['technology'] + '_ccs'  # Rename the technologies
df_gas['value'] = df_gas['value']/r_ccs_cost_gas 
Sc_nitro.add_par("inv_cost", df_gas)   

df_coal = df[df['technology'].isin(tec_for_ccs[2:])]
df_coal['technology'] = df_coal['technology'] + '_ccs'  # Rename the technologies
df_coal['value'] = df_coal['value']/r_ccs_cost_coal 
Sc_nitro.add_par("inv_cost", df_coal) 

df_bio = df[df['technology']=='biomass_NH3']
df_bio['technology'] = df_bio['technology'] + '_ccs'  # Rename the technologies
df_bio['value'] = df_bio['value']/r_ccs_cost_bio 
Sc_nitro.add_par("inv_cost", df_bio) 




# Fixed cost

df = Sc_nitro.par("fix_cost")
df_gas = df[df['technology']=='gas_NH3']
df_gas['technology'] = df_gas['technology'] + '_ccs'  # Rename the technologies
df_gas['value'] = df_gas['value']/r_ccs_cost_gas # Same scaling (same 4% of inv_cost in the end)
Sc_nitro.add_par("fix_cost", df_gas)   

df_coal = df[df['technology'].isin(tec_for_ccs[2:])]
df_coal['technology'] = df_coal['technology'] + '_ccs'  # Rename the technologies
df_coal['value'] = df_coal['value']/r_ccs_cost_coal # Same scaling (same 4% of inv_cost in the end)
Sc_nitro.add_par("fix_cost", df_coal)  

df_bio = df[df['technology']=='biomass_NH3']
df_bio['technology'] = df_bio['technology'] + '_ccs'  # Rename the technologies
df_bio['value'] = df_bio['value']/r_ccs_cost_bio # Same scaling (same 4% of inv_cost in the end)
Sc_nitro.add_par("fix_cost", df_bio)  
      


# Emission factors (Relation)
 
# Gas
df = Sc_nitro.par("relation_activity")
df_gas = df[df['technology']=='gas_NH3'] # Originally all CO2_cc (truly emitted, bottom-up)
ef_org = df_gas.value.values.copy()
df_gas.value = ef_org * h2_smr_vent_ratio
df_gas = df_gas.assign(technology = df_gas.technology + '_ccs')
Sc_nitro.add_par("relation_activity", df_gas)   

df_gas.value = ef_org * (h2_smr_vent_ratio-1) # Negative
df_gas.relation = 'CO2_Emission'
Sc_nitro.add_par("relation_activity", df_gas)   

# Coal / Oil
df_coal = df[df['technology'].isin(tec_for_ccs[2:])] # Originally all CO2_cc (truly emitted, bottom-up)
ef_org = df_coal.value.values.copy()
df_coal.value = ef_org * h2_coal_vent_ratio
df_coal = df_coal.assign(technology = df_coal.technology + '_ccs')
Sc_nitro.add_par("relation_activity", df_coal)   

df_coal.value = ef_org * (h2_coal_vent_ratio-1) # Negative
df_coal.relation = 'CO2_Emission'
Sc_nitro.add_par("relation_activity", df_coal)   

# Biomass
df_bio = df[df['technology']=='biomass_NH3'] # Originally all CO2.cc (truly emitted, bottom-up)
df_bio.value = biomass_input*(h2_bio_vent_ratio-1)*biomass_ef
df_bio = df_bio.assign(technology = df_bio.technology + '_ccs')
Sc_nitro.add_par("relation_activity", df_bio)   

df_bio.relation = 'CO2_Emission'
Sc_nitro.add_par("relation_activity", df_bio)   


#%% Copy some bgnd parameters (values identical to _NH3 tecs)

par_bgnd_ccs = par_bgnd + ['technical_lifetime', 'capacity_factor', 'var_cost', 'growth_activity_lo']

for t in par_bgnd_ccs:
    df = Sc_nitro.par(t)
    df = df[df['technology'].isin(tec_for_ccs)]
    df['technology'] = df['technology'] + '_ccs'  # Rename the technologies
    Sc_nitro.add_par(t, df)   


#%% Shift model horizon for policy scenarios
    
if run_scen != "baseline":
    Sc_nitro_base = message_ix.Scenario(mp, "JM_GLB_NITRO", "NoPolicy")
    
    import Utilities.shift_model_horizon as shift_year
    if scen_names[run_scen] == "NPi":
        fyear = 2040
    else:
        fyear = 2030
    shift_year(Sc_nitro, Sc_nitro_base, fyear, newtechnames_ccs + newtechnames) 
    

#%% Regional cost calibration 
    
# Scaler based on WEO 2014 (based on IGCC tec)    
scaler_cost = pd.DataFrame(
    {'scaler_std': [1.00,	0.81,	0.96,	0.96,	0.96,	0.88,	0.81,	0.65,	0.42,	0.65,	1.12],
     'scaler_ccs': [1.00,	0.80, 	0.98,	0.92,	0.92,	0.82,	0.80,	0.70,	0.61,	0.70,	1.05], # NA values are replaced with WEO 2014 values. (LAM & MEA = 0.80)
     'node_loc': ['R11_NAM',	
                'R11_LAM',	
                'R11_WEU',	
                'R11_EEU',	
                'R11_FSU',	
                'R11_AFR',	
                'R11_MEA',	
                'R11_SAS',	
                'R11_CPA',	
                'R11_PAS',	
                'R11_PAO']})
    
#tec_scale = (newtechnames + newtechnames_ccs)
tec_scale = [e for e in newtechnames if e not in ('NH3_to_N_fertil', 'electr_NH3')]

# Scale all NH3 tecs in each region with the scaler
for t in tec_scale:    
    for p in ['inv_cost', 'fix_cost', 'var_cost']:
        df = Sc_nitro.par(p, {"technology":t}) 
        temp = df.join(scaler_cost.set_index('node_loc'), on='node_loc')
        df.value = temp.value * temp.scaler_std
        Sc_nitro.add_par(p, df) 

for t in newtechnames_ccs:    
    for p in ['inv_cost', 'fix_cost', 'var_cost']:
        df = Sc_nitro.par(p, {"technology":t}) 
        temp = df.join(scaler_cost.set_index('node_loc'), on='node_loc')
        df.value = temp.value * temp.scaler_ccs
        Sc_nitro.add_par(p, df) 
                
# For CPA and SAS, experiment to make the coal_NH3 cheaper than gas_NH3
scalers = [[0.66*0.91, 1, 0.75*0.9], [0.59, 1, 1]] # f, g, c : Scale based on the original techno-economic assumptions
reg2scale = ['R11_CPA', 'R11_SAS']
for p in ['inv_cost', 'fix_cost']:#, 'var_cost']:
    for r in reg2scale:
        df_c = Sc_nitro.par(p, {"technology":'coal_NH3', "node_loc":r}) 
        df_g = Sc_nitro.par(p, {"technology":'gas_NH3', "node_loc":r})
        df_f = Sc_nitro.par(p, {"technology":'fueloil_NH3', "node_loc":r})
        
        df_cc = Sc_nitro.par(p, {"technology":'coal_NH3_ccs', "node_loc":r}) 
        df_gc = Sc_nitro.par(p, {"technology":'gas_NH3_ccs', "node_loc":r})
        df_fc = Sc_nitro.par(p, {"technology":'fueloil_NH3_ccs', "node_loc":r})

        df_fc.value *= scalers[reg2scale.index(r)][0] # Gas/fueloil cost the same as coal
        df_gc.value *= scalers[reg2scale.index(r)][1]
        df_cc.value *= scalers[reg2scale.index(r)][2]
        
        df_f.value *= scalers[reg2scale.index(r)][0] # Gas/fueloil cost the same as coal
        df_g.value *= scalers[reg2scale.index(r)][1]
        df_c.value *= scalers[reg2scale.index(r)][2]
        
        Sc_nitro.add_par(p, df_g)
        Sc_nitro.add_par(p, df_c)
        Sc_nitro.add_par(p, df_f)
#        Sc_nitro.add_par(p, df_f.append(df_c).append(df_g))
        Sc_nitro.add_par(p, df_gc)
        Sc_nitro.add_par(p, df_cc)
        Sc_nitro.add_par(p, df_fc)
#        Sc_nitro.add_par(p, df_fc.append(df_cc).append(df_gc))


    
    
#%% Solve the model.

Sc_nitro.commit('Nitrogen Fertilizer for Global model - no policy')

start_time = time.time()

#Sc_nitro.to_gdx(r'..\..\message_ix\message_ix\model\data', "MsgData_"+Sc_nitro.model+"_"+
#                Sc_nitro.scenario+"_"+str(Sc_nitro.version))

# I do this because the current set up doesn't recognize the model path correctly.
Sc_nitro.solve(model='MESSAGE', case=Sc_nitro.model+"_"+
                Sc_nitro.scenario+"_"+str(Sc_nitro.version))
#Sc_ref.solve(model='MESSAGE', case=Sc_ref.model+"_"+Sc_ref.scenario+"_"+str(Sc_ref.version))


print(".solve: %.6s seconds taken." % (time.time() - start_time))



#%% Run reference model

#
#start_time = time.time()
#Sc_ref.remove_solution()
#Sc_ref.solve(model='MESSAGE', case=Sc_ref.model+"_"+
#                Sc_ref.scenario+"_"+str(Sc_ref.version))
#print(".solve: %.6s seconds taken." % (time.time() - start_time))
#
