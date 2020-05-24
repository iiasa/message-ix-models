# -*- coding: utf-8 -*-
"""
Created on Tue Aug  6 11:17:06 2019

@author: min
"""

import time
import pandas as pd
        
        
#%% Add climate policies

                                                                                           
mp = ix.Platform(dbprops=r'H:\MyDocuments\MESSAGE\message_ix\config\default.properties')



#%%

# new model name in ix platform
modelName = "JM_GLB_NITRO"
basescenarioName = "Baseline" # CCS now merged to the Baseline
newscenarioName = "EmBound" # '2degreeC' # 

comment = "MESSAGE_Global test for new representation of nitrogen cycle with climate policy"

Sc_nitro = message_ix.Scenario(mp, modelName, basescenarioName)


#%% Clone
Sc_nitro_2C = Sc_nitro.clone(modelName, newscenarioName, comment)

lasthistyear = 2020 # New last historical year
df_ACT = Sc_nitro_2C.var('ACT', {'year_act':lasthistyear}).groupby(['node_loc', 'technology', 'year_act']).sum().reset_index()
df_CAP_NEW = Sc_nitro_2C.var('CAP_NEW', {'year_vtg':lasthistyear}).groupby(['node_loc', 'technology', 'year_vtg']).sum().reset_index()
df_EXT = Sc_nitro_2C.var('EXT', {'year':lasthistyear}).groupby(['node', 'commodity', 'grade', 'year']).sum().reset_index()

Sc_nitro_2C.remove_solution()
Sc_nitro_2C.check_out()

#%% Put global emissions bound 
bound = 5000 #15000 #
bound_emissions_2C = {
    'node': 'World',
    'type_emission': 'TCE',
    'type_tec': 'all',
    'type_year' : 'cumulative', #'2050', #
    'value': bound, #1076.0, # 1990 and on
    'unit' : 'tC',
}
     
df = pd.DataFrame(bound_emissions_2C, index=[0])
        
Sc_nitro_2C.add_par("bound_emission", df) 

#%% Change first modeling year (dealing with the 5-year step scenario)

df = Sc_nitro_2C.set('cat_year')
fyear = 2030 #2025
a = df[((df.type_year=="cumulative") & (df.year<fyear)) | ((df.type_year=="firstmodelyear") & (df.year<fyear))]

df.loc[df.type_year=='firstmodelyear', 'year'] = fyear
Sc_nitro_2C.add_set("cat_year", df) 
Sc_nitro_2C.remove_set("cat_year", a) 

#%% Filling in history for 2020

lasthistyear_org = 2010 #2015
# historical_activity
df = Sc_nitro_2C.par('historical_activity', {'year_act':lasthistyear_org})
#df = df[df.value > 0]
a = pd.merge(df[['node_loc', 'technology', 'mode', 'time', 'unit']], 
             df_ACT[['node_loc', 'technology', 'year_act', 'lvl']], how='left', on=['node_loc', 'technology']).rename(columns={'lvl':'value'})
a = a[a.value > 0]
a['unit'] = 'GWa'
a['year_act'] = a['year_act'].astype(int)
Sc_nitro_2C.add_par("historical_activity", a) 

# historical_new_capacity
df = Sc_nitro_2C.par('historical_new_capacity', {'year_vtg':lasthistyear_org})
#df = df[df.value > 0]
a = pd.merge(df[['node_loc', 'technology', 'unit']], 
             df_CAP_NEW[['node_loc', 'technology', 'year_vtg', 'lvl']], how='left', on=['node_loc', 'technology']).rename(columns={'lvl':'value'})
a = a[a.value > 0]
a['year_vtg'] = a['year_vtg'].astype(int)
Sc_nitro_2C.add_par("historical_new_capacity", a) 

# historical_extraction
df = Sc_nitro_2C.par('historical_extraction', {'year':lasthistyear_org})
#df = df[df.value > 0]
a = pd.merge(df[['node', 'commodity', 'grade', 'unit']], 
             df_EXT[['node', 'commodity', 'grade', 'year', 'lvl']], how='outer', on=['node', 'commodity', 'grade']).rename(columns={'lvl':'value'})
a = a[a.value > 0]
a['unit'] = 'GWa'
a['year'] = a['year'].astype(int)
Sc_nitro_2C.add_par("historical_extraction", a) 

# historical_land
df = Sc_nitro_2C.par('historical_land', {'year':lasthistyear_org})
df['year'] = lasthistyear
Sc_nitro_2C.add_par("historical_land", df) 

# historical_gdp
#Sc_INDC = mp.Scenario("CD_Links_SSP2", "INDCi_1000-con-prim-dir-ncr")
#df = Sc_nitro.par('historical_gdp')
#df = df[df.year < fyear]
#Sc_nitro_2C.add_par("historical_gdp", df) 


#%%

#Sc_nitro_2C.commit('Hydro AllReg-Global w/ 2C constraint (starting 2020)')
Sc_nitro_2C.commit('Fertilizer Global w/ 2C constraint (starting 2030)')

# to_gdx only
#start_time = time.time()
#Sc_nitro_2C.to_gdx(r'..\..\message_ix\message_ix\model\data', "MsgData_"+Sc_nitro_2C.model+"_"+
#                Sc_nitro_2C.scenario+"_"+str(Sc_nitro_2C.version))
#print(".to_gdx: %s seconds taken." % (time.time() - start_time))

# solve
start_time = time.time()
Sc_nitro_2C.solve(model='MESSAGE', case=Sc_nitro_2C.model+"_"+
#                Sc_nitro_2C.scenario+"_"+str(Sc_nitro_2C.version))
                Sc_nitro_2C.scenario+"_"+str(bound))

print(".solve: %.6s seconds taken." % (time.time() - start_time))


#%%
rep = Reporter.from_scenario(Sc_nitro_2C)

# Set up filters for N tecs
rep.set_filters(t= newtechnames_ccs + newtechnames + ['NFert_imp', 'NFert_exp', 'NFert_trd'])

# NF demand summary
NF = rep.add_product('useNF', 'land_input', 'LAND')

print(rep.describe(rep.full_key('useNF')))
rep.get('useNF:n-y')
rep.write('useNF:n-y', 'nf_demand_'+str(bound)+'_notrade.xlsx')
rep.write('useNF:y', 'nf_demand_total_'+str(bound)+'_notrade.xlsx')
