# -*- coding: utf-8 -*-
"""
Created on Tue Sep 24 14:36:18 2019

@author: min
"""
import numpy as np
import pandas as pd
import time

import ixmp as ix
import message_ix

import importlib
#import LoadParams
importlib.reload(LoadParams)

#%% Base model load

# launch the IX modeling platform using the local default database                                                                                                                       
mp = ix.Platform(dbprops=r'H:\MyDocuments\MESSAGE\message_ix\config\default.org.properties')

# new model name in ix platform
modelName = "JM_GLB_NITRO"
basescenarioName =  "NoPolicy" 
newscenarioName = "NoPolicy_Trd" 

comment = "MESSAGE global test for new representation of nitrogen cycle with global trade"

Sc_nitro = message_ix.Scenario(mp, modelName, basescenarioName)

#%% Clone the model

Sc_nitro_trd = Sc_nitro.clone(modelName, newscenarioName, comment)

Sc_nitro_trd.remove_solution()
Sc_nitro_trd.check_out()

#%% Add tecs to set
# Only fertilizer traded for now (NH3 trade data not yet available)
comm_for_trd = ['Fertilizer Use|Nitrogen'] 
lvl_for_trd = ['material_final'] 
newtechnames_trd = ['NFert_trd']
newtechnames_imp = ['NFert_imp']
newtechnames_exp = ['NFert_exp']

#comm_for_trd = comm_for_trd # = ['NH3', 'Fertilizer Use|Nitrogen'] 
#newtechnames_trd = ['NH3_trd', 'NFert_trd']
#newtechnames_imp = ['NH3_imp', 'NFert_imp']
#newtechnames_exp = ['NH3_exp', 'NFert_exp']

Sc_nitro_trd.add_set("technology", newtechnames_trd + newtechnames_imp + newtechnames_exp)

cat_add = pd.DataFrame({'type_tec': ['import', 'export'],  # 'all' not need to be added here
                        'technology': newtechnames_imp + newtechnames_exp})

Sc_nitro_trd.add_set("cat_tec", cat_add)

#%% input & output

for t in newtechnames_trd:
    # output
    df = Sc_nitro_trd.par("output", {"technology":["coal_trd"]}) 
    df['technology'] = t
    df['commodity'] = comm_for_trd[newtechnames_trd.index(t)]
    df['value'] = 1
    df['unit'] = 'Tg N/yr'
    Sc_nitro_trd.add_par("output", df.copy()) 
    
    df = Sc_nitro_trd.par("input", {"technology":["coal_trd"]}) 
    df['technology'] = t
    df['commodity'] = comm_for_trd[newtechnames_trd.index(t)]
    df['value'] = 1
    df['unit'] = 'Tg N/yr'
    Sc_nitro_trd.add_par("input", df.copy()) 
    
reg = REGIONS.copy()
reg.remove('R11_GLB')

for t in newtechnames_imp:
    # output
    df = Sc_nitro_trd.par("output", {"technology":["coal_imp"], "node_loc":['R11_CPA']}) 
    df['technology'] = t
    df['commodity'] = comm_for_trd[newtechnames_imp.index(t)]
    df['value'] = 1
    df['unit'] = 'Tg N/yr'
    df['level'] = lvl_for_trd[newtechnames_imp.index(t)]
    for r in reg:
        df['node_loc'] = r    
        df['node_dest'] = r     
        Sc_nitro_trd.add_par("output", df.copy()) 
        
    # input
    df = Sc_nitro_trd.par("input", {"technology":["coal_imp"], "node_loc":['R11_CPA']}) 
    df['technology'] = t
    df['commodity'] = comm_for_trd[newtechnames_imp.index(t)]
    df['value'] = 1
    df['unit'] = 'Tg N/yr'
    for r in reg:
        df['node_loc'] = r 
        Sc_nitro_trd.add_par("input", df.copy())    
    
for t in newtechnames_exp:
    # output
    df = Sc_nitro_trd.par("output", {"technology":["coal_exp"], "node_loc":['R11_CPA']}) 
    df['technology'] = t
    df['commodity'] = comm_for_trd[newtechnames_exp.index(t)]
    df['value'] = 1
    df['unit'] = 'Tg N/yr'
    for r in reg:
        df['node_loc'] = r    
        Sc_nitro_trd.add_par("output", df.copy()) 
        
    # input
    df = Sc_nitro_trd.par("input", {"technology":["coal_exp"], "node_loc":['R11_CPA']}) 
    df['technology'] = t
    df['commodity'] = comm_for_trd[newtechnames_exp.index(t)]
    df['value'] = 1
    df['unit'] = 'Tg N/yr'
    df['level'] = lvl_for_trd[newtechnames_exp.index(t)]
    for r in reg:
        df['node_loc'] = r 
        df['node_origin'] = r    
        Sc_nitro_trd.add_par("input", df.copy())         

# Need to incorporate the regional trade pattern

#%% Cost
    
for t in newtechnames_exp:
    df = Sc_nitro_trd.par("inv_cost", {"technology":["coal_exp"]}) 
    df['technology'] = t
    Sc_nitro_trd.add_par("inv_cost", df) 
    
    df = Sc_nitro_trd.par("var_cost", {"technology":["coal_exp"]}) 
    df['technology'] = t
    Sc_nitro_trd.add_par("var_cost", df) 
    
    df = Sc_nitro_trd.par("fix_cost", {"technology":["coal_exp"]}) 
    df['technology'] = t
    Sc_nitro_trd.add_par("fix_cost", df) 
      
for t in newtechnames_imp:
#   No inv_cost for importing tecs
#    df = Sc_nitro_trd.par("inv_cost", {"technology":["coal_imp"]}) 
#    df['technology'] = t
#    Sc_nitro_trd.add_par("inv_cost", df) 
    
    df = Sc_nitro_trd.par("var_cost", {"technology":["coal_imp"]}) 
    df['technology'] = t
    Sc_nitro_trd.add_par("var_cost", df) 
    
    df = Sc_nitro_trd.par("fix_cost", {"technology":["coal_imp"]}) 
    df['technology'] = t
    Sc_nitro_trd.add_par("fix_cost", df) 
    
for t in newtechnames_trd:    
    df = Sc_nitro_trd.par("var_cost", {"technology":["coal_trd"]}) 
    df['technology'] = t
    Sc_nitro_trd.add_par("var_cost", df) 
            

    
#%% Other background variables
    
paramList_tec = [x for x in Sc_nitro_trd.par_list() if 'technology' in Sc_nitro_trd.idx_sets(x)]
#paramList_comm = [x for x in Sc_nitro.par_list() if 'commodity' in Sc_nitro.idx_sets(x)]

def get_params_with_tech(scen, name):
    result = []
    # Iterate over all parameters with a tech dimension
    for par_name in paramList_tec:
        if len(scen.par(par_name, filters={'technology': name})):
            # Parameter has >= 1 data point with tech *name*
            result.append(par_name)
    return result

params_exp = get_params_with_tech(Sc_nitro_trd, 'coal_exp')
params_imp = get_params_with_tech(Sc_nitro_trd, 'coal_imp')
params_trd = get_params_with_tech(Sc_nitro_trd, 'coal_trd')

# Got too slow for some reason
#params_exp = [x for x in paramList_tec if 'coal_exp' in set(Sc_nitro_trd.par(x)["technology"].tolist())]
#params_imp = [x for x in paramList_tec if 'coal_imp' in set(Sc_nitro_trd.par(x)["technology"].tolist())]
#params_trd = [x for x in paramList_tec if 'coal_trd' in set(Sc_nitro_trd.par(x)["technology"].tolist())]

a = set(params_exp + params_imp + params_trd) 
suffix = ('cost', 'put')
prefix = ('historical', 'ref', 'relation')
a = a - set([x for x in a if x.endswith(suffix)] + [x for x in a if x.startswith(prefix)] + ['emission_factor'])

for p in list(a):
    for t in newtechnames_exp:
        df = Sc_nitro_trd.par(p, {"technology":["coal_exp"]}) 
        df['technology'] = t
        if df.size:
            Sc_nitro_trd.add_par(p, df.copy())
                      
    for t in newtechnames_imp:
        df = Sc_nitro_trd.par(p, {"technology":["coal_imp"]}) 
        df['technology'] = t
        if df.size:
            Sc_nitro_trd.add_par(p, df.copy())
        
    for t in newtechnames_trd:
        df = Sc_nitro_trd.par(p, {"technology":["coal_trd"]}) 
        df['technology'] = t
        if df.size:
            Sc_nitro_trd.add_par(p, df.copy())
        
# Found coal_exp doesn't have full cells filled for technical_lifetime.
for t in newtechnames_exp:
    df = Sc_nitro_trd.par('technical_lifetime', {"technology":t, "node_loc":['R11_CPA']}) 
    for r in reg:
        df['node_loc'] = r   
        Sc_nitro_trd.add_par("technical_lifetime", df.copy())       


#%% Histrorical trade activity
# Export capacity - understood as infrastructure enabling the trade activity (port, rail etc.)
        
# historical_activity     
N_trade = LoadParams.N_trade_R11.copy()      
  
df_histexp = N_trade.loc[(N_trade.Element=='Export') & (N_trade.year_act<2015),]        
df_histexp = df_histexp.assign(mode = 'M1') 
df_histexp = df_histexp.assign(technology = newtechnames_exp[0]) #t
df_histexp = df_histexp.drop(columns="Element")

df_histimp = N_trade.loc[(N_trade.Element=='Import') & (N_trade.year_act<2015),]    
df_histimp = df_histimp.assign(mode = 'M1') 
df_histimp = df_histimp.assign(technology = newtechnames_imp[0]) #t
df_histimp = df_histimp.drop(columns="Element")

# GLB trd historical activities (Now equal to the sum of imports)
dftrd = Sc_nitro_trd.par("historical_activity", {"technology":["coal_trd"]})
dftrd = dftrd.loc[(dftrd.year_act<2015) & (dftrd.year_act>2000),]
dftrd.value = df_histimp.groupby(['year_act']).sum().values
dftrd['unit'] = 'Tg N/yr'
dftrd['technology'] = newtechnames_trd[0]
Sc_nitro_trd.add_par("historical_activity", dftrd)
Sc_nitro_trd.add_par("historical_activity", df_histexp.append(df_histimp))

# historical_new_capacity
trdlife = Sc_nitro_trd.par("technical_lifetime", {"technology":["NFert_exp"]}).value[0]

df_histcap = df_histexp.drop(columns=['time', 'mode'])
df_histcap = df_histcap.rename(columns={"year_act":"year_vtg"})
df_histcap.value = df_histcap.value.values/trdlife

Sc_nitro_trd.add_par("historical_new_capacity", df_histcap)


#%%

# Adjust i_feed demand 
#demand_fs_org is the original i-feed in the reference SSP2 scenario
"""
In the scenario w/o trade, I assumed 100% NH3 demand is produced in each region.
Now with trade, I subtract the net import (of tN) from total demand, and the difference is produced regionally.
Still this does not include NH3 trade, so will not match the historical NH3 regional production.
Also, historical global production exceeds the global demand level from SSP2 scenario for the same year. 
I currently ignore this excess production and only produce what is demanded.
"""
df = demand_fs_org.loc[demand_fs_org.year==2010,:].join(LoadParams.N_feed.set_index('node'), on='node')
sh = pd.DataFrame( {'node': demand_fs_org.loc[demand_fs_org.year==2010, 'node'], 
                    'r_feed': df.totENE / df.value})    # share of NH3 energy among total feedstock (no trade assumed)
df = demand_fs_org.join(sh.set_index('node'), on='node')
df.value *= 1 - df.r_feed # Carve out the same % from tot i_feed values
df = df.drop('r_feed', axis=1)
Sc_nitro_trd.add_par("demand", df)



#%% Solve the scenario

Sc_nitro_trd.commit('Nitrogen Fertilizer for global model with fertilizer trade via global pool')

start_time = time.time()

Sc_nitro_trd.solve(model='MESSAGE', case=Sc_nitro_trd.model+"_"+
                Sc_nitro_trd.scenario + "_" + str(Sc_nitro_trd.version))

print(".solve: %.6s seconds taken." % (time.time() - start_time))

#%% utils
#Sc_nitro_trd.discard_changes()

#Sc_nitro_trd = message_ix.Scenario(mp, modelName, newscenarioName)


