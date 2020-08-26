# -*- coding: utf-8 -*-
"""
Created on Wed Aug 26 14:54:24 2020
Generate techno economic generic furncaedata based on 
generic_furnace_boiler_techno_economic.xlsx

@author: unlu

"""
import pandas as pd
import numpy as np
from collections import defaultdict
from message_data.tools import broadcast, make_df, same_node
import message_ix
import ixmp

def gen_data_generic():
    
    mp = ixmp.Platform()
    
    scenario = message_ix.Scenario(mp,"MESSAGE_material","baseline", cache=True)
    
    data_generic = pd.read_excel("generic_furnace_boiler_techno_economic.xlsx", 
                                 sheet_name="generic")
    data_generic= data_generic.drop(['Region', 'Source', 'Description'], 
                                    axis = 1)
    
    # List of data frames, to be concatenated together at the end
    results = defaultdict(list)
    
    # This comes from the config file normally 
    technology_add = ['furnace_coal_aluminum', 'furnace_foil_aluminum', 
                      'furnace_methanol_aluminum', 'furnace_biomass_aluminum', 
                      'furnace_ethanol_aluminum', 'furnace_gas_aluminum', 
                      'furnace_elec_aluminum', 'furnace_h2_aluminum', 
                      'hp_gas_aluminum', 'hp_elec_aluminum', 'fc_h2_aluminum',
                      'solar_aluminum','dheat_aluminum',"furnace_loil_aluminum"]
    
    # normally from s_info.Y
    years = [2010,2020,2030,2040,2050,2060,2070,2080,2090,2100]
    
    # normally from s_info.N
    nodes = ["CHN"]
    
    # For each technology there are differnet input and output combinations 
    # Iterate over technologies 
    
    for t in technology_add: 
        
        # Obtain the active and vintage years 
        av = data_generic.loc[(data_generic["technology"] == t),
                              'availability'].values[0]
        lifetime = data_generic.loc[(data_generic["technology"] == t) \
                                    & (data_generic["parameter"]== 
                                       "technical_lifetime"),'value'].values[0]
        years_df = scenario.vintage_and_active_years()
        years_df = years_df.loc[years_df["year_vtg"]>= av]
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
        
        params = data_generic.loc[(data_generic["technology"] == t),
                                  "parameter"].values.tolist()
      
        # To keep the available modes and use it in the emission_factor 
        # parameter. 
        mode_list = []
    
        # Iterate over parameters 
        for par in params:
            
            split = par.split("|")
            param_name = par.split("|")[0]
            
            val = data_generic.loc[((data_generic["technology"] == t) 
            & (data_generic["parameter"] == par)), 'value'].values[0]
    
            # Common parameters for all input and output tables 
            common = dict(
            year_vtg= vintage_years,
            year_act = act_years,
            time="year",
            time_origin="year",
            time_dest="year",)     
            
            if len(split)> 1: 
       
                if (param_name == "input")|(param_name == "output"):
                
                    com = split[1]
                    lev = split[2]
                    mod = split[3]
                    
                    # Keep the available modes for a technology in a list 
                    mode_list.append(mod)  
                    df = (make_df(param_name, technology=t, commodity=com, 
                                  level=lev,mode=mod, value=val, unit='t', 
                                  **common).pipe(broadcast, node_loc=nodes).
                                            pipe(same_node))
                    results[param_name].append(df)   
                    
                elif param_name == "emission_factor":
                    emi = split[1]
                    mod = data_generic.loc[((data_generic["technology"] == t) 
                    & (data_generic["parameter"] == par)), 'value'].values[0]
            
                    # For differnet modes 
                    for m in np.unique(np.array(mode_list)):
    
                        df = (make_df(param_name, technology=t,value=val,
                                      emission=emi,mode= m, unit='t', 
                                      **common).pipe(broadcast, node_loc=nodes))
                        results[param_name].append(df)   
            
            # Rest of the parameters apart from inpput, output and 
            # emission_factor
            else: 
                df = (make_df(param_name, technology=t, value=val,unit='t', 
                              **common).pipe(broadcast, node_loc=nodes))
                results[param_name].append(df)  

    results_generic = {par_name: pd.concat(dfs) for par_name, 
                       dfs in results.items()}
    return results_generic 
            