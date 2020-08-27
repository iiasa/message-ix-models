# -*- coding: utf-8 -*-
"""
Created on Wed Aug 26 14:43:41 2020
Generate techno economic aluminum data based on aluminum_techno_economic.xlsx 

@author: unlu
"""
import pandas as pd
import numpy as np
from collections import defaultdict
from message_data.tools import broadcast, make_df, same_node
import message_ix
import ixmp


def gen_data_aluminum(file):
    
    mp = ixmp.Platform()
    
    scenario = message_ix.Scenario(mp,"MESSAGE_material","baseline", cache=True)

    data_aluminum = pd.read_excel(file,sheet_name="data")
    
    data_aluminum_hist = pd.read_excel("aluminum_techno_economic.xlsx",
                                   sheet_name="data_historical",
                                   usecols = "A:F")
    # Clean the data
    # Drop columns that don't contain useful information
    data_aluminum= data_aluminum.drop(['Region', 'Source', 'Description'], 
                                      axis = 1)
    # List of data frames, to be concatenated together at the end
    results = defaultdict(list) 
    # Will come from the yaml file 
    technology_add = data_aluminum["technology"].unique()
    # normally from s_info.Y
    years = [2010,2020,2030,2040,2050,2060,2070,2080,2090,2100]
    
    # normally from s_info.N
    nodes = ["CHN"]
    
    # Iterate over technologies 

    for t in technology_add: 
        # Obtain the active and vintage years 
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
            mode="standard",
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

    results_aluminum = {par_name: pd.concat(dfs) for par_name, 
                        dfs in results.items()}
 
    return results_aluminum,  data_aluminum_hist 


