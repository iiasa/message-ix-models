"""
Creating outputs for GLOBIOM input
"""

import ixmp as ix
import message_ix

from message_ix.reporting import Reporter
from pathlib import Path
import pandas as pd

import matplotlib.pyplot as plt
import pyam
import os

os.chdir(r'./code.Global')

#%% Constants

model_name = 'JM_GLB_NITRO_MACRO_TRD'

scen_names = ["baseline",
              "NPi2020-con-prim-dir-ncr",
              "NPi2020_1600-con-prim-dir-ncr",
              "NPi2020_400-con-prim-dir-ncr"]

newtechnames = ['biomass_NH3', 'electr_NH3', 'gas_NH3', 'coal_NH3', 'fueloil_NH3', 'NH3_to_N_fertil']
tec_for_ccs = list(newtechnames[i] for i in [0,2,3,4])
newtechnames_ccs = list(map(lambda x:str(x)+'_ccs', tec_for_ccs)) #include biomass in CCS, newtechnames[2:5]))

# Units are usually taken care of .yaml in message_data.
# In case I don't use message_data for reporting, I need to deal with the units here.
ix.reporting.configure(units={'replace': {'???': '', '-':''}}) # '???' and '-' are not handled in pyint(?)

mp = ix.Platform(dbprops=r'H:\MyDocuments\MESSAGE\message_ix\config\default.org.properties')

def GenerateOutput(model, scen, rep):
    
    rep.set_filters()
    rep.set_filters(t= newtechnames_ccs + newtechnames)

    # 1. Total NF demand
    rep.add_product('useNF', 'land_input', 'LAND')
    
    def collapse(df):
        df['variable'] = 'Nitrogen demand'
        df['unit'] = 'Mt N/yr'
        return df
    
    a = rep.convert_pyam('useNF:n-y', 'y', collapse=collapse)
    rep.write(a[0], Path('nf_demand_'+model+'_'+scen+'.xlsx'))

    """
    'emi' not working with filters on NH3 technologies for now. (This was because of units '???' and '-'.)
    """
    # 2. Total emissions
    rep.set_filters()
    rep.set_filters(t= (newtechnames_ccs + newtechnames)[:-1], e=['CO2_transformation']) # and NH3_to_N_fertil does not have emission factor

    def collapse_emi(df):
        df['variable'] = 'Emissions|CO2|' +df.pop('t')
        df['unit'] = 'Mt CO2'
        return df
    a = rep.convert_pyam('emi:nl-t-ya', 'ya', collapse=collapse_emi)
    rep.write(a[0], Path('nf_emissions_CO2_'+model+'_'+scen+'.xlsx'))


    # 3. Total inputs (incl. final energy) to fertilizer processes
    rep.set_filters()
    rep.set_filters(t= (newtechnames_ccs + newtechnames), c=['coal', 'gas', 'electr', 'biomass', 'fueloil']) 
        
    def collapse_in(df):
        df['variable'] = 'Final energy|'+df.pop('c')
        df['unit'] = 'GWa'
        return df
    a = rep.convert_pyam('in:nl-ya-c', 'ya', collapse=collapse_in)
    rep.write(a[0], Path('nf_input_'+model+'_'+scen+'.xlsx'))   
             
    # 4. Commodity price
    rep.set_filters()
    rep.set_filters(l= ['material_final', 'material_interim'])
    
    def collapse_N(df):
        df.loc[df['c'] == "NH3", 'unit'] = '$/tNH3'
        df.loc[df['c'] == "Fertilizer Use|Nitrogen", 'unit'] = '$/tN'
        df['variable'] = 'Price|' + df.pop('c')
        return df
    
    a = rep.convert_pyam('PRICE_COMMODITY:n-c-y', 'y', collapse=collapse_N)
    rep.write(a[0], Path('price_commodity_'+model+'_'+scen+'.xlsx'))

    # 5. Carbon price
    if scen!="baseline":
        rep.set_filters()
        
        a = rep.convert_pyam('PRICE_EMISSION', 'y')
        rep.write(a[0], Path('price_emission_'+model+'_'+scen+'.xlsx'))


# Generate individual xlsx
for sc in scen_names:
    Sc_ref = message_ix.Scenario(mp, model_name, sc)
    repo = Reporter.from_scenario(Sc_ref)
    GenerateOutput(model_name, sc, repo)
    
# Combine xlsx per each output variable    
for cases in ['nf_demand', 'nf_emissions_CO2', 'nf_input', 'price_commodity', 'price_emission']:
    infiles = []
    for sc in scen_names:
        if sc=="baseline" and cases=='price_emission':
            continue
        infiles.append(pd.read_excel(cases + "_"+ model_name +'_' + sc + ".xlsx"))        
    appended_df = pd.concat(infiles, join='outer', sort=False)
    appended_df.to_excel(cases+"-"+model_name+".xlsx", index=False)
    
    
#%% Generate plots

from pyam.plotting import OUTSIDE_LEGEND

def plot_NF_result(case='nf_demand', model=model_name, scen='baseline'):
    """
        - Generate PNG plots for each case of ['nf_demand', 'nf_emissions_CO2', 'nf_input', 'price_commodity']
        - Data read from the xlsx files created above    
    """        
    if case in ['nf_demand', 'all']:
        data = pyam.IamDataFrame(data='nf_demand'+'_'+model+'_'+scen+'.xlsx', encoding='utf-8')
        fig, ax = plt.subplots(figsize=(12, 12))    
        data.filter(level=0).stack_plot(ax=ax, stack='region')
        plt.savefig('./plots/nf_demand'+'_'+model+'_'+scen+'.png')

    # Pyam currently doesn't allow stack_plot for the next two cases for unknown reasons.
    # (https://github.com/IAMconsortium/pyam/issues/296)    
    if case in ['nf_input', 'all']:    
        data = pyam.IamDataFrame(data='nf_input'+'_'+model+'_'+scen+'.xlsx', encoding='utf-8')
        # aggregate technologies over regions (get global sums)
        for v in list(data.variables()):
            data.aggregate_region(v, append=True)
        
        fig, ax = plt.subplots(figsize=(12, 12))    
#        data.filter(region="World").stack_plot(ax=ax, legend=OUTSIDE_LEGEND['right'])
        data.filter(region="World").line_plot(ax=ax, legend=OUTSIDE_LEGEND['right'])
        plt.savefig('./plots/nf_input'+'_'+model+'_'+scen+'.png')

    if case in ['nf_emissions_CO2', 'all']:    
        data = pyam.IamDataFrame(data='nf_emissions_CO2'+'_'+model+'_'+scen+'.xlsx', encoding='utf-8')
        for v in list(data.variables()):
            data.aggregate_region(v, append=True)
        
        fig, ax = plt.subplots(figsize=(12, 12))    
        data.filter(region="World").line_plot(ax=ax, legend=OUTSIDE_LEGEND['right'])
        plt.savefig('./plots/nf_emissions_CO2'+'_'+model+'_'+scen+'.png')
        
    if case in ['price_commodity', 'all']:
        # N fertilizer
        data = pyam.IamDataFrame(data='price_commodity'+'_'+model+'_'+scen+'.xlsx', encoding='utf-8')
        fig, ax = plt.subplots(figsize=(12, 12))    
        data.filter(variable='Price|Fertilizer Use|*', region='R11_AFR').line_plot(ax=ax, legend=False) # Identical across regoins through trade
        plt.savefig('./plots/price_NF'+'_'+model+'_'+scen+'.png')
        
        #NH3
        fig, ax = plt.subplots(figsize=(12, 12))    
        data.filter(variable='Price|NH3').line_plot(ax=ax, color='region', legend=OUTSIDE_LEGEND['right'])
        plt.savefig('./plots/price_NH3'+'_'+model+'_'+scen+'.png')
    
"""  
scen_names = ["baseline",
              "NPi2020-con-prim-dir-ncr",
              "NPi2020_1600-con-prim-dir-ncr",
              "NPi2020_400-con-prim-dir-ncr"]
"""

cases = ['nf_demand', 'nf_emissions_CO2', 'nf_input', 'price_commodity']

# Individual calls
#plot_NF_result(case='nf_demand', scen='NPi2020_1600-con-prim-dir-ncr')  
#plot_NF_result(case='price_commodity', scen='NPi2020_1600-con-prim-dir-ncr')   
#plot_NF_result(case='nf_input',scen='NPi2020_400-con-prim-dir-ncr')   
#plot_NF_result(case='nf_emissions_CO2',scen='NPi2020_400-con-prim-dir-ncr')   

# call for all plots
for sc in scen_names:
    plot_NF_result(case='all', scen=sc)   



