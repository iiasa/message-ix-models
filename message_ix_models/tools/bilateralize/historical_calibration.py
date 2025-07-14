# -*- coding: utf-8 -*-
"""
Historical Calibration
"""
# Import packages
import os
import sys
import pandas as pd
import logging
import yaml
import message_ix
import ixmp
import itertools
import numpy as np
import pickle

from pathlib import Path
from message_ix_models.util import package_data_path
from message_ix_models.tools.iea import web

# Data paths
data_path = os.path.join("P:", "ene.model", "MESSAGE_Trade")
baci_path = os.path.join(data_path, "UN Comtrade", "BACI")
iea_path = os.path.join(data_path, "IEA")
iea_web_path = os.path.join(iea_path, "WEB2025")

# Reimport large files?
reimport_IEA = False
reimport_BACI = False

# Dictionaries of ISO - IEA - MESSAGE Regions
def generate_cfdict(message_regions):
    
    dict_dir = package_data_path("bilateralize", message_regions + '_node_list.yaml')
    with open(dict_dir, "r") as f:
        dict_message_regions = yaml.safe_load(f) 
    region_list = [i for i in list(dict_message_regions.keys()) if i != 'World']
    
    print('Import conversion factors')  
    cfdf = pd.read_csv(os.path.join(iea_web_path, "CONV.txt"),
                      sep='\s+', header=None,
                      encoding='windows-1252')
    cfdf.columns = ['units', 'country', 'commodity', 'metric', 'year', 'value']
    cfdf = cfdf[cfdf['year'] > 1990]
    cfdf = cfdf[cfdf['units'] == 'KJKG'] #KJ/KG
    cfdf = cfdf[cfdf['metric'].isin(['NAVERAGE', 'NINDPROD'])] #Average NCV and NCV of production
    cfdf = cfdf[cfdf['value'] != 'x']
    
    cfdf['conversion (TJ/t)'] = (cfdf['value'].astype(float) * 1000)*(1e-9)
    
    cf_out = cfdf.groupby(['country', 'commodity'])['conversion (TJ/t)'].mean().reset_index()
    
    # Link ISO codes
    cf_cw = pd.read_csv(os.path.join(iea_web_path, "CONV_country_codes.csv"))
    cf_out = cf_out.merge(cf_cw, left_on = 'country', right_on = 'IEA COUNTRY', how = 'inner')
    
    cf_out[message_regions + '_REGION'] = ""
    for k in region_list:
        if "child" in dict_message_regions[k].keys():
            cf_out[message_regions + '_REGION'] = np.where(cf_out['ISO'].isin(dict_message_regions[k]['child']),
                                                           k, cf_out[message_regions + '_REGION'])
    
    print('Collapse conversion factors to REGION level')
    cf_region = cf_out.groupby([message_regions + '_REGION', 'commodity'])['conversion (TJ/t)'].mean().reset_index()
    
    print('Collapse conversion factors to FUEL level')
    cf_fuel = cf_out.groupby(['commodity'])['conversion (TJ/t)'].mean().reset_index()
    
    print('Clean up ISO level data')
    cf_iso = cf_out[['ISO', 'R12_REGION', 'commodity', 'conversion (TJ/t)']].copy()
    
    print('Save conversion factors')
    cf_region.to_csv(os.path.join(iea_web_path, "conv_by_region.csv"))
    cf_fuel.to_csv(os.path.join(iea_web_path, "conv_by_fuel.csv"))
    cf_iso.to_csv(os.path.join(iea_web_path, "conv_by_iso.csv"))
    
    print('Full dictionaries')
    full_dict = {message_regions: cf_region,
                 'fuel': cf_fuel,
                 'ISO': cf_iso}
    
    picklepath = os.path.join(iea_web_path,"conversion_factors.pickle")
    with open(picklepath, 'wb') as f:
        pickle.dump(full_dict, f)
    
# Import UN Comtrade data and link to conversion factors
# This does not include natural gas pipelines or LNG, which are from IEA
def import_uncomtrade(update_year = 2023):
    
    dict_dir = package_data_path("bilateralize", 'commodity_codes.yaml')
    with open(dict_dir, "r") as f:
        commodity_codes = yaml.safe_load(f) 
    
    full_hs_list = []
    for c in commodity_codes.keys():
        full_hs_list = full_hs_list + commodity_codes[c]['HS']
        
    print('Build BACI')     
    df = pd.DataFrame()
    for y in list(range(2005, update_year, 1)):
        print('Importing BACI' + str(y))
        ydf = pd.read_csv(os.path.join(baci_path, 
                                       "BACI_HS92_Y" + str(y) + "_V202401b.csv"),
                          encoding='windows-1252')
        ydf['k'] = ydf['k'].astype(str).str.zfill(6)
        ydf['hs4'] = ydf['k'].str[0:4]
        ydf['hs5'] = ydf['k'].str[0:5]
        ydf['hs6'] = ydf['k'].str[0:6]
        ydf = ydf[(ydf['hs4'].isin(full_hs_list))|(ydf['hs5'].isin(full_hs_list))|(ydf['hs6'].isin(full_hs_list))]
        df = pd.concat([df, ydf])
    
    print('Save pickle')
    picklepath = os.path.join(baci_path, "full_2005-2022.pickle")
    with open(picklepath, 'wb') as f:
        pickle.dump(df, f)

    df['MESSAGE Commodity'] = ''
    for c in commodity_codes.keys():
        df['MESSAGE Commodity'] = np.where((df['hs4'].isin(commodity_codes[c]['HS']))|\
                                           (df['hs5'].isin(commodity_codes[c]['HS']))|\
                                           (df['hs6'].isin(commodity_codes[c]['HS'])),
                                           commodity_codes[c]['MESSAGE Commodity'], df['MESSAGE Commodity'])     
    
    countrycw =  pd.read_csv(os.path.join(baci_path, "country_codes_V202401b.csv"))
    df = df.merge(countrycw[['country_code', 'country_iso3']], left_on = 'i', right_on = 'country_code', how = 'left')    
    df = df.rename(columns = {'country_iso3': 'i_iso3'})
    df = df.merge(countrycw[['country_code', 'country_iso3']], left_on = 'j', right_on = 'country_code', how = 'left')    
    df = df.rename(columns = {'country_iso3': 'j_iso3'})
    df = df[['t', 'i', 'j',  'i_iso3', 'j_iso3', 'k', 'MESSAGE Commodity', 'v', 'q']]
       
    df.to_csv(os.path.join(baci_path, "shortenedBACI.csv"))

# Convert trade values
def convert_trade(message_regions,
                  conversion_factors_loc = iea_web_path):
    
    df = pd.read_csv(os.path.join(baci_path, "shortenedBACI.csv"))
    
    with open(os.path.join(conversion_factors_loc, "conversion_factors.pickle"), 'rb') as f:
       conversion_factors = pickle.load(f)
    with open(os.path.join(conversion_factors_loc, "CONV_addl.yaml"), 'r') as f:
        conversion_addl = yaml.safe_load(f)
    cf_codes = pd.read_csv(os.path.join(conversion_factors_loc, "CONV_hs.csv"))
    
    df['k'] = df['k'].astype(str)
    cf_codes['HS'] = cf_codes['HS'].astype(str)
    
    df['HS'] = ''
    for hs in [i for i in cf_codes['HS'] if len(i) == 4]: # 4 digit HS
        df['HS'] = np.where(df['k'].str[0:4] == hs, hs, df['HS'])
    df['HS'] = np.where(df['HS'] == '', df['k'], df['HS'])   
    
    # Add MESSAGE regions
    dict_dir = package_data_path("bilateralize", message_regions + '_node_list.yaml')
    with open(dict_dir, "r") as f:
        dict_message_regions = yaml.safe_load(f) 
    region_list = [i for i in list(dict_message_regions.keys()) if i != 'World']

    df['MESSAGE Region'] = ''
    for r in region_list:
        df['MESSAGE Region'] = np.where(df['i_iso3'].isin(dict_message_regions[r]['child']),
                                        r, df['MESSAGE Region'])
    # Add IEA conversion factors
    df = df.merge(cf_codes,
                  left_on = 'HS', right_on = 'HS', how = 'left')

    df = df.merge(conversion_factors['ISO'][['ISO', 'commodity', 'conversion (TJ/t)']],
                  left_on = ['i_iso3', 'IEA CONV Commodity'],
                  right_on = ['ISO', 'commodity'],
                  how = 'left')
    df = df.rename(columns = {'conversion (TJ/t)': 'ISO conversion (TJ/t)'})
    
    df = df.merge(conversion_factors[message_regions],
                  left_on = ['MESSAGE Region', 'IEA CONV Commodity'],
                  right_on = [message_regions + '_REGION', 'commodity'],
                  how = 'left')
    df = df.rename(columns = {'conversion (TJ/t)': 'Region conversion (TJ/t)'})
    
    df = df.merge(conversion_factors['fuel'],
                  left_on = ['IEA CONV Commodity'],
                  right_on = ['commodity'],
                  how = 'left')
    df = df.rename(columns = {'conversion (TJ/t)': 'Fuel conversion (TJ/t)'})
    
    df['conversion (TJ/t)'] = df['ISO conversion (TJ/t)']
    df['conversion (TJ/t)'] = np.where(df['conversion (TJ/t)'].isnull(),
                                       df['Region conversion (TJ/t)'],
                                       df['conversion (TJ/t)'])
    df['conversion (TJ/t)'] = df['ISO conversion (TJ/t)']
    df['conversion (TJ/t)'] = np.where(df['conversion (TJ/t)'].isnull(),
                                       df['Fuel conversion (TJ/t)'],
                                       df['conversion (TJ/t)'])   
    
    df = df[['t', 'i', 'j', 'i_iso3', 'j_iso3', 'k', 'MESSAGE Commodity',
             'v', 'q',
             'conversion (TJ/t)']]
    
    # Add additional conversion factors if missing 
    for f in conversion_addl.keys():
        df['conversion (TJ/t)'] = np.where((df['conversion (TJ/t)'].isnull()) & (df['MESSAGE Commodity'] == f),
                                           conversion_addl[f],
                                           df['conversion (TJ/t)'])
        
    # Convert to energy units 
    df['conversion (TJ/t)'] = df['conversion (TJ/t)'].astype(float)
    
    df = df.rename(columns = {'t': 'YEAR',
                              'i_iso3': 'EXPORTER',
                              'j_iso3': 'IMPORTER',
                              'k': 'HS',
                              'v': 'VALUE (1000USD)',
                              'q': 'WEIGHT (t)',
                              'MESSAGE Commodity': 'MESSAGE COMMODITY'})
    df = df[df['WEIGHT (t)'].str.contains('NA') == False]
    df['WEIGHT (t)'] = df['WEIGHT (t)'].astype(float)
    df['ENERGY (TJ)'] = df['WEIGHT (t)'] * df['conversion (TJ/t)']

    df = df[['YEAR', 'EXPORTER', 'IMPORTER', 'HS', 'MESSAGE COMMODITY', 'ENERGY (TJ)']]
    
    return df

# Import IEA for LNG and pipeline gas
def import_iea_gas():
    ngd = pd.read_csv(os.path.join(iea_path, 'NATGAS', 'WIMPDAT.txt'), sep = '\s+')
    ngd.columns = ["YEAR", "PRODUCT", "IMPORTER", "EXPORTER", "VALUE"]   
    
    ngd = ngd[ngd['YEAR'] > 1989] # Keep after 1990 only
    ngd = ngd[ngd['PRODUCT'].isin(['LNGTJ', 'PIPETJ'])] # Keep only TJ values

    ngd['ENERGY (TJ)'] = np.where(ngd['VALUE'].isin(['..', 'x','c']), np.nan, ngd['VALUE'])
    ngd['ENERGY (TJ)'] = ngd['ENERGY (TJ)'].astype(float)

    ngd['MESSAGE COMMODITY'] = ''
    ngd['MESSAGE COMMODITY'] = np.where(ngd['PRODUCT'] == 'LNGTJ', 'lng', ngd['MESSAGE COMMODITY'])
    ngd['MESSAGE COMMODITY'] = np.where(ngd['PRODUCT'] == 'PIPETJ', 'gas_piped', ngd['MESSAGE COMMODITY'])
    
    cf_cw = pd.read_csv(os.path.join(iea_web_path, "CONV_country_codes.csv"))
    for t in ['EXPORTER', 'IMPORTER']:
        ngd = ngd.merge(cf_cw, left_on = t, right_on = 'IEA COUNTRY', how = 'left')
        ngd[t] = ngd['ISO']
        ngd = ngd.drop(['ISO', 'IEA COUNTRY'], axis = 1)
        ngd = ngd[ngd[t].isnull() == False]

    ngd = ngd[['YEAR', 'EXPORTER', 'IMPORTER', 'MESSAGE COMMODITY', 'ENERGY (TJ)']]
    
    ngd = ngd.groupby(['YEAR', 'EXPORTER', 'IMPORTER', 'MESSAGE COMMODITY'])['ENERGY (TJ)'].sum().reset_index()
    
    return ngd
    
# Check against IEA balances
def import_iea_balances():
    ieadf1 = pd.read_csv(os.path.join(iea_web_path, "EARLYBIG1.txt"),
                         sep='\s+', header=None)
    ieadf2 = pd.read_csv(os.path.join(iea_web_path, "EARLYBIG2.txt"),
                         sep='\s+', header=None)
    
    ieadf = pd.concat([ieadf1, ieadf2])
    ieadf.columns = ['region', 'fuel', 'year', 'flow', 'unit', 'value', 'statisticalerror']
    
    iea_out = pd.DataFrame()
    for t in ['EXPORTS', 'IMPORTS']:
        tdf = ieadf[ieadf['flow'] == t].copy()
        tdf = tdf[tdf['unit'] == 'TJ']
        
        tdf = tdf[['region', 'fuel', 'year', 'flow', 'unit', 'value']]
        tdf = tdf.rename(columns = {'region': 'REGION',
                                    'fuel': 'IEA-WEB COMMODITY',
                                    'year': 'YEAR',
                                    'flow': 'FLOW',
                                    'unit': 'IEA-WEB UNIT',
                                    'value': 'IEA-WEB VALUE'})
        iea_out = pd.concat([iea_out, tdf])
    
    iea_out.to_csv(os.path.join(iea_web_path, "WEB_TRADEFLOWS.csv"))
    
def check_iea_balances(indf):
    
    iea = pd.read_csv(os.path.join(iea_web_path, "WEB_TRADEFLOWS.csv"))
    ieacw = pd.read_csv(os.path.join(iea_web_path, "country_crosswalk.csv"))
    iea = iea.merge(ieacw, left_on = 'REGION', right_on = 'REGION', how = 'left')
    iea['IEA-WEB VALUE'] = np.where(iea['FLOW'] == 'EXPORTS', iea['IEA-WEB VALUE'] * -1, iea['IEA-WEB VALUE'])
        
    indf = indf[indf['MESSAGE COMMODITY'].isin(['gas_piped', 'lng']) == False].copy() # LNG and pipe gas are directly from IEA
    
    dict_dir = package_data_path("bilateralize", 'commodity_codes.yaml')
    with open(dict_dir, "r") as f:
        commodity_codes = yaml.safe_load(f)
    
    iea['COMMODITY'] = ''; indf['COMMODITY'] = ''
    for c in commodity_codes.keys():
        iea['COMMODITY'] = np.where(iea['IEA-WEB COMMODITY'].isin(commodity_codes[c]['IEA-WEB']), c, iea['COMMODITY'])
        indf['COMMODITY'] = np.where(indf['MESSAGE COMMODITY'] == commodity_codes[c]['MESSAGE Commodity'], c, indf['COMMODITY'])
        
    exports = indf.groupby(['YEAR', 'EXPORTER', 'COMMODITY'])['ENERGY (TJ)'].sum().reset_index()
    imports = indf.groupby(['YEAR', 'IMPORTER', 'COMMODITY'])['ENERGY (TJ)'].sum().reset_index()

    exports = exports.merge(iea[iea['FLOW'] == 'EXPORTS'][['ISO', 'COMMODITY', 'YEAR', 'IEA-WEB UNIT', 'IEA-WEB VALUE']], 
                            left_on = ['YEAR', 'EXPORTER', 'COMMODITY'],
                            right_on = ['YEAR', 'ISO', 'COMMODITY'], 
                            how = 'left')
    imports = imports.merge(iea[iea['FLOW'] == 'IMPORTS'][['ISO', 'COMMODITY', 'YEAR', 'IEA-WEB UNIT', 'IEA-WEB VALUE']], 
                            left_on = ['YEAR', 'IMPORTER', 'COMMODITY'],
                            right_on = ['YEAR', 'ISO', 'COMMODITY'], 
                            how = 'left')
    
    exports['DIFFERENCE'] = (exports['ENERGY (TJ)'] - exports['IEA-WEB VALUE'])/exports['IEA-WEB VALUE']
    imports['DIFFERENCE'] = (imports['ENERGY (TJ)'] - imports['IEA-WEB VALUE'])/imports['IEA-WEB VALUE']

    outdir = package_data_path("bilateralize")
    exports.to_csv(os.path.join(outdir, 'diagnostics', 'iea_calibration_exports.csv'))
    imports.to_csv(os.path.join(outdir, 'diagnostics', 'iea_calibration_imports.csv'))
    
    
# Aggregate UN Comtrade data to MESSAGE regions and set up historical activity parameter dataframe
def reformat_to_parameter(indf, message_regions):

    dict_dir = package_data_path("bilateralize", message_regions + '_node_list.yaml')
    with open(dict_dir, "r") as f:
        dict_message_regions = yaml.safe_load(f) 
    region_list = [i for i in list(dict_message_regions.keys()) if i != 'World']
    
    indf['EXPORTER REGION'] = ''; indf['IMPORTER REGION'] = ''
    for t in ['EXPORTER', 'IMPORTER']:
        for r in region_list:
            indf[t + ' REGION'] = np.where(indf[t].isin(dict_message_regions[r]['child']),
                                               r, indf[t + ' REGION'])

    # Collapse to regional level
    indf = indf.groupby(['YEAR', 'EXPORTER REGION', 'IMPORTER REGION', 'MESSAGE COMMODITY'])['ENERGY (TJ)'].sum().reset_index()
    indf = indf[(indf['EXPORTER REGION'] != '') & (indf['IMPORTER REGION'] != '')]
    indf = indf[indf['EXPORTER REGION'] != indf['IMPORTER REGION']]
    
    # Add MESSAGE columns for exports
    exdf = indf.copy()
    exdf['node_loc'] = exdf['EXPORTER REGION']
    exdf['node_importer'] = exdf['IMPORTER REGION'].str.replace(message_regions + '_', '').str.lower()
    
    exdf['technology'] = exdf['MESSAGE COMMODITY'] + '_exp_' + exdf['node_importer']
    exdf['year_act'] = exdf['YEAR']
    exdf['mode'] = 'M1'
    exdf['time'] = 'year'
    exdf['unit'] = 'GWa'
    
    exdf['value'] = exdf['ENERGY (TJ)'] * (3.1712 * 1e-5) # TJ to GWa
    
    exdf = exdf[['node_loc', 'technology', 'year_act', 'mode', 'time', 'value', 'unit']]
    
    # Add MESSAGE columns for imports
    imdf = indf.copy()
    imdf['node_loc'] = imdf['IMPORTER REGION']    
    imdf['technology'] = imdf['MESSAGE COMMODITY'] + '_imp'
    imdf['year_act'] = imdf['YEAR']
    imdf['mode'] = 'M1'
    imdf['time'] = 'year'
    imdf['unit'] = 'GWa'
    imdf['value'] = imdf['ENERGY (TJ)'] * (3.1712 * 1e-5) # TJ to GWa
    
    imdf = imdf[['node_loc', 'technology', 'year_act', 'mode', 'time', 'value', 'unit']]
    
    outdf = pd.concat([exdf, imdf])
    
    return outdf

# Run all
def build_historical_activity(message_regions = 'R12'):
        
    if reimport_IEA == True:
        generate_cfdict(message_regions = message_regions)    
        import_iea_balances()
    if reimport_BACI == True:
        import_uncomtrade()
        
    bacidf = convert_trade(message_regions = message_regions)
    bacidf = bacidf[bacidf['MESSAGE COMMODITY'] != 'lng'] # Get LNG from IEA instead

    ngdf = import_iea_gas()

    tradedf = bacidf.merge(ngdf, 
                           left_on = ['YEAR', 'EXPORTER', 'IMPORTER', 'MESSAGE COMMODITY'],
                           right_on = ['YEAR', 'EXPORTER', 'IMPORTER', 'MESSAGE COMMODITY'],
                           how = 'outer')
    tradedf['ENERGY (TJ)'] = tradedf['ENERGY (TJ)_x']
    tradedf['ENERGY (TJ)'] = np.where(tradedf['MESSAGE COMMODITY'].isin(['lng', 'gas_piped']),
                                      tradedf['ENERGY (TJ)_y'], tradedf['ENERGY (TJ)'])
    tradedf['ENERGY (TJ)'] = tradedf['ENERGY (TJ)'].astype(float)
    tradedf = tradedf[['YEAR', 'EXPORTER', 'IMPORTER', 'HS', 'MESSAGE COMMODITY', 'ENERGY (TJ)']].reset_index()

    check_iea_balances(indf = tradedf)

    outdf = reformat_to_parameter(indf = tradedf, 
                                  message_regions = message_regions)
    
    return outdf