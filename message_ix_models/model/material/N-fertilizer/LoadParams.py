# -*- coding: utf-8 -*-
"""
Created on Tue Jun  4 12:01:47 2019

@author: min

Load parameters for the new technologies
"""

import pandas as pd

# Read parameters in xlsx
te_params = pd.read_excel(r'..\n-fertilizer_techno-economic.xlsx', sheet_name='Sheet1')
n_inputs_per_tech = 12 # Number of input params per technology

inv_cost = te_params[2010][list(range(0, te_params.shape[0], n_inputs_per_tech))].reset_index(drop=True)
fix_cost = te_params[2010][list(range(1, te_params.shape[0], n_inputs_per_tech))].reset_index(drop=True)
var_cost = te_params[2010][list(range(2, te_params.shape[0], n_inputs_per_tech))].reset_index(drop=True)
technical_lifetime = te_params[2010][list(range(3, te_params.shape[0], n_inputs_per_tech))].reset_index(drop=True)
input_fuel = te_params[2010][list(range(4, te_params.shape[0], n_inputs_per_tech))].reset_index(drop=True)
input_fuel[0:5] = input_fuel[0:5] * 0.0317 # 0.0317 GWa/PJ, GJ/t = PJ/Mt NH3
input_elec = te_params[2010][list(range(5, te_params.shape[0], n_inputs_per_tech))].reset_index(drop=True)
input_elec = input_elec * 0.0317 # 0.0317 GWa/PJ
input_water = te_params[2010][list(range(6, te_params.shape[0], n_inputs_per_tech))].reset_index(drop=True)
output_NH3 = te_params[2010][list(range(7, te_params.shape[0], n_inputs_per_tech))].reset_index(drop=True)
output_water = te_params[2010][list(range(8, te_params.shape[0], n_inputs_per_tech))].reset_index(drop=True)
output_heat = te_params[2010][list(range(9, te_params.shape[0], n_inputs_per_tech))].reset_index(drop=True)
output_heat = output_heat * 0.0317 # 0.0317 GWa/PJ
emissions = te_params[2010][list(range(10, te_params.shape[0], n_inputs_per_tech))].reset_index(drop=True) * 12 / 44 # CO2 to C
capacity_factor = te_params[2010][list(range(11, te_params.shape[0], n_inputs_per_tech))].reset_index(drop=True)


#%% Demand scenario [Mt N/year] from GLOBIOM
# N_demand_FSU = pd.read_excel(r'..\CD-Links SSP2 N-fertilizer demand.FSU.xlsx', sheet_name='data', nrows=3)
N_demand_GLO = pd.read_excel(r'..\CD-Links SSP2 N-fertilizer demand.Global.xlsx', sheet_name='data')

# NH3 feedstock share by region in 2010 (from http://ietd.iipnetwork.org/content/ammonia#benchmarks)
feedshare_GLO = pd.read_excel(r'..\Ammonia feedstock share.Global.xlsx', sheet_name='Sheet2', skiprows=13)

# Regional N demaand in 2010
ND = N_demand_GLO.loc[N_demand_GLO.Scenario=="NoPolicy", ['Region', 2010]]
ND = ND[ND.Region!='World']
ND.Region = 'R11_' + ND.Region
ND = ND.set_index('Region')

# Derive total energy (GWa) of NH3 production (based on demand 2010)
N_energy = feedshare_GLO[feedshare_GLO.Region!='R11_GLB'].join(ND, on='Region')
N_energy = pd.concat([N_energy.Region, N_energy[["gas_pct", "coal_pct", "oil_pct"]].multiply(N_energy[2010], axis="index")], axis=1)
N_energy.gas_pct *= input_fuel[2] * 17/14   # NH3 / N
N_energy.coal_pct *= input_fuel[3] * 17/14
N_energy.oil_pct *= input_fuel[4] * 17/14
N_energy = pd.concat([N_energy.Region, N_energy.sum(axis=1)], axis=1).rename(columns={0:'totENE', 'Region':'node'}) #GWa


#%% Import trade data (from FAO)

N_trade_R14 = pd.read_csv(r'..\trade.FAO.R14.csv', index_col=0)

N_trade_R11 = pd.read_csv(r'..\trade.FAO.R11.csv', index_col=0)
N_trade_R11.msgregion = 'R11_' + N_trade_R11.msgregion
N_trade_R11.Value = N_trade_R11.Value/1e6
N_trade_R11.Unit = 'Tg N/yr'
N_trade_R11 = N_trade_R11.assign(time = 'year')
N_trade_R11 = N_trade_R11.rename(columns={"Value":"value", "Unit":"unit", "msgregion":"node_loc", "Year":"year_act"})

df = N_trade_R11.loc[N_trade_R11.year_act==2010,]
df = df.pivot(index='node_loc', columns='Element', values='value')
NP = pd.DataFrame({'netimp': df.Import - df.Export,
                   'demand': ND[2010]})
NP['prod'] = NP.demand - NP.netimp

# Derive total energy (GWa) of NH3 production (based on demand 2010)
N_feed = feedshare_GLO[feedshare_GLO.Region!='R11_GLB'].join(NP, on='Region')
N_feed = pd.concat([N_feed.Region, N_feed[["gas_pct", "coal_pct", "oil_pct"]].multiply(N_feed['prod'], axis="index")], axis=1)
N_feed.gas_pct *= input_fuel[2] * 17/14
N_feed.coal_pct *= input_fuel[3] * 17/14
N_feed.oil_pct *= input_fuel[4] * 17/14
N_feed = pd.concat([N_feed.Region, N_feed.sum(axis=1)], axis=1).rename(columns={0:'totENE', 'Region':'node'}) #GWa
