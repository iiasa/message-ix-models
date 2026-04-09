# Historical calibration of steel trade

import ixmp
import message_ix
import pandas as pd
import numpy as np
import os

from message_ix_models.util import package_data_path
from message_ix_models.project.bilateral_steel.steel_yearbook import *
from message_ix_models.tools.bilateralize.utils import load_config

# Import scenario and models
config, config_path = load_config(project_name = 'bilateral_steel', config_name = 'config.yaml')
models_scenarios = config['models_scenarios']
data_path = package_data_path("bilateralize")

# Historical calibration for shipped steel
p_drive = config["p_drive_location"]
steel_yearbook_path = os.path.join(p_drive, "MESSAGE_trade", "Steel", "Steel-Statistical-Yearbook-2025.pdf")

sy_exp = pd.read_excel(os.path.join(p_drive, "MESSAGE_trade", "Steel", "Table_29_Steel_Exports.xlsx"), skiprows = 2)
sy_exp = sy_exp.rename(columns={'Country / Region': 'Country'})
sy_exp['Flow'] = 'Export'

sy_imp = pd.read_excel(os.path.join(p_drive, "MESSAGE_trade", "Steel", "Table_30_Steel_Imports.xlsx"), skiprows = 2)
sy_imp = sy_imp.rename(columns={'Country / Region': 'Country'})
sy_imp['Flow'] = 'Import'

sy_all = pd.concat([sy_exp, sy_imp])
sy_all['Source'] = 'Steel Statistical Yearbook 2025'
sy_all = gen_r12_data(sy_all)
for y in ['2015', '2020', '2024']:
    sy_all[y] = sy_all[y].replace(r'^\s*\.\.\.\s*$', 0, regex=True)
    sy_all[y] = sy_all[y].astype(float)

sy_all = sy_all.groupby(['R12', 'Flow', 'Source'])[['2015', '2020', '2024']].sum().reset_index()
sy_out = pd.DataFrame()
for y in ['2015', '2020', '2024']:
    ydf = sy_all[['R12', 'Flow', y]]
    ydf = ydf.rename(columns={y: 'Statistical Yearbook'})
    ydf['Year'] = int(y)
    sy_out = pd.concat([sy_out, ydf])

sy_out['Statistical Yearbook'] = sy_out['Statistical Yearbook']/1000

# Collect from MESSAGEix scenario
mp = ixmp.Platform()
steel_scenario = message_ix.Scenario(mp, model = "steel_trade", scenario = "only_steel")

hist_activity = steel_scenario.par('historical_activity')
hist_activity = hist_activity[hist_activity['technology'].str.contains('steel_shipped_exp')]
hist_activity['Exporter'] = hist_activity['node_loc']
hist_activity['Importer'] = 'R12_' + hist_activity['technology'].str.replace('steel_shipped_exp_', '').str.upper()
hist_activity['Year'] = hist_activity['year_act']

me_exp = hist_activity.groupby(['Exporter', 'Year'])['value'].sum().reset_index()
me_exp = me_exp.rename(columns={'value': 'MESSAGEix', 'Exporter': 'R12'})
me_exp['Flow'] = 'Export'

me_imp = hist_activity.groupby(['Importer', 'Year'])['value'].sum().reset_index()
me_imp = me_imp.rename(columns={'value': 'MESSAGEix', 'Importer': 'R12'})
me_imp['Flow'] = 'Import'

me_all = pd.concat([me_exp, me_imp])

# Output
outdf = pd.merge(me_all, sy_out, on=['R12', 'Year', 'Flow'], how='outer')