# Compile full plotting dataset

from tkinter.constants import W
import pandas as pd
from message_ix_models.util import package_data_path
import matplotlib.pyplot as plt
import numpy as np
import yaml

# Full scenario list
scenario_list = ['SSP2', 'FSU2040', 'FSU2100',
                 'SSP2_NAMboost', 'FSU2040_NAMboost', 'FSU2100_NAMboost',
                 'SSP2_MEACON', 'FSU2040_MEACON', 'FSU2100_MEACON']

scenario_order = {'SSP2': 1, 'FSU2040': 2, 'FSU2100': 3,
                  'SSP2_NAMboost': 4, 'FSU2040_NAMboost': 5, 'FSU2100_NAMboost': 6,
                  'SSP2_MEACON': 7, 'FSU2040_MEACON': 8, 'FSU2100_MEACON': 9}

# Full year list
year_list = [2030, 2035, 2040, 2045, 2050, 2055, 2060, 2070, 2080, 2090, 2100]

# Import scenario_data
wdf = pd.DataFrame()
for scenario in scenario_list:
    indf = pd.read_csv(package_data_path('bilateralize', 'reporting', 'output', f'weu_security_{scenario}.csv'))
    wdf = pd.concat([wdf, indf])

# Pivot long
df = pd.DataFrame()
for y in year_list:
    ydf = wdf[['Model', 'Scenario', 'Region', 'Variable', 'Unit', str(y)]]
    ydf = ydf.rename(columns = {str(y): 'Value'})
    ydf['Year'] = y
    ydf = ydf[['Model', 'Scenario', 'Region', 'Variable', 'Unit', 'Year', 'Value']]
    df = pd.concat([df, ydf])

# Add label
df['Scenario Order'] = df['Scenario'].map(scenario_order) 
df['Scenario Order'] = df['Scenario Order'].astype(str) + ' ' + df['Scenario']

# Export
df.to_csv(package_data_path('weu_security', 'reporting', 'full_data.csv'), index=False)