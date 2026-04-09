"""Reporting workflow for WEU security scenarios"""

import ixmp
import message_ix
import pandas as pd
import numpy as np

from message_ix_models.util import package_data_path
from message_ix_models.tools.bilateralize.reporting.trade_reporting import trade_reporting

# Call all scenarios for analysis
mp = ixmp.Platform()

trade_reporting(mp, scenario = message_ix.Scenario(mp, model = "steel_trade", scenario = "only_steel"),
                out_dir = package_data_path("bilateral_steel", "reporting", "output", "trade"),
                include_segments = ['material'])

trade_reporting(mp, scenario = message_ix.Scenario(mp, model = "SSP_SSP2_v6.4", scenario = "baseline"),
                out_dir = package_data_path("bilateral_steel", "reporting", "output", "trade"),
                include_segments = ['material'])

gpdf = pd.read_csv(package_data_path("bilateral_steel", "reporting", "output", "trade", "SSP_SSP2_v6.4_baseline.csv"))
gpdf['Region'] = np.where(gpdf['Variable'].str.contains('Imports'), 'R12_GLB>' + gpdf['Region'], gpdf['Region'])

bidf = pd.read_csv(package_data_path("bilateral_steel", "reporting", "output", "trade", "steel_trade_only_steel.csv"))
bidf = bidf[bidf['Region'].str.contains(">")]
bidf = bidf[bidf['Region'].str.contains('R12_GLB') == False]

df = pd.concat([gpdf, bidf])
df['Exporter'] = df['Region'].str.split('>').str[0]
df['Importer'] = df['Region'].str.split('>').str[1]

# Make long
outdf = pd.DataFrame()
for y in ['2010', '2015', '2020', '2025', '2030', '2035', '2040', '2045', '2050', '2055', '2060', '2070', '2080', '2090', '2100', '2110']:
    ydf = df[['Model', 'Scenario', 'Region', 'Variable', 'Unit', 'Exporter', 'Importer', y]]
    ydf = ydf.rename(columns={y: 'Value'})
    ydf['Year'] = int(y)
    outdf = pd.concat([outdf, ydf])

outdf.to_csv(package_data_path("bilateral_steel", "reporting", "output", "trade", "steel_trade_combined.csv"), index=False)