'''
Plot issues with crude oil trade (global pool)
'''
import pandas as pd
import plotnine as p9
import numpy as np
import os
import yaml
import requests

from message_ix_models.tools.bilateralize.utils import load_config
from message_ix_models.util import package_data_path

# Plot data
df = pd.read_csv("oil_trade_comparison.csv")
df = df[df['SSP'].isin([1,2,3,4,5])]
df = df[df['YEAR'].isin([2000, 2005, 2010, 2015, 2020, 2025, 
                         2030, 2035, 2040, 2045, 2050, 2055,
                         2060, 2070, 2080, 2090, 2100, 2110])]
df = df.drop_duplicates()
# Plot trade flows
for metric in ['net_exports_MIX', 'net_exports_IEA']:
    gg = (
        p9.ggplot(df[['YEAR', 'node', 'SSP', metric]].drop_duplicates(), p9.aes(x='YEAR', y=metric, fill='node'))
        + p9.geom_bar(stat='identity', width=4.5, color='black', size=0.3)
        + p9.facet_wrap('~SSP')
        + p9.theme_minimal()
        + p9.theme(figure_size=(12, 6))  # width, height in inches
        + p9.labs(x='', y='Net Exports (EJ)', fill='Region')
    )
    gg.save(f"{metric}.png", width=8, height=6, dpi=300)
