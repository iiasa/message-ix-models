'''
Plot issues with crude oil trade (global pool)
'''
import pandas as pd
import plotnine
import numpy as np
import os
import yaml
import requests

from message_ix_models.tools.bilateralize.utils import load_config
from message_ix_models.util import package_data_path

# Plot data
df = pd.read_csv("oil_trade_comparison.csv")

# Plot trade flows
for metric in ['net_exports_MIX', 'net_exports_IEA']:
    gg = (
        ggplot(df, aes(x = 'YEAR', y = 'net_exports_MIX', color = 'node''))
        + geom_bar(stat = 'identity')
        + facet_wrap('~SSP')
        + theme_minimal()
        + labs(x = '', y = 'Net Exports (EJ)', color = 'Region')
    )
    gg.save("net_exports_MIX.png")

for metric in ['exports_MIX', 'imports_MIX', 'exports_IEA', 'imports_IEA']:
    gg = (
        ggplot(df, aes(x = 'YEAR', y = metric, color = 'node''))
        + geom_bar(stat = 'identity')
        + facet_wrap('~SSP')
        + theme_minimal()
        + labs(x = '', y = "EJ", color = 'Region')
    )
    gg.save(f"{metric}.png")