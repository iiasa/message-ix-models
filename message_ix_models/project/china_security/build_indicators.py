"""Build indicators for China security scenarios"""

import ixmp
import message_ix
import message_data
import pandas as pd
import numpy as np

from message_ix_models.util import package_data_path
from message_ix_models.project.china_security.indicators.imports import import_indicators
from message_ix_models.project.china_security.indicators.hhi_energy import calculate_energy_hhi
from message_ix_models.project.china_security.indicators.hhi_trade import calculate_trade_hhi

# Import dataframes
basedf = pd.DataFrame()
for scen in ["SSP2"]:
    basedf_s = pd.read_csv(package_data_path("china_security", 'reporting', 'output', f"weu_security_{scen}.csv"))
    basedf = pd.concat([basedf, basedf_s])

# Make long
year_cols = ['2030', '2035', '2040', '2045', '2050', '2055', '2060',
             '2070', '2080', '2090', '2100', '2110']
basedf = basedf.melt(
    id_vars=['Model', 'Scenario', 'Variable', 'Unit', 'Region'],
    value_vars=year_cols,
    var_name='Year',
    value_name='Value'
)
basedf['Year'] = basedf['Year'].astype(int)

# Region names need to be updated
basedf['Region'] = np.where(basedf['Region'].str.contains('>') == False, "R12_" + basedf['Region'], basedf['Region'])

# Run indicators
pe_trade = import_indicators(input_data = basedf,
                             region = "R12_CHN",
                             portfolio = ["Biomass", "Coal", "Gas", "Oil"],
                             portfolio_level = "Primary Energy",
                             use_units = "EJ/yr")
se_trade = import_indicators(input_data = basedf,
                             region = "R12_CHN",
                             portfolio = ["Ethanol", "Fuel Oil", "Light Oil"],
                             portfolio_level = "Secondary Energy",
                             use_units = "EJ/yr")

pe_hhi = calculate_energy_hhi(input_data = basedf,
                              region = "R12_China",
                              portfolio_level = "Primary Energy",
                              use_units = "EJ/yr")
se_hhi = calculate_energy_hhi(input_data = basedf,
                              region = "R12_China",
                              portfolio_level = "Secondary Energy",
                              use_units = "EJ/yr")

pe_trade_hhi = calculate_trade_hhi(input_data = basedf,
                                   trade_type = "Exports",
                                   portfolio = "Primary Energy",
                                   region = "R12_China",
                                   use_units = "EJ/yr")
