"""Build indicators for China security scenarios"""

import pandas as pd
import numpy as np

from message_ix_models.util import package_data_path
from message_ix_models.project.china_security.indicators.imports import import_indicators
from message_ix_models.project.china_security.indicators.hhi_energy import calculate_energy_hhi
from message_ix_models.project.china_security.indicators.hhi_trade import calculate_trade_hhi
from message_ix_models.project.china_security.indicators.fossil_share import calculate_fossil_share
from message_ix_models.project.china_security.indicators.sdi_final_energy import calculate_sector_sdi
from message_ix_models.project.china_security.indicators.exp_over_gdp import exp_over_gdp

# Import dataframes
basedf = pd.DataFrame()
for scen in ["SSP2_Baseline", "SSP2_2C", "LED_2C"]:
    basedf_s = pd.read_csv(package_data_path("china_security", 'reporting', 'output', f"china_security_{scen}.csv"))
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
                             portfolio_level = "Primary Energy")
se_trade = import_indicators(input_data = basedf,
                             region = "R12_CHN",
                             portfolio = ["Ethanol", "Fuel Oil", "Light Oil"],
                             portfolio_level = "Secondary Energy")

pe_hhi = calculate_energy_hhi(input_data = basedf,
                              region = "R12_CHN",
                              portfolio_level = "Primary Energy",
                              fuel_level = 1,
                              fuel_subset = ["Biomass", "Coal", "Gas", "Geothermal", "Hydro",
                                             "Nuclear", "Ocean", "Oil", "Other", 
                                             "Solar", "Wind"])

elec_hhi = calculate_energy_hhi(input_data = basedf,
                                region = "R12_CHN",
                                portfolio_level = "Secondary Energy",
                                secondary_energy_type = "Electricity",
                                fuel_level = 2,
                                fuel_subset = ["Biomass", "Coal", "Gas", "Geothermal", "Hydro",
                                               "Nuclear", "Oil", "Other", "Solar", "Wind"])

pe_trade_hhi = calculate_trade_hhi(input_data = basedf,
                                   trade_type = "Imports",
                                   portfolio = "Primary Energy",
                                   region = "R12_CHN",
                                   use_units = "EJ/yr")

fe_sector_sdi = calculate_sector_sdi(input_data = basedf,
                                     region = "R12_CHN")

pe_fossil_share = calculate_fossil_share(input_data = basedf,
                                         region = "R12_CHN")

exp_gdp = exp_over_gdp(input_data = basedf,
                       region = "R12_CHN")

