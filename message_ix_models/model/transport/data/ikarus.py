"""Prepare non-LDV data from the IKARUS model via GEAM_TRP_techinput.xlsx."""
from openpyxl import load_workbook
import pandas as pd
import pint

from message_data.model.transport.common import DATA_PATH
from message_data.tools import ScenarioInfo


FILE = 'GEAM_TRP_techinput.xlsx'

# - Set up a pint.UnitRegistry
u = pint.UnitRegistry()

# Compute message_ix quantities *with* proper unit conversion:

# - define all the units in UnitRegistry: i.e. EUR

# Transport units
u.define('vehicle = [vehicle] = v')
u.define('passenger = [passenger] = p = pass')
u.define('tonne_freight = [tonne_freight] = tf = tonnef')
u.define('vkm = vehicle * kilometer')
u.define('pkm = passenger * kilometer')
u.define('tkm = tonne_freight * kilometer')
u.define('@alias vkm = vkt = v km')
u.define('@alias pkm = pkt = p km')
u.define('@alias tkm = tkt = t km')
# Currencies
u.define('euro_2005 = [currency] = EUR_2005 = €_2005')
# Based on Germany's GDP deflator, data from WorldBank
# https://data.worldbank.org/indicator/
# NY.GDP.DEFL.ZS?end=2015&locations=DE&start=2000
u.define('euro_2000 = 0.94662 * EUR_2005 = EUR_2000 = €_2000'),
# Exchange rate EUR/USD in 2005, data from WorldBank
# https://www.statista.com/statistics/412794/
# euro-to-u-s-dollar-annual-average-exchange-rate/
u.define('dollar_2005 = 1.2435 * EUR_2005 = USD_2005 = $_2005')

# Based on units from excel sheet

# input efficiency (~minp)
# output efficiency (~moutp)
# capacity_factor (~plf)
# technical_lifetime (~pll)
# inv (~inv)
# fixed_cost (~fom)
dict_units = {
    'inv_cost': u('MEUR_2005 / vehicle'),
    'fix_cost': u('kEUR_2005 / vehicle'),
    'var_cost': u('EUR_2005 / hectokilometer / vehicle'),
    'technical_lifetime': u('year'),
    'availability': u('hectokilometer / vehicle / year'),
    'input_electricity': u('GJ / hectokilometer / vehicle'),
    'output': u('1')
}


def get_ikarus_data(scenario):
    # Open *GEAM_TRP_techinput.xlsx* using openpyxl
    wb = load_workbook(DATA_PATH / FILE, read_only=True, data_only=True)
    # Open the 'updateTRPdata' sheet
    sheet = wb['updateTRPdata']
    # Read values from table for e.g. "regional train electric efficient"
    # (= rail_pub)
    data = pd.DataFrame(list(sheet['C103':'I109']),
                        index=dict_units.keys()).applymap(lambda c: c.value). \
                        applymap('{:,.2f}'.format)
    data.columns = [2000, 2005, 2010, 2015, 2020, 2025, 2030]

    # Assign units to rows of *data*
    for idx_col, col in enumerate(data.columns):
        aux = []
        for idx_row, unit in enumerate(dict_units):
            aux.append(data.iloc[idx_row, idx_col] * dict_units[unit])
        data[col] = aux

    return

    # TODO broadcast the data across nodes and years

    s_info = ScenarioInfo(scenario)
    nodes = s_info.N  # list of nodes e.g. for node_loc column of parameters
    years = s_info.Y  # list of years e.g. for year_vtg column of parameters

    # Write the resulting data to temporary files: 1 per parameter.
    wb.save("temp_one_parameter.xlsx")
