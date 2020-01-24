"""Prepare non-LDV data from the IKARUS model via GEAM_TRP_techinput.xlsx."""
import warnings

from openpyxl import load_workbook
import pandas as pd
import pint

from message_data.model.transport.common import DATA_PATH
from message_data.tools import ScenarioInfo


FILE = 'GEAM_TRP_techinput.xlsx'

# Catch a warning from pint 0.10
with warnings.catch_warnings():
    warnings.simplefilter('ignore')
    pint.Quantity([])

# Set up a pint.UnitRegistry
UNITS = pint.UnitRegistry()

# Compute message_ix quantities *with* proper unit conversion:

# Define all the units in UnitRegistry: i.e. EUR

# Transport units
UNITS.define("""vehicle = [vehicle] = v
passenger = [passenger] = p = pass
tonne_freight = [tonne_freight] = tf = tonnef
vkm = vehicle * kilometer
pkm = passenger * kilometer
tkm = tonne_freight * kilometer
@alias vkm = vkt = v km
@alias pkm = pkt = p km
@alias tkm = tkt = t km""")

# Currencies
# - EUR_2000: Based on Germany's GDP deflator, data from WorldBank
#   https://data.worldbank.org/indicator/
#   NY.GDP.DEFL.ZS?end=2015&locations=DE&start=2000
# - USD_2005: Exchange rate EUR/USD in 2005, data from WorldBank
#   https://www.statista.com/statistics/412794/
#   euro-to-u-s-dollar-annual-average-exchange-rate/

UNITS.define("""EUR_2005 = [currency] = €_2005
EUR_2000 = 0.94662 * EUR_2005 = €_2000
USD_2005 = 1.2435 * EUR_2005 = $_2005""")

# Based on units from excel sheet

# input efficiency (~minp)
# output efficiency (~moutp)
# capacity_factor (~plf)
# technical_lifetime (~pll)
# inv (~inv)
# fixed_cost (~fom)
params = {
    'inv_cost': UNITS('MEUR_2005 / vehicle'),
    'fix_cost': UNITS('kEUR_2005 / vehicle'),
    'var_cost': UNITS('EUR_2005 / hectokilometer'),
    'technical_lifetime': UNITS('year'),
    'availability': UNITS('hectokilometer / vehicle / year'),
    'input_electricity': UNITS('GJ / hectokilometer'),
    'output': UNITS('1')
}


def get_ikarus_data(scenario):
    """Read IKARUS data from GEAM_TRP_techinput.xlsx and conform to *scenario*.

    .. todo:: Extend for additional technologies.
    """
    # Open *GEAM_TRP_techinput.xlsx* using openpyxl
    wb = load_workbook(DATA_PATH / FILE, read_only=True, data_only=True)

    # Open the 'updateTRPdata' sheet
    sheet = wb['updateTRPdata']

    # Manually found all the starting and final cells delimiting tables in sheet
    cell_slices = {
        'rail_pub': {'C103', 'I109'},
        'dMspeed_rai': {'C125', 'I131'},
        'Mspeed_rai': {'C147', 'I153'},
        'Hspeed_rai': {'C169', 'I175'},
        'con_ar': {'C179', 'I185'},
        # Same parametrization as *con_ar* (see GEAM_TRP_techinput.xlsx):
        'conm_ar': {'C179', 'I185'},
        'conE_ar': {'C179', 'I185'},
        'conh_ar': {'C179', 'I185'},

        'ICE_M_bus': {'C197', 'I203'},
        'ICE_H_bus': {'C205', 'I211'},
        'ICG_bus': {'C213', 'I219'},
        # Same parametrization as *ICG_bus*. Conversion factors will be applied:
        'ICAe_bus': {'C213', 'I219'},
        'ICH_bus': {'C213', 'I219'},
        'PHEV_bus': {'C213', 'I219'},
        'FC_bus': {'C213', 'I219'},
        # Equivalent to *FC_bus*:
        'FCg_bus': {'C213', 'I219'},
        'FCm_bus': {'C213', 'I219'},

        'Trolley_bus': {'C229', 'I235'}
    }

    # Initialize empty DataFrame to concatenate techs
    # indexes = [cell_slices.keys(), params.keys()]
    data = pd.DataFrame()

    for non_LDV_tech, table in cell_slices.items():
        # - Read values from table for e.g. "regional train electric efficient"
        #   (= rail_pub).
        # - Extract the value from each cell object, sorted alphabetically.
        # - Define a multi-level index for wrapping
        # - Transpose so that each variable is in one column.
        cells = slice(*sorted(table))
        index = pd.MultiIndex.from_product([[non_LDV_tech],
                                list(params.keys())], names=['tech', 'params'])
        data_one_tech = pd.DataFrame(list(sheet[cells]), index=index,
            columns=[2000, 2005, 2010, 2015, 2020, 2025, 2030]) \
            .applymap(lambda c: c.value).transpose()

        # Set all non numeric values to NaNs:
        for col in data_one_tech:
            data_one_tech[col] = pd.to_numeric(data_one_tech[col],
                                              errors='coerce')

        # Assign units to each column
        for label, unit in params.items():
            data_one_tech[non_LDV_tech, label] = data_one_tech[non_LDV_tech,
                                                label].apply(lambda v: v * unit)

        data = pd.concat([data, data_one_tech], axis=1)

    # TODO broadcast the data across nodes and years
    if scenario is not None:
        s_info = ScenarioInfo(scenario)
        nodes = s_info.N  # list of nodes e.g. for node_loc column of parameters
        years = s_info.Y  # list of years e.g. for year_vtg column of parameters

    # TODO write the resulting data to temporary files: 1 per parameter.
    NEW_FILE = 'non_LDV_techs_wrapped.csv'
    data.to_csv(DATA_PATH / NEW_FILE)

    return data
