"""Import/recreate LDV data from spreadsheet calculations."""
from openpyxl import load_workbook
import pandas as pd

from message_data.model.transport.common import DATA_PATH, UNITS


FILE = 'LDV_costs_efficiencies_US-TIMES_MA3T.xlsx'

units = {
    'efficiency': UNITS('10^9 v km / GW / year'),
    'inv_cost': UNITS('USD / vehicle'),
    'fix_cost': UNITS('USD / vehicle'),
}


def get_ldv_data(scenario):
    wb = load_workbook(DATA_PATH / FILE, read_only=True, data_only=True)

    # Efficiency table
    unit = UNITS('10^9 v km / GW / year')

    sheet = wb['MESSAGE_LDV_nam']
    data = pd.DataFrame(sheet['B3':'Q15']) \
             .applymap(lambda c: c.value)
    data.columns = data.loc[0, :]
    data = data.iloc[1:, :] \
               .drop('Description', axis=1) \
               .melt(id_vars=data.columns[:2], var_name='year') \
               .astype({'year': int})

    return data
