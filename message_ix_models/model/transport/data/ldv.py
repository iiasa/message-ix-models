"""Import/recreate LDV data from spreadsheet calculations."""
from openpyxl import load_workbook
import pandas as pd

from message_data.model.transport.common import DATA_PATH, UNITS
from message_data.tools import ScenarioInfo


FILE = 'LDV_costs_efficiencies_US-TIMES_MA3T.xlsx'

TABLES = [
    ('efficiency', slice('B3', 'Q15'), UNITS('10^9 v km / GW h / year')),
    ('inv_cost', slice('B33', 'Q45'), UNITS('USD / vehicle')),
    ('fix_cost', slice('B62', 'Q74'), UNITS('USD / vehicle')),
]


def get_ldv_data(scenario):
    s_info = ScenarioInfo(scenario)

    wb = load_workbook(DATA_PATH / FILE, read_only=True, data_only=True)

    # Tables
    dfs = []

    # Iterate over regions/nodes
    for node in s_info.N:
        if node == 'World':
            continue
        else:
            sheet_node = node.split('_')[-1].lower()

        # Worksheet for this region
        sheet = wb[f'MESSAGE_LDV_{sheet_node}']

        # Read tables for efficiency, investment, and fixed O&M cost
        # NB fix_cost varies by distance driven, thus this is the value for
        #    average driving.
        # TODO calculate the values for modest and frequent driving
        # TODO these values are calculated; transfer the calculations to code
        for name, cells, unit in TABLES:
            df = pd.DataFrame(sheet[cells]) \
                     .applymap(lambda c: c.value)

            # Make the first row the headers
            df.columns = df.loc[0, :]

            # Remaining rows: rearrange columns, period as column.
            df = df.iloc[1:, :] \
                   .drop(['Technology', 'Description'], axis=1) \
                   .rename(columns={'MESSAGE name': 'technology'}) \
                   .melt(id_vars=['technology'], var_name='year') \
                   .astype({'year': int}) \
                   .assign(node=node, name=name)

            dfs.append(df)

    return pd.concat(dfs, ignore_index=True)
