from collections import defaultdict
from functools import lru_cache

from openpyxl import load_workbook
import pandas as pd

from message_data.model.transport.utils import read_config
from message_data.tools import commodities, get_context, make_io


#: Input file containing data from US-TIMES and MA3T models.
FILE = 'LDV_costs_efficiencies_US-TIMES_MA3T.xlsx'

#: (parameter name, cell range, units) for data to be read from multiple
#: sheets in the file.
TABLES = [
    ('efficiency', slice('B3', 'Q15'), '10^9 v km / GWh / year'),
    ('inv_cost', slice('B33', 'Q45'), 'USD / vehicle'),
    ('fix_cost', slice('B62', 'Q74'), 'USD / vehicle'),
]


def get_ldv_data(info):
    """Read LDV cost and efficiency data from US-TIMES and MA3T."""
    # Open workbook
    path = get_context().get_path('transport', FILE)
    wb = load_workbook(path, read_only=True, data_only=True)

    # Tables
    data = defaultdict(list)

    # Iterate over regions/nodes
    for node in info.N:
        if node == 'World':
            continue

        # Worksheet for this region
        sheet_node = node.split('_')[-1].lower()
        sheet = wb[f'MESSAGE_LDV_{sheet_node}']

        # Read tables for efficiency, investment, and fixed O&M cost
        # NB fix_cost varies by distance driven, thus this is the value for
        #    average driving.
        # TODO calculate the values for modest and frequent driving
        # TODO these values are calculated; transfer the calculations to code
        for par_name, cells, unit in TABLES:
            df = pd.DataFrame(list(sheet[cells])) \
                   .applymap(lambda c: c.value)

            # Make the first row the headers
            df.columns = df.loc[0, :]

            # Remaining rows: rearrange columns, period as column.
            data[par_name].append(
                df.iloc[1:, :]
                  .drop(['Technology', 'Description'], axis=1)
                  .rename(columns={'MESSAGE name': 'technology'})
                  .melt(id_vars=['technology'], var_name='year')
                  .astype({'year': int})
                  .assign(node=node, unit=unit)
            )

    # Concatenate data frames
    data = {par: pd.concat(dfs) for par, dfs in data.items()}

    # Convert 'efficiency' into 'input' and 'output' parameter data
    base = data.pop('efficiency')
    i_o = make_io(
        src=(None, None, 'GWh'),
        dest=('transport pax vehicle', 'useful', 'Gv km'),
        efficiency=1. / base['value'],
        on='input',
        # Other data
        node_loc=base['node'],
        node_origin=base['node'],
        node_dest=base['node'],
        technology=base['technology'],
        year_vtg=base['year'],
        year_act=base['year'],
        mode='all',
        # No subannual detail
        time='year',
        time_origin='year',
        time_dest='year',
    )

    # Add input commodity and level
    read_config()
    t_info = get_context()['transport technology']['technology']
    c_info = commodities.get_info()

    @lru_cache()
    def t_cl(t):
        # Commodity must be specified
        commodity = t_info[t]['input commodity']
        # Use the default level for the commodity in the RES (per
        # commodity.yaml) or 'secondary'
        level = c_info[commodity].get('level', 'secondary')

        return commodity, level

    def add_commodity_and_level(row):
        row[['commodity', 'level']] = t_cl(row['technology'])
        return row

    i_o['input'] = i_o['input'].apply(add_commodity_and_level, axis=1)

    data.update(i_o)

    return data
