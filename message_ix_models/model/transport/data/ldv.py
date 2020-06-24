from collections import defaultdict

from openpyxl import load_workbook
import pandas as pd

from message_data.model.transport.utils import (
    add_commodity_and_level,
    read_config,
)
from message_data.tools import get_context, make_df, make_io


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
    context = read_config()
    source = context['transport config']['data source'].get('LDV', None)

    if source == 'US-TIMES MA3T':
        return get_USTIMES_MA3T(info)
    elif source is None:
        return {}  # Don't add any data
    else:
        raise ValueError(f'invalid source for non-LDV data: {source}')


def get_USTIMES_MA3T(info):
    """Read LDV cost and efficiency data from US-TIMES and MA3T."""
    # Ensure transport config is loaded
    context = read_config()

    # Open workbook
    path = context.get_path("transport", FILE)
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

            data[par_name].append(
                # Make the first row the headers
                df.iloc[1:, :]
                .set_axis(df.loc[0, :], axis=1)
                # Drop extra columns
                .drop(['Technology', 'Description'], axis=1)
                # Use 'MESSAGE name' as the technology name
                .rename(columns={'MESSAGE name': 'technology'})
                # Pivot to long format
                .melt(id_vars=['technology'], var_name='year')
                # Year as integer
                .astype({'year': int})
                # Within the model horizon/time resolution
                .query(f"year in [{', '.join(map(str, info.Y))}]")
                # Assign values
                .assign(node=node, unit=unit)
                # Drop NA values (e.g. ICE_L_ptrp after the first year)
                .dropna(subset=['value'])
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

    i_o['input'] = add_commodity_and_level(i_o['input'])

    # Transform costs
    for par in 'fix_cost', 'inv_cost':
        base = data.pop(par)
        # Rename 'node' and 'year' columns
        data[par] = make_df(
            par,
            node_loc=base['node'],
            technology=base['technology'],
            year_vtg=base['year'],
            year_act=base['year'],  # fix_cost only
            value=base['value'],
            unit=base['unit'],
        )

    return data
