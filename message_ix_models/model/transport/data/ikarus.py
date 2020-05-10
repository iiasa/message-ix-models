"""Prepare non-LDV data from the IKARUS model via GEAM_TRP_techinput.xlsx."""
from collections import defaultdict

from openpyxl import load_workbook
import pandas as pd

from message_data.model.transport import read_config
from message_data.tools import make_df

#: Name of the input file.
#
# The input file uses the old, MESSAGE V names for parameters:
# - inv_cost = inv
# - fix_cost = fom
# - technical_lifetime = pll
# - input (efficiency) = minp
# - output (efficiency) = moutp
# - capacity_factor = plf
FILE = 'GEAM_TRP_techinput.xlsx'

#: Mapping from parameters to 3-tuples of units:
#: 1. Factor for units appearing in the input file.
#: 2. Units appearing in the input file.
#: 3. Target units for MESSAGEix-GLOBIOM.
UNITS = dict(
    # Appearing in input file
    inv_cost=(1.e6, 'EUR_2000 / vehicle', 'MUSD_2005 / vehicle'),
    fix_cost=(1000., 'EUR_2000 / vehicle / year',
              'MUSD_2005 / vehicle / year'),
    var_cost=(0.01, 'EUR_2000 / kilometer', None),
    technical_lifetime=(1, 'year', None),
    availability=(100, 'kilometer / vehicle / year', None),
    input=(0.01, 'GJ / kilometer', None),
    output=(1, '', None),

    # Created below
    capacity_factor=(None, None, 'gigapassenger kilometre / vehicle / year')
)
ROWS = [
    'inv_cost',
    'fix_cost',
    'var_cost',
    'technical_lifetime',
    'availability',
    'input',
    'output',
]

#: Starting and final cells delimiting tables in sheet.
CELL_RANGE = {
    'rail_pub': ['C103', 'I109'],
    'dMspeed_rai': ['C125', 'I131'],
    'Mspeed_rai': ['C147', 'I153'],
    'Hspeed_rai': ['C169', 'I175'],

    'con_ar': ['C179', 'I185'],
    # Same parametrization as 'con_ar' (per cell references in spreadsheet):
    'conm_ar': ['C179', 'I185'],
    'conE_ar': ['C179', 'I185'],
    'conh_ar': ['C179', 'I185'],

    'ICE_M_bus': ['C197', 'I203'],
    'ICE_H_bus': ['C205', 'I211'],
    'ICG_bus': ['C213', 'I219'],
    # Same parametrization as 'ICG_bus'. Conversion factors will be applied.
    'ICAe_bus': ['C213', 'I219'],
    'ICH_bus': ['C213', 'I219'],
    'PHEV_bus': ['C213', 'I219'],
    'FC_bus': ['C213', 'I219'],
    # Both equivalent to 'FC_bus'
    'FCg_bus': ['C213', 'I219'],
    'FCm_bus': ['C213', 'I219'],

    'Trolley_bus': ['C229', 'I235']
}

#: Years appearing in the input file.
COLUMNS = [2000, 2005, 2010, 2015, 2020, 2025, 2030]


def convert_units(s, context):
    """Convert units of pd.Series *s*, for use with :meth:`~DataFrame.apply`.

    The ``s.name`` is used to retrieve a tuple of (factor, input unit, output
    unit) from :obj:`UNITS`. The (:class:`float`) values of *s* are converted
    to :class:`pint.Quantity` with the input units and factor; then cast to the
    output units.

    Parameters
    ----------
    s : pandas.Series
    context : Context

    Returns
    -------
    pandas.Series
        Same shape, index, and values as *s*, with output units.
    """
    factor, unit_in, unit_out = UNITS[s.name]
    # replace None with the input unit
    unit_out = unit_out or unit_in
    # Convert the values to a pint.Quantity(array) with the input units
    qty = context.units.Quantity(factor * s.values, unit_in)
    # Convert to output units, then to a list of scalar Quantity
    return pd.Series(qty.to(unit_out).tolist(), index=s.index)


def get_ikarus_data(info):
    """Read IKARUS :cite:`Martinsen2006` data and conform to Scenario *info*.

    The data is read from from ``GEAM_TRP_techinput.xlsx``, and the processed
    data is exported into ``non_LDV_techs_wrapped.csv``.

    Parameters
    ----------
    info : .ScenarioInfo
        Information about target Scenario.

    Returns
    -------
    data : dict of (str -> pandas.DataFrame)
        Keys are MESSAGE parameter names such as 'input', 'fix_cost'. Values
        are data frames ready for :meth:`~.Scenario.add_par`.
    """
    # Read configuration, including units and conversion factors
    context = read_config()
    # Reference to the transport configuration
    config = context['transport config']
    tech_info = context["transport set"]["technology"]["add"]

    # Open the input file using openpyxl
    wb = load_workbook(context.get_path('transport', FILE), read_only=True,
                       data_only=True)
    # Open the 'updateTRPdata' sheet
    sheet = wb['updateTRPdata']

    # Additional output efficiency and investment cost factors for some bus
    # technologies
    out_factor = config['factor']['efficiency']['bus output']
    inv_factor = config['factor']['cost']['bus inv']

    # 'technology name' -> pd.DataFrame
    dfs = {}
    for tec, cell_range in CELL_RANGE.items():
        # - Read values from table for one technology, e.g. "regional train
        #   electric efficient" = rail_pub.
        # - Extract the value from each openpyxl cell object.
        # - Set all non numeric values to NaN.
        # - Transpose so that each variable is in one column.
        # - Convert from input units to desired units.
        df = pd.DataFrame(
            list(sheet[slice(*cell_range)]),
            index=ROWS,
            columns=COLUMNS) \
            .applymap(lambda c: c.value) \
            .apply(pd.to_numeric, errors='coerce') \
            .transpose() \
            .apply(convert_units, context=context)

        # Conversion of IKARUS data to MESSAGEix-scheme parameters.

        # Read output efficiency (occupancy factor) from config and apply units
        output = (config['non-ldv']['output'][tec] * context.units('pkm / km'))
        # Convert to a Series so operations are element-wise
        output = pd.Series([output] * len(df.index), index=df.index)

        # Compute output efficiency
        df['output'] = output / df['input'] * out_factor.get(tec, 1.0)

        df['capacity_factor'] = df['availability'] * output

        df['inv_cost'] *= inv_factor.get(tec, 1.0)

        # Include variable cost * availability in fix_cost
        df['fix_cost'] += (df['availability'] * df['var_cost'])

        df.drop(columns='availability', inplace=True)

        df.drop(columns='var_cost', inplace=True)

        # Store
        dfs[tec] = df

    # Finished reading IKARUS data from spreadsheet
    wb.close()

    # - Concatenate to pd.DataFrame with technology and param as columns.
    # - Reformat as a pd.Series with a 3-level index: year, technology, param
    data = pd.concat(dfs, axis=1, names=['technology', 'param']) \
             .rename_axis(index='year') \
             .stack(['technology', 'param'])

    # Create data frames to add imported params to MESSAGEix

    # Vintage and active years from Scenario
    vtg_years, act_years = info.yv_ya['year_vtg'], info.yv_ya['year_act']

    # Default values to be used as args in make_df()
    defaults = dict(
        mode='all',
        year_act=act_years.astype(int),
        year_vtg=vtg_years.astype(int),
        time='year',
        time_origin='year',
        time_dest='year',
    )

    # Dict of ('parameter name' -> [list of data frames])
    result = defaultdict(list)

    # Iterate over each parameter and technology
    for (par, tec), group_data in data.groupby(['param', 'technology']):
        # Dict including the default values to be used as args in make_df()
        args = defaults.copy()
        args['technology'] = tec

        # Parameter-specific arguments/processing
        if par == 'input':
            tech = tech_info[tech_info.index(tec)]
            args["commodity"] = tech.anno["input"]["commodity"]
            # TODO use the appropriate level for the given commodity; see
            #      ldv.py
            args['level'] = 'final'
        elif par == 'output':
            args['level'] = 'useful'
            args['commodity'] = 'transport pax vehicle'
        elif par == 'capacity_factor':
            # Convert to preferred units
            group_data = group_data.apply(lambda v: v.to(UNITS[par][2]))

        # Units, as an abbreviated string
        units = group_data.apply(lambda x: x.units).unique()
        assert len(units) == 1, 'Units must be unique per (tec, par)'
        args['unit'] = f'{units[0]:~}'

        # Create data frame with values from *args*
        df = make_df(par, **args)

        # Copy data into the 'value' column, by vintage year
        for (year, *_), value in group_data.items():
            df.loc[df['year_vtg'] == year, 'value'] = value.magnitude

        # Drop duplicates. For parameters with 'year_vtg' but no 'year_act'
        # dimension, the same year_vtg appears multiple times because of the
        # contents of *defaults*
        df.drop_duplicates(inplace=True)

        # Fill remaining values for the rest of vintage years with the last
        # value registered, in this case for 2030.
        df['value'] = df['value'].fillna(method='ffill')

        # Broadcast across all nodes
        result[par].append(
            df.pipe(broadcast, node_loc=info.N)
            .pipe(same_node)
        )

    # Concatenate data frames for each model parameter
    for par, list_of_df in result.items():
        result[par] = pd.concat(list_of_df)

        # DEBUG write each parameter's data to a file
        result[par].to_csv(context.get_path('debug', f'ikarus-{par}.csv'),
                           index=False)

    return result
