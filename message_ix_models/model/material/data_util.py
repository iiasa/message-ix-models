from collections import defaultdict

import pandas as pd

from .util import read_config
import re


# Read in technology-specific parameters from input xlsx
def process_china_data_tec(sectname):

    import numpy as np

    # Ensure config is loaded, get the context
    context = read_config()

    # data_df = data_steel_china.append(data_cement_china, ignore_index=True)
    data_df = pd.read_excel(
        context.get_path("material", context.datafile),
        sheet_name=sectname,
    )

    # Clean the data
    data_df = data_df \
        [['Technology', 'Parameter', 'Level',  \
        'Commodity', 'Mode', 'Species', 'Units', 'Value']] \
        .replace(np.nan, '', regex=True)

    # Combine columns and remove ''
    list_series = data_df[['Parameter', 'Commodity', 'Level', 'Mode']] \
        .apply(list, axis=1).apply(lambda x: list(filter(lambda a: a != '', x)))
    list_ef = data_df[['Parameter', 'Species', 'Mode']] \
        .apply(list, axis=1)

    data_df['parameter'] = list_series.str.join('|')
    data_df.loc[data_df['Parameter'] == "emission_factor", \
        'parameter'] = list_ef.str.join('|')

    data_df = data_df.drop(['Parameter', 'Level', 'Commodity', 'Mode'] \
        , axis = 1)
    data_df = data_df.drop( \
        data_df[data_df.Value==''].index)

    data_df.columns = data_df.columns.str.lower()

    # Unit conversion

    # At the moment this is done in the excel file, can be also done here
    # To make sure we use the same units

    return data_df


# Read in time-dependent parameters
# Now only used to add fuel cost for bare model
def read_timeseries():

    import numpy as np

    # Ensure config is loaded, get the context
    context = read_config()

    if context.scenario_info['scenario'] == 'NPi400':
        sheet_name="timeseries_NPi400"
    else:
        sheet_name = "timeseries"

    # Read the file
    df = pd.read_excel(
        context.get_path("material", context.datafile), sheet_name)

    import numbers
    # Take only existing years in the data
    datayears = [x for x in list(df) if isinstance(x, numbers.Number)]

    df = pd.melt(df, id_vars=['parameter', 'technology', 'mode', 'units'], \
        value_vars = datayears, \
        var_name ='year')

    df = df.drop(df[np.isnan(df.value)].index)
    return df
