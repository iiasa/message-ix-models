from collections import defaultdict

import pandas as pd

from .util import read_config
import re



def modify_demand(scen):
    """Take care of demand changes due to the introduction of material parents

    Shed industrial energy demand properly.

    Also need take care of remove dynamic constraints for certain energy carriers
    """

    # NOTE Temporarily modifying industrial energy demand
    #       (30% of non-elec industrial energy for steel)

    # For aluminum there is no significant deduction required
    # (refining process not included and thermal energy required from
    # recycling is not a significant share.)
    # For petro: based on 13.1 GJ/tonne of ethylene and the demand in the model

    df = scen.par('demand', filters={'commodity':'i_therm'})
    df.value = df.value * 0.38 #(30% steel, 25% cement, 7% petro)

    scen.check_out()
    scen.add_par('demand', df)
    scen.commit(comment = 'modify i_therm demand')

    # Adjust the i_spec.
    # Electricity usage seems negligable in the production of HVCs.
    # Aluminum: based on IAI China data 20%.

    df = scen.par('demand', filters={'commodity':'i_spec'})
    df.value = df.value * 0.80  #(15% aluminum)

    scen.check_out()
    scen.add_par('demand', df)
    scen.commit(comment = 'modify i_spec demand')

    # Adjust the i_feedstock.
    # 45 GJ/tonne of ethylene or propylene or BTX
    # 2020 demand of one of these: 35.7 Mt
    # Makes up around 30% of total feedstock demand.

    df = scen.par('demand', filters={'commodity':'i_feed'})
    df.value = df.value * 0.7  #(70% HVCs)

    scen.check_out()
    scen.add_par('demand', df)
    scen.commit(comment = 'modify i_feed demand')

    # NOTE Aggregate industrial coal demand need to adjust to
    #      the sudden intro of steel setor in the first model year

    t_i = ['coal_i','elec_i','gas_i','heat_i','loil_i','solar_i']

    for t in t_i:
        df = scen.par('growth_activity_lo', \
                filters={'technology':t, 'year_act':2020})

        scen.check_out()
        scen.remove_par('growth_activity_lo', df)
        scen.commit(comment = 'remove growth_lo constraints')



# Read in technology-specific parameters from input xlsx
# Now used for steel and cement, which are in one file
def read_sector_data(sectname):

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
        [['Region', 'Technology', 'Parameter', 'Level',  \
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
def read_timeseries(filename):

    import numpy as np

    # Ensure config is loaded, get the context
    context = read_config()

    if context.scenario_info['scenario'] == 'NPi400':
        sheet_name="timeseries_NPi400"
    else:
        sheet_name = "timeseries"

    # Read the file
    df = pd.read_excel(
        context.get_path("material", filename), sheet_name)

    import numbers
    # Take only existing years in the data
    datayears = [x for x in list(df) if isinstance(x, numbers.Number)]

    df = pd.melt(df, id_vars=['parameter', 'region', 'technology', 'mode', 'units'], \
        value_vars = datayears, \
        var_name ='year')

    df = df.drop(df[np.isnan(df.value)].index)
    return df

def read_rel(filename):

    import numpy as np

    # Ensure config is loaded, get the context
    context = read_config()

    # Read the file
    data_rel = pd.read_excel(
        context.get_path("material", filename), sheet_name="relations",
    )

    return data_rel
