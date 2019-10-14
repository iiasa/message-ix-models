import pandas as pd

from .remove_emission_bounds import main as remove_emission_bounds


def main(scen, data, type_emission='TCE', unit='Mt C/yr',
         remove_bounds_emission=True):
    """Modify *scen* to include an emission bound.

    Parameters
    ----------
    scen : :class:`message_ix.Scenario`
        scenario to which changes should be applied
    data : :class:`pandas.DataFrame`
        data with index region (node), columns (years)
    type_emission : string (default is TCE)
        type_emission to which constraint is applied
    unit : string (Default is Mt C/yr)
        units in which values are provided
    remove_bound_emissions : boolean (Default is True)
        option whether or not existing bounds withing the
        optimization time horizon are removed
    """

    if remove_bounds_emission:
        remove_emission_bounds(scen)

    scen.check_out()
    for r in data.index.get_level_values(0).unique().tolist():
        df = pd.DataFrame({
            'node': r,
            'type_emission': type_emission,
            'type_tec': 'all',
            'type_year': data.loc[r].index
            .get_level_values(0).tolist(),
            'value': data.loc[r].values.tolist(),
            'unit': unit,
        })
        scen.add_par('bound_emission', df)

    scen.commit('added emission trajectory')
