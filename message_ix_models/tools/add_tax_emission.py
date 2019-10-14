from . import get_optimization_years


def main(scen, price):
    """Adds a global CO2 price to a scenario.

    A global carbon price is implemented with an annual growth rate
    equal to the discount rate.

    Parameters
    ----------
    scen : :class:`message_ix.Scenario`
        Scenario for which a carbon price should be added

    price : int
        Carbon price which should be added to the model.  This value
        will be applied from the 'firstmodelyear' onwards. The price
        needs to be specified in $(2005)/tCO2. This is converted to
        $(2005)/tC as required by the model.
    """
    years = get_optimization_years(scen)
    df = scen.par('duration_period',
                  filters={'year': years})\
             .drop(['unit'], axis=1)\
             .rename(columns={'value': 'duration'})\
             .set_index(['year'])
    for yr in years:
        if years.index(yr) == 0:
            val = price / (12/44)
        else:
            val = df.loc[years[years.index(yr)-1]].value\
                * (1+scen.par('drate').value.unique()[0])\
                ** df.loc[yr].duration
        df.set_value(yr, 'value', val)
    df = df.reset_index()\
           .drop(['duration'], axis=1)\
           .rename(columns={'year': 'type_year'})
    df.loc[:, 'node'] = 'World'
    df.loc[:, 'type_emission'] = 'TCE'
    df.loc[:, 'type_tec'] = 'all'
    df.loc[:, 'unit'] = 'USD/tCO2'

    scen.check_out()
    scen.add_par('tax_emission', df)
    scen.commit('Added carbon price')


if __name__ == '__main__':
    main('test', 'test')
