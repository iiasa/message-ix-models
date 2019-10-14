def main(scen, budget, adjust_cumulative=False, type_emission='TCE'):
    """Adds a cumulative budget to the global region.

    Parameters
    ----------

    scen : :class:`message_ix.Scenario`
        Scenario to which budget should be applied
    budget : int
        Budget in average MtC
    adjust_cumulative : bool, optional
        Option whether to adjust cumulative years to which the budget
        is applied to the optimization time horizon.
    type_emission : str, optional
        type_emission for which the constraint should be applied
    """

    scen.check_out()
    if adjust_cumulative:
        current_cumulative_years = scen.set('cat_year',
                                            {'type_year': ['cumulative']})
        remove_cumulative_years = current_cumulative_years[
            current_cumulative_years['year']
            < int(scen.set('cat_year',
                           {'type_year': ['firstmodelyear']})['year'])]

        if not remove_cumulative_years.empty:
            scen.remove_set('cat_year', remove_cumulative_years)
    scen.add_par('bound_emission', ['World', type_emission,
                                    'all', 'cumulative'], budget, 'tC')
    scen.commit('Global emission bound {} added'.format(budget))


if __name__ == '__main__':
    main('test', 'test')
