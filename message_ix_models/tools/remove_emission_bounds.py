def main(scen, remove_all=False):
    """ Removes parameters 'tax_emission' and 'bound_emission'
    from a given scenario.

    Parameters
    ----------
    scen : :class:`message_ix.Scenario`
        Scenario for which the parameters should be removed.
    """

    scen.check_out()
    for par in ['bound_emission', 'tax_emission']:
        df = scen.par(par)
        if not df.empty:
            # Remove cumulative years
            df_cum = df[df.type_year == 'cumulative']
            if not df_cum.empty:
                scen.remove_par(par, df_cum)
                df = df[df.type_year != 'cumulative']
            # Remove yearly bounds
            if not remove_all:
                df['type_year'] = df['type_year'].astype('int64')
                df = df[df['type_year'] > int(
                    scen.set('cat_year',
                             {'type_year': ['firstmodelyear']})['year'])]
            scen.remove_par(par, df)
    scen.commit('removed emission bounds and taxes')


if __name__ == '__main__':
    main('test', 'test')
