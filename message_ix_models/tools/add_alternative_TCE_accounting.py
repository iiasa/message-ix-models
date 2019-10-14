def main(scen):
    """Adds alternative emission_types for constraints.

    Add alternative emission_types (TCE_CO2 and TCE_non-CO2)
    so that constraints for both CO2 and non-CO2 GHGs can be
    separately defined. All relevant emission factors are
    added.

    Parameters
    ----------
    scen : :class:`message_ix.Scenario`
        scenario to which changes should be applied
    """

    # Create a new type_emission 'TCE_CO2' and 'TCE_non-CO2'
    scen.check_out()
    for type_emi in ['TCE_CO2', 'TCE_non-CO2']:
        if type_emi not in scen.set('type_emission').tolist():
            scen.add_set('type_emission', type_emi)
            scen.add_set('emission', type_emi)
            scen.add_set('cat_emission', [type_emi, type_emi])
    scen.commit('added new emission types TCE_CO2 and TCE_non-CO2')

    # Copy all emission factors with string.find(CO2)
    emi_fac = scen.par('emission_factor', filters={'emission': ['TCE']})
    emi = [e for e in emi_fac.technology.unique().tolist() if
           e.find('CO2') >= 0]
    tce_co2 = emi_fac[emi_fac.technology.isin(emi)]
    tce_co2.emission = 'TCE_CO2'
    scen.check_out()
    scen.add_par('emission_factor', tce_co2)
    scen.commit('added emission factors for type_emission TCE_CO2')

    # Create emission factor from land_output 'LU_CO2'
    lu_co2 = scen.par('land_emission', filters={'emission': ['LU_CO2']})
    lu_co2.emission = 'TCE_CO2'
    scen.check_out()
    scen.add_par('land_emission', lu_co2)
    scen.commit('added land use emission factors for type_emission TCE_CO2')

    # Copy all emission factors with inverse of string.find(CO2)
    tce_nonco2 = emi_fac[~emi_fac.technology.isin(emi)]
    tce_nonco2.emission = 'TCE_non-CO2'
    scen.check_out()
    scen.add_par('emission_factor', tce_nonco2)
    scen.commit('added emission factors for type_emission TCE_non-CO2')

    # Create emission factor from land_use TCE - land_ouput 'LU_CO2'
    lu_co2.emission = 'TCE_non-CO2'
    lu_co2 = lu_co2.set_index(['node', 'land_scenario', 'year',
                               'emission', 'unit'])
    lu_nonco2 = scen.par('land_emission', filters={'emission': ['TCE']})
    lu_nonco2.emission = 'TCE_non-CO2'
    lu_nonco2 = lu_nonco2.set_index(['node', 'land_scenario', 'year',
                                     'emission', 'unit'])
    lu_nonco2 = lu_nonco2 - lu_co2
    lu_nonco2 = lu_nonco2.reset_index()
    scen.check_out()
    scen.add_par('land_emission', lu_nonco2)
    scen.commit('added land use emission factors for type_emission'
                + 'TCE_non-CO2')
