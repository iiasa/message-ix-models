import pandas as pd

from .get_nodes import get_nodes
from .add_CO2_emission_constraint import main as add_CO2_emission_constraint
from .get_optimization_years import main as get_optimization_years


def add_AFOLU_CO2_accounting(scen, relation_name, glb_reg='R11_GLB',
                             constraint_value=None):
    """Adds regional CO2 entries from AFOLU to a generic relation in a
    specified region.

    Specifically for the land_use sceanrios: For each land_use scenario
    a new commodity is created on a new `level` "LU".  Each land_use
    scenario has an output of "1" onto their commodity.  For each of
    these commodities (which are set to = 0), there is a corresponding
    new technology which has an input of "1" and an entry into the
    relation, which corresponds to the the CO2 emissions of the land_use
    pathway. This complicated setup is required, because Land-use
    scenarios only have a single entry in the emission factor TCE,
    which is the sum of all land-use realted emissions.

    Parameters
    ----------
    scen : :class:`message_ix.Scenario`
        scenario to which changes should be applied
    relation_name: str
        name of the generic relation for which the limit should be set
    glb_reg : str (Default='R11_GLB')
        node in scen to which constraitn should be applied
    constraint_value: number (optional)
        value for which the lower constraint should be set
    """

    if relation_name not in scen.set('relation').tolist():
        scen.check_out()
        scen.add_set('relation', relation_name)
        scen.commit('relation {} for limiting'.format(relation_name)
                    + ' regional CO2 emissions at the global level added')

    if constraint_value:
        add_CO2_emission_constraint(scen, relation_name,
                                    constraint_value, type_rel='lower')

    # Add entires into sets required for the generation of the constraint
    #     new level - "LU"
    #     new commodities (set to `equal`) - name = LU scenario
    #     new technologies - name = LU scenario
    scen.check_out()
    scen.add_set('level', 'LU')
    ls = scen.set('land_scenario').tolist()
    scen.add_set('commodity', ls)
    for commodity in ls:
        scen.add_set('balance_equality', [commodity, 'LU'])
    scen.add_set('technology', ls)

    # Retrieve LU_CO2 emissions
    loutput = scen.par('land_output', filters={'commodity': ['LU_CO2']})
    if loutput.empty:
        raise ValueError("'land_output' not available for commodity 'LU_CO2'")

    # Add land-use scenario `output` parameter onto new level/commodity
    df_land_output = loutput.copy()
    df_land_output.commodity = df_land_output.land_scenario
    df_land_output.level = 'LU'
    df_land_output.value = 1
    df_land_output.unit = '%'
    scen.add_par('land_output', df_land_output)

    # Add technology `input` and `relation_activity` parameter
    for reg in get_nodes(scen):
        if reg == glb_reg:
            continue
        for y in get_optimization_years(scen):
            for s in ls:
                if s.find('BIO0N') >= 0:
                    continue

                df = pd.DataFrame({
                    'node_loc': [reg],
                    'technology': s,
                    'year_vtg': y,
                    'year_act': y,
                    'mode': 'M1',
                    'node_origin': reg,
                    'commodity': s,
                    'level': 'LU',
                    'time': 'year',
                    'time_origin': 'year',
                    'value': 1,
                    'unit': '???'
                    })

                scen.add_par('input', df)

                df = pd.DataFrame({
                    'relation': [relation_name],
                    'node_rel': glb_reg,
                    'year_rel': y,
                    'node_loc': reg,
                    'technology': s,
                    'year_act': y,
                    'mode': 'M1',
                    'value': loutput[(loutput.node == reg)
                                     & (loutput.year == y)
                                     & (loutput.land_scenario == s)].value,
                    'unit': '???'
                    })

                scen.add_par('relation_activity', df)
    scen.commit("Added technology to mimic land-use technologies")
