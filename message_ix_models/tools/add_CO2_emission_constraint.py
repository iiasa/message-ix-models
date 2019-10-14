import pandas as pd

from .get_optimization_years import main as get_optimization_years


def main(scen, relation_name, constraint_value, type_rel, reg='R11_GLB'):
    """Adds bound for generic relation at the global level.

    This specific bound added to the scenario can be used to account
    for CO2 emissions.

    Parameters
    ----------
    scen : :class:`message_ix.Scenario`
        scenario to which changes should be applied
    relation_name : str
        name of the generic relation for which the limit should be set
    constraint_value : number
        value to which the constraint should be set
    type_rel : str
        relation type (lower or upper)
    reg : str (Default='R11_GLB')
        node in scen to which constraitn should be applied
    """

    df = pd.DataFrame({
        'node_rel': reg,
        'relation': relation_name,
        'year_rel': get_optimization_years(scen),
        'value': constraint_value,
        'unit': 'tC',
        })

    scen.check_out()
    scen.add_par('relation_{}'.format(type_rel), df)
    scen.commit('added lower limit of zero for CO2 emissions'
                + 'accounted for in the relation {}'.format(relation_name))
