from .add_CO2_emission_constraint import main as add_CO2_emission_constraint


def main(scen, relation_name, reg="R11_GLB", constraint_value=None):
    """Adds accounting possibility for CO2 emissions from FFI.

    The constraint on FFI CO2 emissions can be added to a generic
    relation in a specified region.

    Parameters
    ----------
    scen : :class:`message_ix.Scenario`
        scenario to which changes should be applied
    relation_name: str
        name of the generic relation for which the limit should be set
    reg : str (Default='R11_GLB')
        node in scen to which constraitn should be applied
    constraint_value: number (optional)
        value for which the lower constraint should be set
    """

    if relation_name not in scen.set("relation").tolist():
        with scen.transact(
            f"relation {relation_name!r} for limiting regional CO2 emissions at the "
            "global level added"
        ):
            scen.add_set("relation", relation_name)

    if constraint_value:
        add_CO2_emission_constraint(
            scen, relation_name, constraint_value, type_rel="lower"
        )

    df = (
        scen.par(
            "relation_activity",
            filters={"relation": ["CO2_Emission", "CO2_shipping", "CO2_trade"]},
        )
        .query("technology not in ['CO2_TCE', 'CO2t_TCE', 'CO2s_TCE']")
        .assign(relation=relation_name, node_rel=reg)
    )

    with scen.transact("added new relation for accounting for FFI CO2 emissions"):
        scen.add_par("relation_activity", df)
