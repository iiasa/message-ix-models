"""Add accounting possibility for CO2 emissions from FFI.

.. caution:: |gh-350|
"""

from typing import TYPE_CHECKING, Optional

from .add_CO2_emission_constraint import main as add_CO2_emission_constraint

if TYPE_CHECKING:
    from message_ix import Scenario


def main(
    scen: "Scenario",
    relation_name: str,
    reg: str = "R11_GLB",
    constraint_value: Optional[float] = None,
) -> None:
    """Add accounting possibility for CO2 emissions from FFI.

    The constraint on FFI CO2 emissions can be added to a generic relation in a
    specified region.

    Parameters
    ----------
    scen :
        Scenario to which changes should be applied.
    relation_name :
        Name of the generic relation for which the limit should be set.
    reg :
        Node in `scen` to which constraint should be applied.
    constraint_value :
        Value for which the lower constraint should be set.
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
