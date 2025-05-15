"""Add bound for generic relation at the global level.

.. caution:: |gh-350|
"""

from typing import TYPE_CHECKING

import pandas as pd

from message_ix_models import ScenarioInfo

if TYPE_CHECKING:
    from message_ix import Scenario


def main(
    scen: "Scenario",
    relation_name: str,
    constraint_value: float,
    type_rel: str,
    reg: str = "R11_GLB",
) -> None:
    """Add bound for generic relation at the global level.

    This specific bound added to the scenario can be used to account for CO2 emissions.

    Parameters
    ----------
    scen :
        Scenario to which changes should be applied.
    relation_name :
        Name of the generic relation for which the limit should be set.
    constraint_value :
        Value to which the constraint should be set.
    type_rel :
        Relation type (lower or upper).
    reg : str (Default='R11_GLB')
        Node in `scen` to which constraint should be applied.
    """

    df = pd.DataFrame(
        {
            "node_rel": reg,
            "relation": relation_name,
            "year_rel": ScenarioInfo(scen).Y,
            "value": constraint_value,
            "unit": "tC",
        }
    )

    with scen.transact(
        "added lower limit of zero for CO2 emissions accounted for in the relation "
        f"{relation_name}"
    ):
        scen.add_par(f"relation_{type_rel}", df)
