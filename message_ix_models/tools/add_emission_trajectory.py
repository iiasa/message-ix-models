"""Modify `scen` to include an emission bound.

.. caution:: |gh-350|
"""

from typing import TYPE_CHECKING

import pandas as pd

from .remove_emission_bounds import main as remove_emission_bounds

if TYPE_CHECKING:
    from message_ix import Scenario


def main(
    scen: "Scenario",
    data: pd.DataFrame,
    type_emission: str = "TCE",
    unit: str = "Mt C/yr",
    remove_bounds_emission: bool = True,
) -> None:
    """Modify `scen` to include an emission bound.

    Parameters
    ----------
    scen :
        Scenario to which changes should be applied.
    data :
        Data with index region (node), columns (years).
    type_emission :
        ``type_emission`` to which constraint is applied.
    unit :
        Units in which values are provided.
    remove_bound_emissions :
        Option whether or not existing bounds withing the optimization time horizon are
        removed.
    """

    if remove_bounds_emission:
        remove_emission_bounds(scen)

    with scen.transact("added emission trajectory"):
        for r in data.index.get_level_values(0).unique().tolist():
            df = pd.DataFrame(
                {
                    "node": r,
                    "type_emission": type_emission,
                    "type_tec": "all",
                    "type_year": data.loc[r].index.get_level_values(0).tolist(),
                    "value": data.loc[r].values.tolist(),
                    "unit": unit,
                }
            )
            scen.add_par("bound_emission", df)
