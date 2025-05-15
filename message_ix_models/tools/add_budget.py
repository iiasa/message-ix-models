"""Add a budget constraint to a given region.

.. caution:: |gh-350|
"""

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from message_ix import Scenario

log = logging.getLogger(__name__)


def main(
    scen: "Scenario",
    budget: float,
    adjust_cumulative: bool = False,
    type_emission: str = "TCE",
    type_tec: str = "all",
    type_year: str = "cumulative",
    region: str = "World",
    unit: str = "tC",
) -> None:
    """Add a budget constraint to a given region.

    Parameters
    ----------

    scen :
        Scenario to which budget should be applied.
    budget :
        Budget in average tC.
    adjust_cumulative :
        Option whether to adjust cumulative years to which the budget is applied to the
        optimization time horizon.
    type_emission :
        type_emission for which the constraint should be applied. This element must
        already be defined in `scen`.
    type_tec :
        Technology type for which the bound applies.
    region :
        Region to which the bound applies.
    unit :
        Unit in which the bound is provided.
    """
    if adjust_cumulative:
        current_cumulative_years = scen.set("cat_year", {"type_year": ["cumulative"]})

        remove_cumulative_years = current_cumulative_years[
            current_cumulative_years["year"] < scen.firstmodelyear
        ]

        if not remove_cumulative_years.empty:
            with scen.transact("Remove cumulative years"):
                scen.remove_set("cat_year", remove_cumulative_years)

    args = [region, type_emission, type_tec, type_year], budget, unit
    log.info(repr(args))

    with scen.transact(f"bound_emission {budget} added"):
        scen.add_par("bound_emission", *args)
