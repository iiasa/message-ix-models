"""Remove ``bound_emission`` and ``tax_emission`` data from a scenario.

.. caution:: |gh-350|
"""

from collections.abc import Collection
from typing import TYPE_CHECKING

from message_ix_models import ScenarioInfo

if TYPE_CHECKING:
    from message_ix import Scenario


def main(
    scen: "Scenario",
    remove_all: bool = False,
    *,
    parameters: Collection[str] = ("bound_emission", "tax_emission"),
) -> None:
    """Remove ``bound_emission`` and ``tax_emission`` data from `scen`.

    Parameters
    ----------
    scen :
        Scenario for which the parameters should be removed.
    remove_all :
        If :any:`True`, remove all data in the parameters. If :any:`False` (default), do
        not remove data where ``type_year`` is equal to or less than |y0|.
    parameters :
        Parameters from which to remove data.
    """

    info = ScenarioInfo(scen)

    with scen.transact("removed emission bounds and taxes"):
        for par in parameters:
            df = scen.par(par)
            if df.empty:
                continue

            # Remove cumulative years
            df_cum = df[df.type_year == "cumulative"]
            if not df_cum.empty:
                scen.remove_par(par, df_cum)
                df = df[df.type_year != "cumulative"]

            # Remove yearly bounds
            if not remove_all:
                df["type_year"] = df["type_year"].astype("int64")
                df = df[df["type_year"] > info.y0]
            scen.remove_par(par, df)
