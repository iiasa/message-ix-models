"""Remove all ``tax_emission`` and ``bound_emission`` from a given scenario.

.. caution:: |gh-350|
"""

from typing import TYPE_CHECKING

from message_ix_models import ScenarioInfo

if TYPE_CHECKING:
    from message_ix import Scenario


def main(scen: "Scenario", remove_all: bool = False) -> None:
    """Remove all ``tax_emission`` and ``bound_emission`` from a given scenario.

    Parameters
    ----------
    scen :
        Scenario for which the parameters should be removed.
    """

    info = ScenarioInfo(scen)

    with scen.transact("removed emission bounds and taxes"):
        for par in ["bound_emission", "tax_emission"]:
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
