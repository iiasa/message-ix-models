"""Add regional CO2 entries from AFOLU to a generic relation in a specified region.

.. caution:: |gh-350|
"""

from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from message_ix import Scenario


def add_AFOLU_CO2_accounting(
    scen: "Scenario",
    relation_name: str,
    glb_reg: str = "R11_GLB",
    constraint_value: Optional[float] = None,
    emission: str = "LU_CO2_orig",
    level: str = "LU",
    suffix=None,
) -> None:
    """Add regional CO2 entries from AFOLU to a generic relation in a specified region.

    Specifically for the land_use scenarios: For each land_use scenario a new commodity
    is created on a new `level` "LU".  Each land_use scenario has an output of "1" onto
    their commodity. For each of these commodities (which are set to = 0), there is a
    corresponding new technology which has an input of "1" and an entry into the
    relation, which corresponds to the the CO2 emissions of the land_use pathway. This
    complicated setup is required, because Land-use scenarios only have a single entry
    in the emission factor TCE, which is the sum of all land-use related emissions.

    The default configuration applies to CO2.

    Parameters
    ----------
    scen :
        Scenario to which changes should be applied.
    relation_name :
        Name of the generic relation for which the limit should be set.
    glb_reg :
        Node in `scen` to which constraint should be applied.
    constraint_value :
        Value for which the lower constraint should be set.
    emission : str (default="LU_CO2_orig")
        Name of the `land_output` which should be accounted in the relation.
    level : str (default="LU")
        Name of the level onto which duplicate `land_output` should be parametrized for.
    suffix : str
        The suffix will be applied to all uses of the land-use scenario names e.g. for
        helper technologies or commodities.
    """
    with scen.transact("Add relation based land-use TCE emission accounting"):
        # Add relation to set
        if relation_name not in scen.set("relation").tolist():
            scen.add_set("relation", relation_name)

        # Add land-use level to set
        if level not in scen.set("level"):
            scen.add_set("level", level)

        # Add commodities to set and set commodities to equal
        ls = scen.set("land_scenario").tolist()
        ls = [f"{s}{suffix}" for s in ls] if suffix else ls
        scen.add_set("commodity", ls)
        scen.add_set("technology", ls)

        for commodity in ls:
            scen.add_set("balance_equality", [commodity, level])

        # - Retrieve land-use TCE emissions
        # - Add land-use scenario `output` parameter onto new level/commodity
        name = "land_output"
        data = scen.par(name, filters={"commodity": [emission]}).assign(
            commodity=lambda df: df.land_scenario + (suffix or ""),
            level=level,
            value=1.0,
            unit="%",
        )

        if data.empty:
            raise ValueError(f"No {name!r} data for commodity {emission}")

        scen.add_par(name, data)
