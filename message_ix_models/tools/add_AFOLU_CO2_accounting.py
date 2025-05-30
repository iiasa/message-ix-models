"""Add ``land_output`` and set entries for accounting AFOLU emissions of CO2."""

import logging
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from message_ix import Scenario

log = logging.getLogger(__name__)


def add_AFOLU_CO2_accounting(
    scen: "Scenario",
    relation_name: str,
    emission: str = "LU_CO2_orig",
    level: str = "LU",
    suffix: Optional[str] = None,
    *,
    # Unused/deprecated parameters
    constraint_value: Optional[float] = None,
    glb_reg: Optional[str] = None,
    reg: Optional[str] = None,
) -> None:
    """Add ``land_output`` and set entries for accounting AFOLU emissions of CO2.

    The function has the following effects on `scen`:

    1. A ``relation`` set member `relation_name` is added. However, **no data** for this
       relation is added or changed.
    2. A ``level`` set member `level` is added.
    3. For every member of set ``land_scenario``:

       a. Members with the same ID are added to the sets ``commodity`` and
          ``technology``. If `suffix` is given, it is appended to these added members.
       b. A ``balance_equality`` set member is added for the commodity and `level`.

    4. Data in ``land_output`` are:

       - Retrieved where :py:`commodity=emission`. With the default value of `emission`
         "LU_CO2_orig", the configuration.
       - Modified to set `level`, value 1.0, unit "%", and replace the commodity label
         with ``{land_scenario}{suffix}``, using the value of land_scenario from the
         respective row.
       - Added to `scen`.

    This structure and data interact with other data that are **not** configured by this
    function, whereby:

    - The technologies added in 3(a) receive ``input`` from the respective commodities.
      This, combined with the ``balance_equality`` setting, ensure that the ``ACT`` of
      these technologies is exactly equal to the corresponding ``LAND``.
    - The technologies in turn have entries into a relation that is used for other
      purposes.

    This complicated setup is required, because land-use scenarios only have a single
    entry in the emission factor TCE, which is the sum of all land-use related
    emissions.

    .. versionchanged:: :pull:`354`

       - The function no longer sets values of ``relation_activity`` or ``input``.
       - Parameters `constraint_value` and `glb_reg` are ignored.

    Parameters
    ----------
    scen :
        Scenario to which changes should be applied.
    relation_name :
        Name of a generic relation.
    emission :
        Commodity name for filtering ``land_output`` data.
    level :
        Level for added ``land_output`` data.
    suffix :
        Optional suffix for added commodity and level names.

    Other parameters
    ----------------
    constraint_value :
        Deprecated, unused.
    glb_reg :
        Deprecated, unused.
    reg :
        Deprecated, unused.

    Raises
    ------
    ValueError
        if there is no ``land_output`` data for :py:`commodity=emission`.
    """
    if constraint_value is not None:
        log.warning(
            f"Argument add_AFOLU_CO2_accounting(…, constraint_value={constraint_value})"
            " is ignored"
        )
    if glb_reg is not None:
        log.warning(
            f"Argument add_AFOLU_CO2_accounting(…, glb_reg={constraint_value}) is"
            " ignored"
        )

    with scen.transact("Add land_output entries for "):
        # Add relation to set
        if relation_name not in scen.set("relation").tolist():
            scen.add_set("relation", relation_name)

        # Add land-use level to set
        if level not in scen.set("level"):
            scen.add_set("level", level)

        # Add commodities and technologies to sets
        ls = [f"{s}{suffix or ''}" for s in scen.set("land_scenario").tolist()]
        scen.add_set("commodity", ls)
        scen.add_set("technology", ls)

        # Enforce commodity balance equal (implicitly, to zero)
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
