"""Add regional CO2 entries from AFOLU to a generic relation in a specified region.

.. caution:: |gh-350|
"""

import logging
from enum import Enum, auto
from typing import TYPE_CHECKING, Optional

from message_ix import make_df

from message_ix_models import ScenarioInfo
from message_ix_models.util import broadcast, nodes_ex_world

if TYPE_CHECKING:
    from message_ix import Scenario
    from pandas import DataFrame

log = logging.getLogger(__name__)


class METHOD(Enum):
    """Method."""

    #: Version for e.g. :mod:`project.navigate`.
    A = auto()

    #: Version subsequent to :pull:`354` and ScenarioMIP7/SSP 2024 update.
    B = auto()


def _log_ignored_arg(name: str, value, method: METHOD) -> None:
    log.warning(
        f"Argument add_AFOLU_CO2_accounting(…, {name}={value!r}) is ignored with "
        f"method={method!r}"
    )


def add_AFOLU_CO2_accounting(
    scen: "Scenario",
    relation_name: str,
    constraint_value: Optional[float] = None,
    emission: Optional[str] = None,
    level: str = "LU",
    suffix: str = "",
    *,
    method: METHOD = METHOD.B,
    **kwargs,
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
    # Handle arguments
    if method is METHOD.A and suffix:
        _log_ignored_arg("suffix", suffix, method)
        suffix = ""
    if method is METHOD.B and constraint_value is not None:
        _log_ignored_arg("constraint_value", constraint_value, method)
        constraint_value = None
    if method is METHOD.B and {"reg", "glb_reg"} & set(kwargs):
        _log_ignored_arg("glb_reg", kwargs, method)

    emission = emission or {METHOD.A: "LU_CO2", METHOD.B: "LU_CO2_orig"}[method]

    # Retrieve land-use TCE emissions
    name = "land_output"
    land_output = scen.par(name, filters={"commodity": [emission]})

    if land_output.empty:
        raise ValueError(f"No {name!r} data for commodity {emission}")

    land_scenarios = [s + suffix for s in scen.set("land_scenario").tolist()]

    with scen.transact(f"Add land_output entries for {relation_name!r}"):
        # Add relation to set
        if relation_name not in scen.set("relation").tolist():
            scen.add_set("relation", relation_name)

        # Add land-use level to set
        if level not in scen.set("level"):
            scen.add_set("level", level)

        for ls in land_scenarios:
            # Add commodities and technologies to sets
            scen.add_set("commodity", ls)
            scen.add_set("technology", ls)

            # Enforce commodity balance equal (implicitly, to zero)
            scen.add_set("balance_equality", [ls, level])

        # Add land-use scenario `output` parameter onto new level/commodity
        # (common to METHOD.A and METHOD.B)
        data = land_output.assign(
            commodity=lambda df: df.land_scenario + suffix,
            level=level,
            value=1.0,
            unit="%",
        )
        scen.add_par(name, data)

        if method is METHOD.A:
            add_par_A(
                scen,
                land_output,
                level,
                relation_name,
                kwargs.get("reg", "R11_GLB"),
                land_scenarios,
            )

    if method is METHOD.A and constraint_value:
        from . import add_CO2_emission_constraint

        add_CO2_emission_constraint.main(
            scen,
            relation_name,
            constraint_value,
            type_rel="lower",
            reg=kwargs.get("reg", "R11_GLB"),
        )


def add_par_A(
    scen: "Scenario",
    land_output: "DataFrame",
    level: str,
    relation_name: str,
    glb_reg: Optional[str],
    land_scenarios: list[str],
) -> None:
    """Add parameter data specific to :attr:`METHOD.A`."""
    info = ScenarioInfo(scen)

    assert glb_reg in info.N, f"{glb_reg=!r} not among {info.N}"

    # Common/fixed dimensions
    fixed = dict(
        level=level,
        mode="M1",
        node_rel=glb_reg,
        relation=relation_name,
        time_origin="year",
        time="year",
        unit="???",
    )
    # Dimensions to broadcast. Exclude `land_scenarios` starting with BIO0N
    bcast = dict(
        node_loc=nodes_ex_world(info.N),
        technology=list(filter(lambda s: "BIO0N" not in s, land_scenarios)),
        year_act=info.Y,
    )

    name = "input"
    # Construct data: commodity ID is same as technology ID
    df = (
        make_df(name, **fixed, value=1.0)
        .pipe(broadcast, **bcast)
        .eval("commodity = technology \n node_origin = node_loc \n year_vtg = year_act")
    )
    scen.add_par(name, df)

    name = "relation_activity"
    # Construct partial data for "relation_activity"
    tmp = (
        make_df(name, **fixed, value=0.0)
        .pipe(broadcast, **bcast)
        .eval("year_rel = year_act")
    )
    # - Use 'value' column from `land_output`, aligned to `tmp`.
    # - Keep only relation_activity columns per `tmp`.
    df = tmp.merge(
        land_output,
        how="left",
        left_on=("node_loc", "year_act", "technology"),
        right_on=("node", "year", "land_scenario"),
        suffixes=("_left", ""),
    )[tmp.columns]
    scen.add_par(name, df)
