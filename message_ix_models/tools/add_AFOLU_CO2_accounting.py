"""Add ``land_output`` and set entries for accounting AFOLU emissions of CO2."""

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
    """Method for :func:`add_AFOLU_CO2_accounting`."""

    #: Version for e.g. :mod:`project.navigate`.
    A = auto()

    #: Version for |ssp-scenariomip| (:pull:`354`).
    B = auto()


def _log_ignored_arg(name: str, value, method: METHOD) -> None:
    log.warning(
        f"Argument add_AFOLU_CO2_accounting(…, {name}={value!r}) is ignored with "
        f"method={method!r}"
    )


log = logging.getLogger(__name__)


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
    """Add ``land_output`` and set entries for accounting AFOLU emissions of CO2.

    The function has the following effects on `scen`:

    1. A ``relation`` set member `relation_name` is added. However, **no data** for this
       relation is added or changed.
    2. A ``level`` set member `level` is added.
    3. For every member of set ``land_scenario``:

       a. Members with the same ID are added to both of the sets ``commodity`` and
          ``technology``. If `suffix` is given, it is appended to these added members.
       b. A ``balance_equality`` set member is added for the commodity and `level`.

    4. Data in ``land_output`` are:

       - Retrieved where :py:`commodity=emission` according to parameter `emission`.
       - Modified to set `level`, value 1.0, unit "%", and replace the commodity label
         with ``{land_scenario}{suffix}``, using the value of land_scenario from the
         respective row.
       - Added to `scen`.

    This structure and data interact with other data whereby:

    - The technologies added in 3(a) receive ``input`` from the respective commodities.
      This, combined with the ``balance_equality`` setting, ensure that the ``ACT`` of
      these technologies is exactly equal to the corresponding ``LAND``.
    - The technologies in turn have entries into a relation that is used for other
      purposes.

    With `method` = :attr:`METHOD.A`, :func:`add_par_A` is called to add these data.
    With :attr:`METHOD.B` (the default), this is not done, and those other entries
    **must** already be present in `scen`.

    This complicated setup is required, because land-use scenarios only have a single
    entry in the emission factor TCE, which is the sum of all land-use related
    emissions.

    .. versionchanged:: NEXT-RELEASE

       With `method` :attr:`METHOD.B` now the default, the function no longer sets
       values of ``relation_activity`` or ``input``, and parameters `constraint_value`
       and `glb_reg` are ignored. To preserve the original behaviour, pass
       :attr:`METHOD.A`. (:pull:`354`)

    Parameters
    ----------
    scen :
        Scenario to which changes should be applied.
    relation_name :
        Name of a generic relation.
    constraint_value :
        (:attr:`METHOD.A` only) Passed to :func:`add_CO2_emission_constraint.main`.
    emission :
        Commodity name for filtering ``land_output`` data. If not given, defaults to
        "LU_CO2" (:attr:`METHOD.A`) or "LU_CO2_orig" (:attr:`METHOD.B`).
    level :
        Level for added ``land_output`` data.
    suffix :
        (:attr:`METHOD.B` only) Suffix for added commodity and level names.
    method :
        A member of the :class:`METHOD` enumeration.

    Other parameters
    ----------------
    glb_reg :
        (:attr:`METHOD.A` only) Region for ``node_rel`` dimension of
        ``relation_activity`` parameters.
    reg :
        Same as `glb_reg`.

    Raises
    ------
    ValueError
        if there is no ``land_output`` data for :py:`commodity=emission`.
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


def test_data(scenario: "Scenario") -> tuple[str, list[str], "DataFrame"]:
    """Add minimal data for testing to `scenario`.

    This includes a bare minimum of data such that :func:`add_AFOLU_CO2_accounting` runs
    without error.
    """
    info = ScenarioInfo(scenario)

    commodity = "LU_CO2_orig"
    land_scenario = ["BIO00GHG000", "BIO06GHG3000"]

    land_output = make_df(
        "land_output",
        commodity=commodity,
        level="primary",
        value=123.4,
        unit="-",
        time="year",
    ).pipe(broadcast, year=info.Y, node=info.N, land_scenario=land_scenario)

    with scenario.transact("Prepare for test of add_AFOLU_CO2_accounting()"):
        scenario.add_set("commodity", commodity)
        scenario.add_set("land_scenario", land_scenario)
        scenario.add_par("land_output", land_output)

    return commodity, land_scenario, land_output
