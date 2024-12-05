import logging
from collections.abc import Collection
from copy import copy
from typing import TYPE_CHECKING, Literal

import genno
import ixmp
import pytest
from pytest import mark, param

from message_ix_models.model.structure import get_codes
from message_ix_models.model.transport import build, demand, report, structure
from message_ix_models.model.transport.ldv import TARGET
from message_ix_models.model.transport.testing import MARK, configure_build, make_mark
from message_ix_models.testing import bare_res
from message_ix_models.testing.check import (
    ContainsDataForParameters,
    Dump,
    HasCoords,
    HasUnits,
    Log,
    NoneMissing,
    NonNegative,
    Size,
    insert_checks,
)

if TYPE_CHECKING:
    from message_ix_models.testing.check import Check
    from message_ix_models.types import KeyLike

log = logging.getLogger(__name__)


@pytest.fixture
def N_node(request) -> int:
    """Expected number of nodes, by introspection of other parameter values."""
    if "build_kw" in request.fixturenames:
        build_kw = request.getfixturevalue("build_kw")

        # NB This could also be done by len(.model.structure.get_codelist(…)), but hard-
        #    coding is probably a little faster
        return {"ISR": 1, "R11": 11, "R12": 12, "R14": 14}[build_kw["regions"]]
    else:
        raise NotImplementedError


@pytest.mark.parametrize("years", [None, "A", "B"])
@pytest.mark.parametrize(
    "regions_arg, regions_exp",
    [
        ("R11", "R11"),
        ("R12", "R12"),
        ("R14", "R14"),
        ("ISR", "ISR"),
    ],
)
def test_make_spec(regions_arg, regions_exp, years):
    # The spec can be generated
    spec = structure.make_spec(regions_arg)

    # The required elements of the "node" set match the configuration
    nodes = get_codes(f"node/{regions_exp}")
    expected = list(map(str, nodes[nodes.index("World")].child))
    assert expected == spec["require"].set["node"]


@MARK[7]
@build.get_computer.minimum_version
@pytest.mark.parametrize(
    "regions, years, dummy_LDV, nonldv, solve",
    [
        param("R11", "B", True, None, False, marks=MARK[1]),
        param(  # 44s; 31 s with solve=False
            "R11",
            "A",
            True,
            None,
            True,
            marks=[
                MARK[1],
                pytest.mark.xfail(
                    raises=ixmp.ModelError,
                    reason="No supply of non-LDV commodities w/o IKARUS data",
                ),
            ],
        ),
        param("R11", "A", False, "IKARUS", False, marks=MARK[1]),  # 43 s
        param("R11", "A", False, "IKARUS", True, marks=[mark.slow, MARK[1]]),  # 74 s
        # R11, B
        param("R11", "B", False, "IKARUS", False, marks=[mark.slow, MARK[1]]),
        param("R11", "B", False, "IKARUS", True, marks=[mark.slow, MARK[1]]),
        # R12, B
        param("R12", "B", False, "IKARUS", True, marks=MARK[8]),
        # R14, A
        param(
            "R14",
            "A",
            False,
            "IKARUS",
            False,
            marks=[mark.slow, make_mark[2](genno.ComputationError)],
        ),
        # Pending iiasa/message_data#190
        param("ISR", "A", True, None, False, marks=MARK[3]),
    ],
)
def test_build_bare_res(
    request, tmp_path, test_context, regions, years, dummy_LDV: bool, nonldv, solve
):
    """.transport.build() works on the bare RES, and the model solves."""
    # Generate the relevant bare RES
    ctx = test_context
    ctx.update(regions=regions, years=years)
    scenario = bare_res(request, ctx)

    # Build succeeds without error
    options = {
        "data source": {"non-LDV": nonldv},
        "dummy_LDV": dummy_LDV,
        "dummy_supply": True,
    }
    build.main(ctx, scenario, options, fast=True)

    # dump_path = tmp_path / "scenario.xlsx"
    # log.info(f"Dump contents to {dump_path}")
    # scenario.to_excel(dump_path)

    if solve:
        scenario.solve(solve_options=dict(lpmethod=4, iis=1))

        # commented: Appears to be giving a false negative
        # # Use Reporting calculations to check the result
        # result = report.check(scenario)
        # assert result.all(), f"\n{result}"


@pytest.mark.ece_db
@pytest.mark.parametrize(
    "url",
    (
        "ixmp://ene-ixmp/CD_Links_SSP2_v2/baseline",
        "ixmp://ixmp-dev/ENGAGE_SSP2_v4.1.7/EN_NPi2020_1000f",
        "ixmp://ixmp-dev/ENGAGE_SSP2_v4.1.7/baseline",
        "ixmp://ixmp-dev/ENGAGE_SSP2_v4.1.7_ar5_gwp100/EN_NPi2020_1000_emif_new",
        "ixmp://ixmp-dev/MESSAGEix-GLOBIOM_R12_CHN/baseline#17",
        "ixmp://ixmp-dev/MESSAGEix-GLOBIOM_R12_CHN/baseline_macro#3",
        # Local clones of the above
        # "ixmp://clone-2021-06-09/ENGAGE_SSP2_v4.1.7/baseline",
        # "ixmp://clone-2021-06-09/ENGAGE_SSP2_v4.1.7/EN_NPi2020_1000f",
        # "ixmp://local/MESSAGEix-Transport on ENGAGE_SSP2_v4.1.7/baseline",
    ),
)
def test_build_existing(tmp_path, test_context, url, solve=False):
    """Test that model.transport.build works on certain existing scenarios.

    These are the ones listed in the documenation, at :ref:`transport-base-scenarios`.
    """
    ctx = test_context

    # Update the Context with the base scenario's `url`
    ctx.handle_cli_args(url=url)

    # Destination for built scenarios: uncomment one of
    # the platform prepared by the text fixture…
    ctx.dest_platform = copy(ctx.platform_info)
    # # or, a specific, named platform.
    # ctx.dest_platform = dict(name="local")

    # New model name for the destination scenario
    ctx.dest_scenario = copy(ctx.scenario_info)
    ctx.dest_scenario["model"] = f"{ctx.dest_scenario['model']} +transport"

    # Clone the base scenario to the test platform
    scenario = ctx.clone_to_dest(create=False)
    mp = scenario.platform

    # Build succeeds without error
    build.main(ctx, scenario, fast=True)

    # commented: slow
    # dump_path = tmp_path / "scenario.xlsx"
    # log.info(f"Dump contents to {dump_path}")
    # scenario.to_excel(dump_path)

    if solve:
        scenario.solve(solve_options=dict(lpmethod=4))

        # Use Reporting calculations to check the result
        result = report.check(scenario)
        assert result.all(), f"\n{result}"

    del mp


CHECKS: dict["KeyLike", Collection["Check"]] = {
    "broadcast:t-c-l:transport+input": (HasUnits("dimensionless"),),
    "broadcast:t-c-l:transport+output": (
        HasUnits("dimensionless"),
        HasCoords({"commodity": ["transport F RAIL vehicle"]}),
    ),
    "output::F+ixmp": (
        HasCoords(
            {"commodity": ["transport F RAIL vehicle", "transport F ROAD vehicle"]}
        ),
    ),
    "other::F+ixmp": (HasCoords({"technology": ["f rail electr"]}),),
    "transport F::ixmp": (
        ContainsDataForParameters(
            {"capacity_factor", "input", "output", "technical_lifetime"}
        ),
        # HasCoords({"technology": ["f rail electr"]}),
    ),
    #
    # The following are intermediate checks formerly in .test_demand.test_exo
    "mode share:n-t-y:base": (HasUnits(""),),
    "mode share:n-t-y": (HasUnits(""),),
    "population:n-y": (HasUnits("Mpassenger"),),
    "cg share:n-y-cg": (HasUnits(""),),
    "GDP:n-y:PPP+capita": (HasUnits("kUSD / passenger / year"),),
    "GDP:n-y:PPP+capita+index": (HasUnits(""),),
    "votm:n-y": (HasUnits(""),),
    "PRICE_COMMODITY:n-c-y:transport+smooth": (HasUnits("USD / km"),),
    "cost:n-y-c-t": (HasUnits("USD / km"),),
    # These units are implied by the test of "transport pdt:*":
    # "transport pdt:n-y:total" [=] Mm / year
    demand.pdt_nyt + "1": (HasUnits("passenger km / year"),),
    demand.ldv_ny + "total": (HasUnits("Gp km / a"),),
    # FIXME Handle dimensionality instead of exact units
    # demand.ldv_nycg: (HasUnits({"[length]": 1, "[passenger]": 1, "[time]": -1}),),
    "pdt factor:n-y-t": (HasUnits(""),),
    # "fv factor:n-y": (HasUnits(""),),  # Fails: this key no longer exists
    # "fv:n:advance": (HasUnits(""),),  # Fails: only fuzzed data in message-ix-models
    demand.fv_cny: (HasUnits("Gt km"),),
    #
    # Exogenous demand calculation succeeds
    "transport demand::ixmp": (
        # Data is returned for the demand parameter only
        ContainsDataForParameters({"demand"}),
        HasCoords({"level": ["useful"]}),
        # Certain labels are specifically excluded/dropped in the calculation
        HasCoords(
            {"commodity": ["transport pax ldv", "transport F WATER"]}, inverse=True
        ),
        # No negative values
        NonNegative(),
        # …plus default NoneMissing
    ),
    #
    # The following partly replicates .test_ldv.test_get_ldv_data()
    TARGET: (
        ContainsDataForParameters(
            {
                "bound_new_capacity_lo",
                "bound_new_capacity_up",
                "capacity_factor",
                "emission_factor",
                "fix_cost",
                "growth_activity_lo",
                "growth_activity_up",
                "historical_new_capacity",
                "initial_activity_up",
                "input",
                "inv_cost",
                "output",
                "relation_activity",
                "technical_lifetime",
                "var_cost",
            }
        ),
    ),
}


@build.get_computer.minimum_version
@pytest.mark.parametrize(
    "build_kw",
    (
        dict(regions="R11", years="A", options=dict()),
        dict(regions="R11", years="B", options=dict()),
        dict(regions="R11", years="B", options=dict(futures_scenario="A---")),
        dict(regions="R11", years="B", options=dict(futures_scenario="debug")),
        dict(regions="R12", years="B", options=dict()),
        dict(regions="R12", years="B", options=dict(navigate_scenario="act+ele+tec")),
        dict(regions="R14", years="B", options=dict()),
        param(dict(regions="ISR", years="A", options=dict()), marks=MARK[3]),
    ),
)
def test_debug(
    test_context,
    tmp_path,
    build_kw,
    N_node,
    verbosity: Literal[0, 1, 2, 3] = 2,  # NB Increase this to show more verbose output
):
    """Debug particular calculations in the transport build process."""
    # Get a Computer prepared to build the model with the given options
    c, info = configure_build(test_context, tmp_path=tmp_path, **build_kw)

    # Construct a list of common checks
    verbose: dict[int, list["Check"]] = {
        0: [],
        1: [Log(7)],
        2: [Log(None)],
        3: [Dump(tmp_path)],
    }
    common = [Size({"n": N_node}), NoneMissing()] + verbose[verbosity]

    # Insert key-specific and common checks
    k = "test_debug"
    result = insert_checks(c, k, CHECKS, common)

    # Show what will be computed
    if verbosity == 2:
        print(c.describe(k))

    # Compute the test key
    c.get(k)

    assert result, "1 or more checks failed"
