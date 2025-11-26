import logging
from collections.abc import Iterator
from copy import copy
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import genno
import ixmp
import pytest
from pytest import mark, param

from message_ix_models import Context
from message_ix_models.model.structure import get_codes
from message_ix_models.model.transport import (
    CL_SCENARIO,
    build,
    check,
    report,
    structure,
)
from message_ix_models.model.transport.testing import MARK, configure_build, make_mark
from message_ix_models.testing import bare_res

if TYPE_CHECKING:
    from sdmx.model.common import Code


log = logging.getLogger(__name__)


@pytest.fixture
def N_node(request) -> int:
    """Expected number of nodes, by introspection of other parameter values."""
    if "build_kw" in request.fixturenames:
        regions = request.getfixturevalue("build_kw")["regions"]
    elif "regions" in request.fixturenames:
        regions = request.getfixturevalue("regions")

    # NB This could also be done by len(.model.structure.get_codelist(…)), but hard-
    #    coding is probably a little faster
    return {"ISR": 1, "R11": 11, "R12": 12, "R14": 14}[regions]


@pytest.fixture(scope="session")
def scenario_code() -> Iterator["Code"]:
    return CL_SCENARIO.get()["SSP2"]


@MARK[10]
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
        ("R12", "B", False, "IKARUS", True),
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
def test_bare_res(
    request: "pytest.FixtureRequest",
    tmp_path: "Path",
    test_context: "Context",
    scenario_code: "Code",
    regions: str,
    years: str,
    dummy_LDV: bool,
    nonldv: str,
    solve: bool,
) -> None:
    """.transport.build() works on the bare RES, and the model solves."""
    # Generate the relevant bare RES
    ctx = test_context
    ctx.update(regions=regions, years=years)
    scenario = bare_res(request, ctx)

    # Build succeeds without error
    options = {
        "code": scenario_code,
        "data source": {"non-LDV": nonldv},
        "dummy_LDV": dummy_LDV,
        "dummy_supply": True,
    }
    build.main(ctx, scenario, options)

    # dump_path = tmp_path / "scenario.xlsx"
    # log.info(f"Dump contents to {dump_path}")
    # scenario.to_excel(dump_path)

    if solve:
        scenario.solve(solve_options=dict(lpmethod=4, iis=1))

        # commented: Appears to be giving a false negative
        # # Use Reporting calculations to check the result
        # result = report.check(scenario)
        # assert result.all(), f"\n{result}"


@build.get_computer.minimum_version
@MARK[10]
@pytest.mark.parametrize(
    "regions, years, options",
    (
        # commented: Reduce runtimes of GitHub Actions jobs
        # ("R11", "A", {}),
        # ("R11", "B", {}),
        # ("R11", "B", dict(futures_scenario="A---")),
        # ("R11", "B", dict(futures_scenario="debug")),
        ("R12", "B", dict(code="SSP2")),
        ("R12", "B", dict(code="SSP2 tax")),
        ("R12", "B", dict(code="SSP2 exo price")),
        # ("R12", "B", dict(navigate_scenario="act+ele+tec")),
        ("R12", "B", dict(code="LED-SSP2")),
        ("R12", "B", dict(code="EDITS-CA")),
        ("R12", "B", dict(code="DIGSY-BEST-C")),
        # param("R14", "B", {}, marks=MARK[9]),
        # param("ISR", "A", {}, marks=MARK[3]),
    ),
)
def test_debug(
    test_context: Context,
    tmp_path: Path,
    regions: str,
    years: str,
    options: dict,
    N_node: int,
    *,
    verbosity: Literal[0, 1, 2, 3] = 0,
):
    """Check and debug particular steps in the transport build process.

    By default, this test applies all of the :data:`.CHECKS` using
    :func:`.insert_checks` and then runs the entire build process, asserting that all
    the checks pass.

    It can also be used by uncommenting and adjusting the lines marked :py:`# DEBUG` to
    inspect the behaviour of a sub-graph of the :class:`.Computer`. Such changes
    **should not** be committed.

    Parameters
    ----------
    verbosity : int
        Passed to :func:`.verbose_check`.
    """
    # Get a Computer prepared to build the model with the given options
    c, _ = configure_build(
        test_context, regions=regions, years=years, tmp_path=tmp_path, options=options
    )

    # Insert key-specific and common checks
    result = check.insert(c, N_node, verbosity, tmp_path)

    k = "test_debug"
    # DEBUG Show and compute a different key
    # k = key.pdt_cny

    # Show what will be computed
    # verbosity = True  # DEBUG Force printing the description
    if verbosity:
        print(c.describe(k))

    # return  # DEBUG Exit before doing any computation

    # Compute the test key
    tmp = c.get(k)

    # DEBUG Handle a subset of the result for inspection
    # print(tmp)

    result.assert_all_passed()
    del tmp


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
def test_existing(tmp_path, test_context, url, solve=False):
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
    build.main(ctx, scenario)

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
