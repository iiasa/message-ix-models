import logging
from copy import copy

import pytest
from pytest import mark, param

from message_ix_models import testing
from message_ix_models.model.structure import get_codes

from message_data.model.transport import build, report
from message_data.testing import NIE

log = logging.getLogger(__name__)


@pytest.mark.parametrize("years", ["A", "B"])
@pytest.mark.parametrize(
    "regions_arg, regions_exp",
    [(None, "R14"), ("ISR", "ISR"), ("R11", "R11"), ("R14", "R14")],
)
def test_get_spec(transport_context_f, regions_arg, regions_exp, years):
    ctx = transport_context_f
    if regions_arg:
        # Non-default value
        ctx.regions = regions_arg

    ctx.years = years

    # The spec can be generated
    spec = build.get_spec(ctx)

    # The required elements of the "node" set match the configuration
    nodes = get_codes(f"node/{regions_exp}")
    exp = list(map(str, nodes[nodes.index("World")].child))
    assert spec["require"].set["node"] == exp


@pytest.mark.parametrize(
    "regions, years, ldv, nonldv, solve",
    [
        ("R11", "A", None, None, False),  # 31 s
        ("R11", "B", None, None, False),
        param("R11", "A", None, None, True, marks=mark.slow),  # 44 s
        param("R11", "A", "US-TIMES MA3T", "IKARUS", False, marks=mark.slow),  # 43 s
        param("R11", "A", "US-TIMES MA3T", "IKARUS", True, marks=mark.slow),  # 74 s
        # Non-R11 configurations currently fail
        param("R14", "A", None, None, False, marks=NIE),
        param("ISR", "A", None, None, False, marks=NIE),
        # Periods "B" currently fail
        param("R11", "B", "US-TIMES MA3T", "IKARUS", False, marks=(mark.slow, NIE)),
    ],
)
def test_build_bare_res(
    request, tmp_path, transport_context_f, regions, years, ldv, nonldv, solve
):
    """Test that model.transport.build works on the bare RES, and the model solves."""
    # Pre-load transport config/metadata
    ctx = transport_context_f
    ctx.regions = regions
    ctx.years = years

    # Manually modify some of the configuration per test parameters
    ctx["transport config"]["data source"]["LDV"] = ldv
    ctx["transport config"]["data source"]["non-LDV"] = nonldv

    # Generate the relevant bare RES
    scenario = testing.bare_res(request, ctx)

    # Build succeeds without error
    build.main(ctx, scenario, fast=True)

    dump_path = tmp_path / "scenario.xlsx"
    log.info(f"Dump contents to {dump_path}")
    scenario.to_excel(dump_path)

    if solve:
        scenario.solve(solve_options=dict(lpmethod=4))

        # Use Reporting calculations to check the result
        result = report.check(scenario)
        assert result.all(), f"\n{result}"


@pytest.mark.ece_db
@pytest.mark.parametrize(
    "url",
    (
        "ixmp://ene-ixmp/CD_Links_SSP2_v2/baseline",
        "ixmp://ixmp-dev/ENGAGE_SSP2_v4.1.7_ar5_gwp100/EN_NPi2020_1000_emif_new",
        "ixmp://ixmp-dev/MESSAGEix-GLOBIOM_R12_CHN/baseline#17",
        "ixmp://ixmp-dev/MESSAGEix-GLOBIOM_R12_CHN/baseline_macro#3",
    ),
)
def test_build_existing(tmp_path, transport_context_f, url, solve=False):
    """Test that model.transport.build works on certain existing scenarios.

    These are the ones listed in the documenation, at :ref:`transport-base-scenarios`.
    """
    ctx = transport_context_f

    # Get the platform prepared by the text fixture
    ctx.dest_platform = copy(ctx.platform)

    # Update the Context with the base scenario's `url`
    ctx.handle_cli_args(url=url)

    # Clone the base scenario to the test platform
    scenario = ctx.clone_to_dest()

    # Build succeeds without error
    build.main(ctx, scenario, fast=True)

    dump_path = tmp_path / "scenario.xlsx"
    log.info(f"Dump contents to {dump_path}")
    scenario.to_excel(dump_path)

    if solve:
        scenario.solve(solve_options=dict(lpmethod=4))

        # Use Reporting calculations to check the result
        result = report.check(scenario)
        assert result.all(), f"\n{result}"
