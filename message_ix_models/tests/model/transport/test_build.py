import pytest

from message_data import testing
from message_data.model.transport import build, report
from message_data.tools import set_info


@pytest.mark.parametrize(
    "regions_arg, regions_exp",
    [(None, "R14"), ("ISR", "ISR"), ("R11", "R11"), ("R14", "R14")],
)
def test_get_spec(transport_context_f, regions_arg, regions_exp):
    ctx = transport_context_f
    if regions_arg:
        # Non-default value
        ctx.regions = regions_arg

    # The spec can be generated
    spec = build.get_spec(ctx)

    # The required elements of the "node" set match the configuration
    nodes = set_info(f"node/{regions_exp}")
    exp = list(map(str, nodes[nodes.index("World")].child))
    assert spec["require"].set["node"] == exp


@pytest.mark.parametrize(
    "ldv, nonldv, solve",
    [
        (None, None, False),
        (None, None, True),
        ("US-TIMES MA3T", "IKARUS", False),
        ("US-TIMES MA3T", "IKARUS", True),
    ],
)
@pytest.mark.parametrize("regions", ["R11"])
def test_build_bare_res(request, transport_context_f, ldv, nonldv, solve, regions):
    """Test that model.transport.build works on the bare RES, and the model solves."""
    # Pre-load transport config/metadata
    ctx = transport_context_f
    ctx.regions = "R11"

    # Manually modify some of the configuration per test parameters
    ctx["transport config"]["data source"]["LDV"] = ldv
    ctx["transport config"]["data source"]["non-LDV"] = nonldv

    # Generate the relevant bare RES
    scenario = testing.bare_res(request, ctx)

    # Build succeeds without error
    build.main(ctx, scenario, fast=True)

    if solve:
        scenario.solve(solve_options=dict(lpmethod=4))

        # Use Reporting calculations to check the result
        result = report.check(scenario)
        assert result.all(), f"\n{result}"
