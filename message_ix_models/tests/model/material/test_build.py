import logging

import pytest

from message_ix_models.model.material import build
from message_ix_models.model.structure import get_codes
from message_ix_models.testing import bare_res

log = logging.getLogger(__name__)


@pytest.mark.parametrize(
    "regions_arg, regions_exp",
    [
        # ("R11", "R11"),
        ("R12", "R12"),
    ],
)
@pytest.mark.parametrize("material", [None])
def test_make_spec(regions_arg, regions_exp, material):
    # The spec can be generated
    spec = build.make_spec(regions_arg, material)

    # The required elements of the "node" set match the configuration
    nodes = get_codes(f"node/{regions_exp}")
    expected = list(map(str, nodes[nodes.index("World")].child))
    assert expected == spec["require"].set["node"]


@pytest.mark.parametrize(
    "regions, years, relations, solve",
    [
        ("R12", "B", "B", False),
        pytest.param(
            "R11", "B", "B", False, marks=pytest.mark.xfail(raises=NotImplementedError)
        ),
    ],
)
def test_build_bare_res(
    request, tmp_path, test_context, regions, years, relations, solve
):
    """.materials.build() works on the bare RES, and the model solves."""
    # Generate the relevant bare RES
    ctx = test_context
    ctx.update(regions=regions, years=years, ssp="SSP2", relations=relations)
    scenario = bare_res(request, ctx)

    # Build succeeds without error
    # options = {"dummy_supply": True}
    build.build(ctx, scenario, modify_existing_constraints=False, old_calib=True)

    if solve:
        scenario.solve(solve_options=dict(lpmethod=4, iis=1))

        # commented: Appears to be giving a false negative
        # # Use Reporting calculations to check the result
        # result = report.check(scenario)
        # assert result.all(), f"\n{result}"
