import logging
import platform

import pytest

from message_ix_models.model.material import build
from message_ix_models.model.structure import get_codes
from message_ix_models.testing import GHA, bare_res

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


# Conditions and marks for one case of test_build_bare_res() on GitHub Actions
_C1 = GHA and platform.system() == "Windows"
_C2 = GHA and platform.system() == "Darwin"
_MARKS = [
    # On Windows, the test frequently fails; if it passes, the run time is ~1200
    # seconds (20 minutes). Always SKIP.
    pytest.mark.skipif(_C1, reason="Slow/Java heap space error"),
    # On macOS, the test occasionally times out the run (~360 minutes = 21600 seconds).
    # When it passes, it does so in ~700 seconds.
    #
    # # Time out after a +20% margin, and XFAIL if this timeout occurs.
    # pytest.mark.xfail(_C2, reason="Times out"),
    # pytest.mark.timeout(700 * 1.2 if _C2 else 0),
    #
    # Skip unconditionally. See
    # https://github.com/iiasa/message-ix-models/pull/346#issuecomment-2873056272
    pytest.mark.skipif(_C2, reason="Times out"),
    pytest.mark.xfail(
        condition=GHA and platform.system() == "Linux",
        raises=KeyError,
        reason="Temporary, for https://github.com/iiasa/message-ix-models/pull/345, "
        "pending adjustment to access of context['ssp']",
    ),
]


@pytest.mark.usefixtures("ssp_user_data")
@pytest.mark.parametrize(
    "regions, years, relations, solve",
    [
        pytest.param("R12", "B", "B", False, marks=_MARKS),
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
