import logging
from copy import copy

import genno
import ixmp
import pytest
from genno import Quantity
from genno.testing import assert_units
from pytest import mark, param

from message_ix_models.model.structure import get_codes
from message_ix_models.model.transport import build, report, structure
from message_ix_models.model.transport.testing import MARK, configure_build
from message_ix_models.testing import bare_res

log = logging.getLogger(__name__)


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
        ("R12", "B", False, "IKARUS", True),
        # R14, A
        param(
            "R14",
            "A",
            False,
            "IKARUS",
            False,
            marks=[mark.slow, MARK[2](genno.ComputationError)],
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


@build.get_computer.minimum_version
@pytest.mark.parametrize(
    "regions, years, N_node, options",
    [
        ("R12", "B", 12, dict()),
    ],
)
def test_debug(test_context, tmp_path, regions, years, N_node, options):
    """Debug particular calculations in the transport build process."""
    # Import certain keys
    # from message_ix_models.model.transport.key import pdt_ny

    c, info = configure_build(
        test_context, tmp_path=tmp_path, regions=regions, years=years, options=options
    )

    fail = False  # Sentinel value for deferred failure assertion

    # Check that some keys (a) can be computed without error and (b) have correct units
    # commented: these are slow because they repeat some calculations many times.
    # Uncommented as needed for debugging
    for key, unit in (
        # Uncomment and modify these line(s) to check certain values
        # ("transport nonldv::ixmp", None),
    ):
        print(f"\n\n-- {key} --\n\n")
        print(c.describe(key))

        # Quantity can be computed
        result = c.get(key)

        # # Display the entire `result` object
        # print(f"{result = }")

        if isinstance(result, Quantity):
            print(result.to_series().to_string())

            # Quantity has the expected units
            assert_units(result, unit)

            # Quantity has the expected size on the n/node dimension
            assert N_node == len(result.coords["n"]), result.coords["n"].data

            # commented: dump to a temporary path for inspection
            # fn = f"{key.replace(' ', '-')}-{hash(tuple(options.items()))}"
            # dump = tmp_path.joinpath(fn).with_suffix(".csv")
            # print(f"Dumped to {dump}")
            # qty.to_series().to_csv(dump)
        elif isinstance(result, dict):
            for k, v in sorted(result.items()):
                print(
                    f"=== {k} ({len(v)} obs) ===",
                    v.head().to_string(),  # Initial rows
                    "...",
                    v.tail().to_string(),  # Final rows
                    # v.to_string(),  # Entire value
                    f"=== {k} ({len(v)} obs) ===",
                    sep="\n",
                )
                # print(v.tail().to_string())

                # Write to file
                # if k == "output":
                #     v.to_csv("debug-output.csv", index=False)

                missing = v.isna()
                if missing.any(axis=None):
                    print("… missing values")
                    fail = True  # Fail later

        assert not fail  # Any failure in the above loop

    assert not fail  # Any failure in the above loop
