import logging
from copy import copy
from typing import TYPE_CHECKING, Literal

import genno
import ixmp
import pytest
from iam_units import registry
from pytest import mark, param

from message_ix_models.model.structure import get_codes
from message_ix_models.model.transport import (
    Config,
    build,
    constraint,
    disutility,
    freight,
    key,
    ldv,
    other,
    passenger,
    report,
    structure,
)
from message_ix_models.model.transport.testing import (
    MARK,
    assert_units,
    configure_build,
    make_mark,
)
from message_ix_models.testing import bare_res
from message_ix_models.testing.check import (
    Check,
    ContainsDataForParameters,
    HasCoords,
    HasUnits,
    NoneMissing,
    NonNegative,
    Size,
    insert_checks,
    verbose_check,
)

if TYPE_CHECKING:
    from message_ix_models.testing.check import Check
    from message_ix_models.types import KeyLike

log = logging.getLogger(__name__)


class LDV_PHEV_input(Check):
    """Check magnitudes of input energy intensities of LDV PHEV technologies.

    There are three conditions:

    1. Electricity input to the PHEV is less than electricity input to BEV.
    2. Light oil input to the PHEV is less than light oil input to an ICEV.
    3. Total energy input to the PHEV is between the BEV and ICEV.
    """

    # Technologies to check
    _t = ("ELC_100", "PHEV_ptrp", "ICE_conv")

    types = (dict,)

    def run(self, obj):
        def _join_levels(df):
            """Join multi-index labels to a single str."""
            return df.set_axis(
                df.columns.to_series().apply(lambda v: " ".join(map(str, v))), axis=1
            )

        t0, t1, t2 = self._t
        tmp = (
            obj["input"]
            .query(f"technology in {self._t!r}")
            .set_index(["node_loc", "year_vtg", "year_act"])
            .pivot(columns=["technology", "commodity"], values="value")
            .pipe(_join_levels)
            .eval(f"`{t1}` = `{t1} electr` + `{t1} lightoil`")
            .eval(f"c1 = `{t1} electr` <= `{t0} electr`")
            .eval(f"c2 = `{t1} lightoil` <= `{t2} lightoil`")
            .eval(f"c3 = `{t0} electr` < `{t1}` < `{t2} lightoil`")
            .eval("cond = c1 & c2 & c3")
        )

        if not tmp.cond.all():
            fail = tmp[~tmp.cond]
            return (
                False,
                f"LDV input does not satisfy conditions in {len(fail)} cases:\n"
                f"{fail.to_string()}",
            )
        else:
            return True, "LDV input satisfies conditions"


class Passenger(Check):
    """Check data from :mod:`.transport.passenger`.

    These assertions were formerly in a test at
    :py:`.tests.transport.test_data.test_get_non_ldv_data()`.
    """

    types = (dict,)

    def run(self, obj):
        # Input data have expected units
        df_input = obj["input"]
        mask0 = df_input["technology"].str.endswith(" usage")
        mask1 = df_input["technology"].str.startswith("transport other")

        assert_units(df_input[mask0], registry("Gv km"))
        if mask1.any():
            assert_units(df_input[mask1], registry("GWa"))
        assert_units(df_input[~(mask0 | mask1)], registry("1.0 GWa / (Gv km)"))

        # Output data exist for all non-LDV modes
        df_output = obj["output"]
        modes = list(filter(lambda m: m != "LDV", Config().demand_modes))
        obs = set(df_output["commodity"].unique())
        assert len(modes) * 2 == len(obs)

        # Output data have expected units
        mask = df_output["technology"].str.endswith(" usage")
        assert_units(df_output[~mask], {"[vehicle]": 1, "[length]": 1})
        assert_units(df_output[mask], {"[passenger]": 1, "[length]": 1})

        return True, "Passenger non-LDV input and output satisfy conditions"


#: Inline checks for :func:`.test_debug`.
CHECKS: dict["KeyLike", tuple[Check, ...]] = {
    # .build.add_structure()
    "broadcast:t-c-l:transport+input": (HasUnits("dimensionless"),),
    "broadcast:t-c-l:transport+output": (
        HasUnits("dimensionless"),
        HasCoords({"commodity": ["transport F RAIL vehicle"]}),
    ),
    #
    # From .constraint
    constraint.TARGET: (
        ContainsDataForParameters(
            {
                "bound_new_capacity_up",
                "growth_activity_lo",
                "growth_activity_up",
                "growth_new_capacity_lo",
                "growth_new_capacity_up",
                "initial_activity_lo",
                "initial_activity_up",
                "initial_new_capacity_lo",
                "initial_new_capacity_up",
            }
        ),
    ),
    #
    # From .freight
    # The following replicates a deleted .transport.test_data.test_get_freight_data()
    freight.TARGET: (
        ContainsDataForParameters(
            {
                "demand",
                "capacity_factor",
                "input",
                "output",
                "technical_lifetime",
            }
        ),
        # HasCoords({"technology": ["f rail electr"]}),
    ),
    "output::F+ixmp": (
        HasCoords(
            {"commodity": ["transport F RAIL vehicle", "transport F ROAD vehicle"]}
        ),
    ),
    # .freight.other()
    "other::F+ixmp": (HasCoords({"technology": ["f rail electr"]}),),
    #
    # The following are intermediate checks formerly in .test_demand.test_exo
    "mode share:n-t-y:base": (HasUnits(""),),
    "mode share:n-t-y": (HasUnits(""),),
    "population:n-y": (HasUnits("Mpassenger"),),
    "cg share:n-y-cg": (HasUnits(""),),
    "GDP:n-y:PPP+capita": (HasUnits("kUSD / passenger / year"),),
    "votm:n-y": (HasUnits(""),),
    key.price.base: (HasUnits("USD / km"),),
    "cost:n-y-c-t": (HasUnits("USD / km"),),
    key.pdt_nyt[0]: (HasUnits("passenger km / year"),),
    key.pdt_nyt[1]: (HasUnits("passenger km / year"),),
    key.ldv_ny + "total": (HasUnits("Gp km / a"),),
    # FIXME Handle dimensionality instead of exact units
    # demand.ldv_nycg: (HasUnits({"[length]": 1, "[passenger]": 1, "[time]": -1}),),
    "pdt factor:n-y-t": (HasUnits(""),),
    # "fv factor:n-y": (HasUnits(""),),  # Fails: this key no longer exists
    # "fv:n:advance": (HasUnits(""),),  # Fails: only fuzzed data in message-ix-models
    key.fv_cny: (HasUnits("Gt km"),),
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
    f"input{ldv.Li}": (LDV_PHEV_input(),),
    # .disutility.prepare_computer()
    "disutility:n-cg-t-y": (Size(dict(cg=27 * 12)),),
    disutility.TARGET: (ContainsDataForParameters({"input"}),),
    #
    "historical_new_capacity::LDV+ixmp": (HasUnits("million * v / a"),),
    # The following partly replicates .test_ldv.test_get_ldv_data()
    ldv.TARGET: (
        ContainsDataForParameters(
            {
                "bound_new_capacity_lo",
                "bound_new_capacity_up",
                "capacity_factor",
                "emission_factor",
                "fix_cost",
                "historical_new_capacity",
                "input",
                "inv_cost",
                "output",
                "relation_activity",
                "technical_lifetime",
                "var_cost",
            }
        ),
    ),
    other.TARGET: (
        ContainsDataForParameters({"bound_activity_lo", "bound_activity_up", "input"}),
    ),
    passenger.TARGET: (
        ContainsDataForParameters(
            {
                "bound_activity_lo",  # From .passenger.other(). For R11 this is empty.
                "bound_activity_up",  # act-non_ldv.csv via .passenger.bound_activity()
                "capacity_factor",
                # "emission_factor",
                "fix_cost",
                "input",
                "inv_cost",
                "output",
                # "relation_activity",
                "technical_lifetime",
                "var_cost",
            }
        ),
        Passenger(),
    ),
}


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


@MARK[10]
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
            marks=[mark.slow, make_mark[2](genno.ComputationError)],
        ),
        # Pending iiasa/message_data#190
        param("ISR", "A", True, None, False, marks=MARK[3]),
    ],
)
def test_bare_res(
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
    "build_kw",
    (
        # commented: Reduce runtimes of GitHub Actions jobs
        # dict(regions="R11", years="A"),
        # dict(regions="R11", years="B"),
        # dict(regions="R11", years="B", options=dict(futures_scenario="A---")),
        # dict(regions="R11", years="B", options=dict(futures_scenario="debug")),
        dict(regions="R12", years="B"),
        # dict(regions="R12", years="B", options=dict(navigate_scenario="act+ele+tec")),
        dict(regions="R12", years="B", options=dict(project={"LED": True})),
        # param(dict(regions="R14", years="B"), marks=MARK[9]),
        # param(dict(regions="ISR", years="A"), marks=MARK[3]),
    ),
)
def test_debug(
    test_context, tmp_path, build_kw, N_node, *, verbosity: Literal[0, 1, 2, 3] = 0
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
    c, info = configure_build(test_context, tmp_path=tmp_path, **build_kw)

    # Modify CHECKS according to the settings
    checks = CHECKS.copy()
    if test_context.model.regions == "R12":
        checks["transport::O+ixmp"] = (
            ContainsDataForParameters(
                {"bound_activity_lo", "bound_activity_up", "input"}
            ),
        )

    # Has exactly the periods (y) in the model horizon
    k = "disutility:n-cg-t-y"
    checks[k] = checks[k] + (HasCoords({"y": info.Y}),)

    # Construct a list of common checks to be appended to every value in `checks`
    common = [Size({"n": N_node}), NoneMissing()] + verbose_check(verbosity, tmp_path)

    # Insert key-specific and common checks
    k = "test_debug"
    result = insert_checks(c, k, checks, common)

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

    assert result, "1 or more checks failed"
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
