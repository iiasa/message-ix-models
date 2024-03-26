"""Tests for :mod:`message_ix_models.report`."""

from importlib.metadata import version

import numpy as np
import pandas as pd
import pandas.testing as pdt
import pytest
from ixmp.testing import assert_logs

from message_ix_models import ScenarioInfo, testing
from message_ix_models.report import prepare_reporter, register, report, util
from message_ix_models.report.sim import add_simulated_solution, to_simulate
from message_ix_models.util import package_data_path

# Minimal reporting configuration for testing
MIN_CONFIG = {
    "units": {
        "replace": {"???": ""},
    },
}

MARK = (
    # Used in test_operator
    pytest.mark.xfail(
        condition=version("message_ix") < "3.5",
        reason="Not supported with message_ix < 3.5",
    ),
)


def test_register(caplog):
    # Exception raised for unfindable module
    with pytest.raises(ModuleNotFoundError):
        register("foo.bar")

    # Adding a callback of the same name twice triggers a log message
    def _cb(*args):
        pass

    register(_cb)
    with assert_logs(
        caplog, "Already registered: <function test_register.<locals>._cb"
    ):
        register(_cb)


@prepare_reporter.minimum_version
def test_report_bare_res(request, tmp_path, test_context):
    """Prepare and run the standard MESSAGE-GLOBIOM reporting on a bare RES."""
    scenario = testing.bare_res(request, test_context, solved=True)
    test_context.set_scenario(scenario)

    test_context.report.update(
        from_file="global.yaml",
        # key="message::default",
        # Use a key that doesn't access model solution data
        key="y0",
        output_dir=tmp_path,
    )

    # Prepare the reporter and compute the result
    report(test_context)


@prepare_reporter.minimum_version
def test_report_deprecated(caplog, request, tmp_path, test_context):
    # Create a target scenario
    scenario = testing.bare_res(request, test_context, solved=False)
    test_context.set_scenario(scenario)

    # Use a key that doesn't access model solution data
    test_context.report.key = "y0"

    # Set dry_run = True to not actually perform any calculations or modifications
    test_context.dry_run = True
    # Call succeeds, raises a warning
    with pytest.warns(DeprecationWarning, match="pass a Context instead"):
        report(scenario, tmp_path)

    # Invalid call warns *and* raises TypeError
    with pytest.raises(TypeError), pytest.warns(DeprecationWarning):
        report(scenario, tmp_path, "foo")


@pytest.mark.xfail(raises=ModuleNotFoundError, reason="Requires message_data")
def test_report_legacy(caplog, request, tmp_path, test_context):
    """Legacy reporting can be invoked via :func:`message_ix_models.report.report`."""
    # Create a target scenario
    scenario = testing.bare_res(request, test_context, solved=False)
    test_context.set_scenario(scenario)
    # Set dry_run = True to not actually perform any calculations or modifications
    test_context.dry_run = True
    # Ensure the legacy reporting is used, with default settings
    test_context.report.legacy["use"] = True

    # Call succeeds
    report(test_context)

    # Dry-run message is logged
    assert "DRY RUN" in caplog.messages[-1]
    caplog.clear()

    # Other deprecated usage

    # As called in .model.cli.new_baseline() and .model.create.solve(), with path as a
    # positional argument
    legacy_arg = dict(
        use=True,
        ref_sol="True",  # Must be literal "True" or "False"
        merge_hist=True,
        xlsx=test_context.get_local_path("rep_template.xlsx"),
    )
    with (
        pytest.warns(DeprecationWarning, match="pass a Context instead"),
        pytest.raises(TypeError, match="unexpected keyword argument 'xlsx'"),
    ):
        report(scenario, tmp_path, legacy=legacy_arg)

    # As called in .projects.covid.scenario_runner.ScenarioRunner.solve(), with path as
    # a keyword argument
    with (
        pytest.warns(DeprecationWarning, match="pass a Context instead"),
        pytest.raises(TypeError, match="unexpected keyword argument 'xlsx'"),
    ):
        report(scenario, path=tmp_path, legacy=legacy_arg)


# Common data for tests
DATA_INV_COST = pd.DataFrame(
    [
        ["R11_NAM", "coal_ppl", "2010", 10.5, "USD"],
        ["R11_LAM", "coal_ppl", "2010", 9.5, "USD"],
    ],
    columns="node_loc technology year_vtg value unit".split(),
)

INV_COST_CONFIG = dict(
    iamc=[
        dict(
            variable="Investment Cost",
            base="inv_cost:nl-t-yv",
            rename=dict(nl="region", yv="year"),
            collapse=dict(var=["t"]),
            unit="EUR_2005",
        )
    ]
)


@prepare_reporter.minimum_version
@pytest.mark.parametrize("regions", ["R11"])
def test_apply_units(request, test_context, regions):
    test_context.regions = regions
    bare_res = testing.bare_res(request, test_context, solved=True)

    qty = "inv_cost"

    # Create a temporary config dict
    config = MIN_CONFIG.copy()

    # Prepare the reporter
    test_context.report.update(genno_config=config, key=qty)
    reporter, key = prepare_reporter(test_context, bare_res)

    # Add some data to the scenario
    inv_cost = DATA_INV_COST.copy()
    bare_res.remove_solution()
    with bare_res.transact():
        bare_res.add_par("inv_cost", inv_cost)
    bare_res.solve()

    # Units are retrieved
    USD_2005 = reporter.unit_registry.Unit("USD_2005")
    assert USD_2005 == reporter.get(key).units

    # Add data with units that will be discarded
    inv_cost["unit"] = ["USD", "kg"]
    bare_res.remove_solution()
    with bare_res.transact():
        bare_res.add_par("inv_cost", inv_cost)
    bare_res.solve()

    # Units are discarded
    assert "dimensionless" == str(reporter.get(key).units)

    # Update configuration, re-create the reporter
    test_context.report.genno_config["units"]["apply"] = {"inv_cost": "USD"}
    reporter, key = prepare_reporter(test_context, bare_res)

    # Units are applied
    assert USD_2005 == reporter.get(key).units

    # Update configuration, re-create the reporter
    test_context.report.genno_config.update(INV_COST_CONFIG)
    reporter, key = prepare_reporter(test_context, bare_res)

    # Units are converted
    df = reporter.get("Investment Cost::iamc").as_pandas()
    assert ["EUR_2005"] == df["unit"].unique()


@pytest.mark.xfail(reason="Incomplete")
def test_cli(mix_models_cli):
    # TODO complete by providing a Scenario that is reportable (with solution)
    mix_models_cli.assert_exit_0(["report"])


@pytest.mark.parametrize(
    "input, exp",
    (
        ("x Secondary Energy|Solids|Solids x", "x Secondary Energy|Solids x"),
        ("x Emissions|CH4|Fugitive x", "x Emissions|CH4|Energy|Supply|Fugitive x"),
        (
            "x Emissions|CH4|Heat|foo x",
            "x Emissions|CH4|Energy|Supply|Heat|Fugitive|foo x",
        ),
        (
            "land_out CH4|Emissions|Ch4|Land Use|Agriculture|foo x",
            "Emissions|CH4|AFOLU|Agriculture|Livestock|foo x",
        ),
        ("land_out CH4|foo|bar|Awm x", "foo|bar|Manure Management x"),
        ("x Residential|Biomass x", "x Residential|Solids|Biomass x"),
        ("x Residential|Gas x", "x Residential|Gases|Natural Gas x"),
        ("x Import Energy|Lng x", "x Primary Energy|Gas x"),
        ("x Import Energy|Coal x", "x Primary Energy|Coal x"),
        ("x Import Energy|Oil x", "x Primary Energy|Oil x"),
        ("x Import Energy|Liquids|Biomass x", "x Secondary Energy|Liquids|Biomass x"),
        ("x Import Energy|Lh2 x", "x Secondary Energy|Hydrogen x"),
    ),
)
def test_collapse(input, exp):
    """Test :meth:`.report.util.collapse` and use of :data:`.REPLACE_VARS`.

    This test is parametrized with example input and expected output strings for the
    ``variable`` IAMC column. There should be ≥1 example for each pattern in
    :data:`.REPLACE_VARS`.

    When adding test cases, if the pattern does not start with ``^`` or end with ``$``,
    then prefix "x " or suffix " x" respectively to ensure these are handled as
    intended.

    .. todo:: Extend or duplicate to also cover :data:`.REPLACE_DIMS`.
    """
    # Convert values to data frames with 1 row and 1 column
    df_in = pd.DataFrame([[input]], columns=["variable"])
    df_exp = pd.DataFrame([[exp]], columns=["variable"])

    # collapse() transforms the "variable" column in the expected way
    pdt.assert_frame_equal(util.collapse(df_in), df_exp)


def simulated_solution_reporter():
    """Reporter with a simulated solution for snapshot 0.

    This uses :func:`.add_simulated_solution`, so test functions that use it should be
    marked with :py:`@to_simulate.minimum_version`.
    """
    from message_ix import Reporter

    rep = Reporter()

    # Simulated solution can be added to an empty Reporter
    add_simulated_solution(
        rep,
        ScenarioInfo(),
        path=package_data_path(
            "test", "MESSAGEix-GLOBIOM_1.1_R11_no-policy_baseline", "v0"
        ),
    )

    return rep


@to_simulate.minimum_version
def test_add_simulated_solution(test_context, test_data_path):
    # Simulated solution can be added to an empty Reporter
    rep = simulated_solution_reporter()

    # "out" can be calculated using "output" and "ACT" from files in `path`
    result = rep.get("out:*")

    # Has expected dimensions and length
    assert tuple("nl t yv ya m nd c l h hd".split()) == result.dims
    assert 155461 == len(result)

    # Compare one expected value
    value = result.sel(
        nl="R11_AFR",
        t="biomass_rc",
        yv=2020,
        ya=2020,
        m="M1",
        nd="R11_AFR",
        c="rc_therm",
        l="useful",
        h="year",
        hd="year",
    )
    assert np.isclose(79.76478, value.item())


@to_simulate.minimum_version
def test_prepare_reporter(test_context):
    rep = simulated_solution_reporter()
    N = len(rep.graph)

    # prepare_reporter() works on the simulated solution
    prepare_reporter(test_context, reporter=rep)

    # A number of keys were added
    assert 14299 <= len(rep.graph) - N
