"""Tests for message_data.reporting."""
from importlib.metadata import version

import pandas as pd
import pandas.testing as pdt
import pytest

from message_ix_models import testing
from message_ix_models.report import prepare_reporter, report, util

# Minimal reporting configuration for testing
MIN_CONFIG = {
    "units": {
        "replace": {"???": ""},
    },
}

MARK = (
    pytest.mark.xfail(
        condition=version("message_ix") < "3.6",
        raises=NotImplementedError,
        reason="Not supported with message_ix < 3.6",
    ),
)


@MARK[0]
def test_report_bare_res(request, test_context):
    """Prepare and run the standard MESSAGE-GLOBIOM reporting on a bare RES."""
    scenario = testing.bare_res(request, test_context, solved=True)

    # Prepare the reporter
    test_context.report.update(config="global.yaml", key="message::default")
    reporter, key = prepare_reporter(test_context, scenario)

    # Get the default report
    # NB commented because the bare RES currently contains no activity, so the
    #    reporting steps fail
    # reporter.get(key)


@pytest.mark.xfail(raises=ModuleNotFoundError, reason="Requires message_data")
def test_report_legacy(caplog, request, tmp_path, test_context):
    """Legacy reporting can be invoked through :func:`.report()`."""
    # Create a target scenario
    scenario = testing.bare_res(request, test_context, solved=False)
    test_context.set_scenario(scenario)
    # Set dry_run = True to not actually perform any calculations or modifications
    test_context.dry_run = True
    # Ensure the legacy reporting is used, with default settings
    test_context.report = {"legacy": dict()}

    # Call succeeds
    report(test_context)

    # Dry-run message is logged
    assert "DRY RUN" in caplog.messages
    caplog.clear()

    # Other deprecated usage

    # As called in .model.cli.new_baseline() and .model.create.solve(), with path as a
    # positional argument
    legacy_arg = dict(
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


@MARK[0]
@pytest.mark.parametrize("regions", ["R11"])
def test_apply_units(request, test_context, regions):
    test_context.regions = regions
    bare_res = testing.bare_res(request, test_context, solved=True)

    qty = "inv_cost"

    # Create a temporary config dict
    config = MIN_CONFIG.copy()

    # Prepare the reporter
    test_context.report.update(config=config, key=qty)
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
    test_context.report["config"]["units"]["apply"] = {"inv_cost": "USD"}
    reporter, key = prepare_reporter(test_context, bare_res)

    # Units are applied
    assert USD_2005 == reporter.get(key).units

    # Update configuration, re-create the reporter
    test_context.report["config"].update(INV_COST_CONFIG)
    reporter, key = prepare_reporter(test_context, bare_res)

    # Units are converted
    df = reporter.get("Investment Cost::iamc").as_pandas()
    assert ["EUR_2005"] == df["unit"].unique()


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
    """Test :meth:`.reporting.util.collapse` and use of :data:`.REPLACE_VARS`.

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
