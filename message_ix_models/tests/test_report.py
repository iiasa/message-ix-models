"""Tests for message_data.reporting."""
import pandas as pd
import pandas.testing as pdt
import pytest

from message_data import testing
from message_data.reporting import prepare_reporter, util


# Minimal reporting configuration for testing
MIN_CONFIG = {
    "units": {
        "replace": {"???": ""},
    },
}


def test_report_bare_res(request, session_context):
    """Prepare and run the standard MESSAGE-GLOBIOM reporting on a bare RES."""
    ctx = session_context

    scenario = testing.bare_res(request, ctx, solved=True)

    # Prepare the reporter
    reporter, key = prepare_reporter(
        scenario,
        config=session_context.get_config_file("report", "global"),
        key="message:default",
    )

    # Get the default report
    # NB commented because the bare RES currently contains no activity, so the
    #    reporting steps fail
    # reporter.get(key)


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


@pytest.mark.parametrize("regions", ["R11"])
def test_apply_units(request, test_context, regions):
    test_context.regions = regions
    bare_res = testing.bare_res(request, test_context, solved=True)

    qty = "inv_cost"

    # Create a temporary config dict
    config = MIN_CONFIG.copy()

    # Prepare the reporter
    reporter, key = prepare_reporter(bare_res, config=config, key=qty)

    # Add some data to the scenario
    inv_cost = DATA_INV_COST.copy()
    bare_res.remove_solution()
    bare_res.check_out()
    bare_res.add_par("inv_cost", inv_cost)
    bare_res.commit("")
    bare_res.solve()

    # Units are retrieved
    USD_2005 = reporter.unit_registry.Unit("USD_2005")
    assert reporter.get(key).attrs["_unit"] == USD_2005

    # Add data with units that will be discarded
    inv_cost["unit"] = ["USD", "kg"]
    bare_res.remove_solution()
    bare_res.check_out()
    bare_res.add_par("inv_cost", inv_cost)

    # Units are discarded
    assert str(reporter.get(key).attrs["_unit"]) == "dimensionless"

    # Update configuration, re-create the reporter
    config["units"]["apply"] = {"inv_cost": "USD"}
    bare_res.commit("")
    bare_res.solve()
    reporter, key = prepare_reporter(bare_res, config=config, key=qty)

    # Units are applied
    assert str(reporter.get(key).attrs["_unit"]) == USD_2005

    # Update configuration, re-create the reporter
    config.update(INV_COST_CONFIG)
    reporter, key = prepare_reporter(bare_res, config=config, key=qty)

    # Units are converted
    df = reporter.get("Investment Cost:iamc").as_pandas()
    assert set(df["unit"]) == {"EUR_2005"}


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
