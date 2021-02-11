"""Tests for message_data.reporting."""
import pandas as pd
import pytest

from message_data import testing
from message_data.reporting import prepare_reporter


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
