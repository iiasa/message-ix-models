from typing import TYPE_CHECKING
from unittest.mock import Mock

import pandas as pd
import pytest

from message_ix_models import ScenarioInfo
from message_ix_models.model.material.util import update_macro_calib_file
from message_ix_models.testing import bare_res
from message_ix_models.util import broadcast, package_data_path

if TYPE_CHECKING:
    from message_ix import Scenario
    from pytest import FixtureRequest

    from message_ix_models import Context


@pytest.fixture
def scenario(request: "FixtureRequest", test_context: "Context") -> "Scenario":
    """Same fixture as in :mod:`.test_tools`."""
    test_context.model.regions = "R12"
    scen = bare_res(request, test_context, solved=False)

    # add R12_GLB region required for steel trade model
    with scen.transact():
        scen.add_set("node", "R12_GLB")
    return scen


def mock_scenario_data(scenario: "Scenario") -> None:
    comms = ["rc_spec", "rc_therm", "i_spec", "i_therm", "transport"]
    nodes = ScenarioInfo(scenario).N
    # Mock DataFrames
    df_cost = pd.DataFrame(
        {
            "year": [2020, 2025, 2030],
            "node": None,
            "lvl": [100, 200, 400],
        }
    ).pipe(broadcast, node=nodes)
    df_price = (
        pd.DataFrame(
            {
                "year": [i for i in range(2020, 2055, 5)],
                "node": None,
                "commodity": None,
                "lvl": [10, 20, 30, 35, 35, 35, 35],
            }
        )
        .pipe(broadcast, node=nodes)
        .pipe(
            broadcast,
            commodity=comms,
        )
    )
    df_demand = (
        pd.DataFrame(
            {
                "year": [2020],
                "node": None,
                "commodity": None,
                "value": [10],
            }
        )
        .pipe(broadcast, node=nodes)
        .pipe(
            broadcast,
            commodity=comms,
        )
    )
    df_gdp = pd.DataFrame(
        {
            "year_act": [i for i in range(2015, 2060, 5)]
            + [i for i in range(2060, 2115, 10)],
            "node_loc": None,
            "value": 10,
        }
    ).pipe(broadcast, node_loc=nodes)
    # Scenario mock
    scenario.var = Mock(  # type: ignore[method-assign]
        side_effect=lambda name, **kwargs: {
            "COST_NODAL_NET": df_cost,
            "PRICE_COMMODITY": df_price,
        }[name]
    )

    # Save the original method
    original_par = scenario.par

    def par_side_effect(name, *args, **kwargs):
        if name == "demand":
            return df_demand
        elif name == "gdp":
            return df_gdp
        return original_par(name, *args, **kwargs)

    scenario.par = Mock(side_effect=par_side_effect)  # type: ignore[method-assign]


def test_update_macro_calib_file(scenario) -> None:
    mock_scenario_data(scenario)
    fname = "macro_calibration_input_SSP2.xlsx"
    update_macro_calib_file(scenario, fname, True)
    dem_vals = pd.read_excel(
        package_data_path("material", "macro", fname), sheet_name="demand_ref"
    )
    assert (dem_vals.value == 10).all()
    # # TODO Extend assertions
