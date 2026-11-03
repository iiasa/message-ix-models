import pandas as pd
import pytest

from message_ix_models import ScenarioInfo
from message_ix_models.model.water.data.water_supply import (
    add_e_flow,
    add_water_supply,
    eflow_bound_rows,
    map_basin_region_wat,
    renewable_initial_activity,
    zero_share_bounds,
)
from message_ix_models.tests.model.water.conftest import water_params


@pytest.mark.parametrize(
    "water_context",
    [
        water_params("R11", RCP="2p6", REL="med"),
        water_params("R12", RCP="2p6", REL="med"),
        water_params("ZMB", RCP="2p6", REL="med"),
        water_params("R12", reduced_basin=True, RCP="2p6", REL="med"),
    ],
    indirect=True,
)
def test_map_basin_region_wat(water_context):
    """Test map_basin_region_wat with global and country model configurations."""
    # ScenarioInfo needed for year sets
    sets = {"year": [2020, 2030, 2040]}
    water_context["water build info"] = ScenarioInfo(y0=2020, set=sets)

    result = map_basin_region_wat(water_context)

    assert isinstance(result, pd.DataFrame)
    assert all(
        col in result.columns
        for col in ["region", "mode", "date", "MSGREG", "share", "year", "time"]
    )


@pytest.mark.parametrize(
    "water_context",
    [
        water_params("R11", RCP="2p6", REL="med"),
        water_params("R12", RCP="2p6", REL="med"),
        water_params("ZMB", RCP="2p6", REL="med"),
        water_params("R12", reduced_basin=True, RCP="2p6", REL="med"),
    ],
    indirect=True,
)
def test_add_water_supply(water_context, water_scenario, assert_message_params):
    """Test add_water_supply with global and country model configurations."""
    result = add_water_supply(water_context)

    assert_message_params(
        result,
        expected_keys=["input", "output", "var_cost", "technical_lifetime", "inv_cost"],
    )

    for df in result.values():
        assert isinstance(df, pd.DataFrame)


@pytest.mark.parametrize(
    "water_context",
    [
        water_params("R11", RCP="2p6", REL="med", SDG=True),
        water_params("R12", RCP="2p6", REL="med", SDG=True),
        water_params("ZMB", RCP="2p6", REL="med", SDG=True),
        water_params("R12", reduced_basin=True, RCP="2p6", REL="med", SDG=True),
    ],
    indirect=True,
)
def test_add_e_flow(water_context):
    """Test add_e_flow with global and country model configurations."""
    # ScenarioInfo needed for year sets
    sets = {"year": [2020, 2030, 2040]}
    water_context["water build info"] = ScenarioInfo(y0=2020, set=sets)

    result = add_e_flow(water_context)

    assert isinstance(result, dict)
    assert "bound_activity_lo" in result
    assert isinstance(result["bound_activity_lo"], pd.DataFrame)


def test_zero_share_bounds_only_zero_modes() -> None:
    share = pd.DataFrame(
        {
            "shares": "share_basin",
            "node_share": ["R12_NAM", "R12_NAM", "R12_NAM"],
            "technology": ["basin_to_reg_plus"] * 3,
            "mode": ["M132|NAM", "M90|NAM", "M132|NAM"],
            "year_act": [2035, 2035, 2040],
            "time": "year",
            "value": [0.0, 0.175, 0.0],
            "unit": "%",
        }
    )
    out = zero_share_bounds(share)
    assert len(out) == 2
    assert set(out["mode"]) == {"M132|NAM"}
    assert (out["value"] == 0).all()
    assert set(out["year_act"]) == {2035, 2040}


def test_renewable_initial_activity_share_of_availability() -> None:
    df_sw = pd.DataFrame(
        {"Region": ["1|A", "2|A"], "value": [10.0, -1.0], "year": 2035, "time": "year"}
    )
    df_gw = pd.DataFrame(
        {"Region": ["1|A"], "value": [2.0], "year": 2035, "time": "year"}
    )
    out = renewable_initial_activity(df_sw, df_gw, 0.02)
    sw = out[out["technology"] == "extract_surfacewater"].set_index("node_loc")["value"]
    gw = out[out["technology"] == "extract_groundwater"].set_index("node_loc")["value"]
    assert sw["B1|A"] == pytest.approx(10.0 * 1000 * 0.02)  # km3 -> MCM, 2 %
    assert sw["B2|A"] == 0.0  # negative availability -> no headroom
    assert gw["B1|A"] == pytest.approx(40.0)


def test_eflow_bound_rows() -> None:
    df_env = pd.DataFrame(
        {
            "Region": ["X", "X", "X", "Y"],
            "year": [2020, 2030, 2040, 2030],
            "time": "year",
            "value": [1.0, 2.0, -0.5, 10.0],  # km3
        }
    )
    dmd_df = pd.DataFrame(
        {
            "node": ["BX", "BX", "BX", "BY"],
            "year": [2020, 2030, 2040, 2030],
            "time": "year",
            "value": [5000.0, 2000.0, 1000.0, 20000.0],  # MCM
        }
    )

    df = eflow_bound_rows(df_env, dmd_df)

    assert set(df["technology"]) == {"return_flow"}
    assert (df["unit"] == "MCM/year").all()
    assert 2020 not in set(df["year_act"])  # bounds start in 2025
    rows = df.set_index(["node_loc", "year_act"])["value"]
    assert rows[("BX", 2030)] == pytest.approx(
        0.7 * 2000.0
    )  # 2000 MCM capped at 70 % of 2000
    assert rows[("BX", 2040)] == 0.0  # negative e-flow -> 0
    assert rows[("BY", 2030)] == pytest.approx(10000.0)  # below the 70 % cap
