import pandas as pd
import pytest
from message_ix import Scenario

from message_ix_models import ScenarioInfo, testing
from message_ix_models.model.water.data.water_for_ppl import (
    apply_act_cap_multiplier,
    cool_tech,
    cooling_shares_SSP_from_yaml,
    non_cooling_tec,
)


@pytest.mark.usefixtures("ssp_user_data")
@pytest.mark.parametrize(
    "water_context",
    [
        {"regions": "R11", "type_reg": "global", "RCP": "no_climate", "REL": "med", "ssp": "SSP2"},
        {"regions": "R11", "type_reg": "global", "RCP": "6p0", "REL": "med", "ssp": "SSP2"},
    ],
    indirect=True,
)
def test_cool_tec(request, water_context, assert_message_params):
    """Test cool_tech with global model configuration.

    Requires a scenario with input, historical_activity, and historical_new_capacity
    parameters for cooling technology derivation.
    """
    # cool_tech requires a more elaborate scenario with parameters populated
    mp = water_context.get_platform()
    s = Scenario(
        mp=mp,
        model=f"{request.node.name}/test water model",
        scenario=f"{request.node.name}/test water scenario",
        version="new",
    )
    s.add_horizon(year=[2020, 2030, 2040])
    s.add_set("technology", ["gad_cc", "coal_ppl"])
    s.add_set("node", ["R11_CPA"])
    s.add_set("year", [2020, 2030, 2040])
    s.add_set("mode", ["M1", "M2"])
    s.add_set("commodity", ["electricity", "gas"])
    s.add_set("level", ["secondary", "final"])
    s.add_set("time", ["year"])

    # Add input parameter
    df_add = pd.DataFrame(
        {
            "node_loc": ["R11_CPA"],
            "technology": ["coal_ppl"],
            "year_vtg": [2020],
            "year_act": [2020],
            "mode": ["M1"],
            "node_origin": ["R11_CPA"],
            "commodity": ["electricity"],
            "level": ["secondary"],
            "time": "year",
            "time_origin": "year",
            "value": [1],
            "unit": "GWa",
        }
    )
    # Add historical activity
    df_ha = pd.DataFrame(
        {
            "node_loc": ["R11_CPA"],
            "technology": ["coal_ppl"],
            "year_act": [2020],
            "mode": ["M1"],
            "time": "year",
            "value": [1],
            "unit": "GWa",
        }
    )
    # Add historical new capacity
    df_hnc = pd.DataFrame(
        {
            "node_loc": ["R11_CPA"],
            "technology": ["coal_ppl"],
            "year_vtg": [2020],
            "value": [1],
            "unit": "GWa",
        }
    )
    s.add_par("input", df_add)
    s.add_par("historical_activity", df_ha)
    s.add_par("historical_new_capacity", df_hnc)
    s.commit(comment="water cool_tech test scenario")

    water_context.set_scenario(s)
    water_context["water build info"] = ScenarioInfo(scenario_obj=s)

    result = cool_tech(context=water_context)

    # Standard MESSAGE parameter validation
    assert_message_params(result, expected_keys=["input"])

    # Validate input structure
    assert all(
        col in result["input"].columns
        for col in [
            "technology",
            "value",
            "unit",
            "level",
            "commodity",
            "mode",
            "time",
            "time_origin",
            "node_origin",
            "node_loc",
            "year_vtg",
            "year_act",
        ]
    )


def test_non_cooling_tec(water_scenario, test_context, assert_message_params):
    """Test non_cooling_tec with basic scenario."""
    result = non_cooling_tec(context=test_context)

    assert_message_params(result, expected_keys=["input"])

    assert all(
        col in result["input"].columns
        for col in [
            "technology",
            "value",
            "unit",
            "level",
            "commodity",
            "mode",
            "time",
            "time_origin",
            "node_origin",
            "node_loc",
            "year_vtg",
            "year_act",
        ]
    )


@pytest.mark.parametrize(
    "param_name, cap_fact_parent, expected_values",
    [
        (
            "historical_activity",
            None,
            [100 * 0.5, 150 * 1.2],
        ),
        (
            "historical_new_capacity",
            pd.DataFrame(
                {
                    "node_loc": ["R1", "R2"],
                    "technology": ["TechA", "TechB"],
                    "cap_fact": [0.9, 0.9],
                }
            ),
            [100 * 0.5 * 0.9 * 1.2, 150 * 1.2 * 0.9 * 1.2],
        ),
    ],
)
def test_apply_act_cap_multiplier(
    param_name: str,
    cap_fact_parent: pd.DataFrame | None,
    expected_values: list[float],
) -> None:
    """Test apply_act_cap_multiplier with different parameter configurations."""
    df = pd.DataFrame(
        {
            "node_loc": ["R1", "R2"],
            "technology": ["TechA", "TechB"],
            "value": [100, 150],
        }
    )

    hold_cost = pd.DataFrame(
        {
            "utype": ["Type1", "Type2"],
            "technology": ["TechA", "TechB"],
            "R1": [0.5, 0.8],
            "R2": [1.0, 1.2],
        }
    )

    result = apply_act_cap_multiplier(df, hold_cost, cap_fact_parent, param_name)

    assert result["value"].tolist() == expected_values, (
        f"Failed for param_name: {param_name}"
    )


@pytest.mark.parametrize("SSP, regions", [("SSP2", "R11"), ("LED", "R12")])
def test_cooling_shares_SSP_from_yaml(request, test_context, SSP, regions):
    """Test cooling_shares_SSP_from_yaml with different SSP/region combinations."""
    test_context.model.regions = regions
    scenario = testing.bare_res(request, test_context)
    test_context["water build info"] = ScenarioInfo(scenario_obj=scenario)
    test_context.ssp = SSP

    result = cooling_shares_SSP_from_yaml(test_context)

    assert isinstance(result, pd.DataFrame), "Result should be a DataFrame"
    assert not result.empty, "Resulting DataFrame should not be empty"
    assert result["year_act"].min() >= 2050
