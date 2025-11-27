"""Tests for water_for_ppl cooling technology functions."""

import pandas as pd
import pytest
from message_ix import Scenario

from message_ix_models import ScenarioInfo

from message_ix_models.model.water.data.water_for_ppl import (
    cool_tech,
    non_cooling_tec,
)
from message_ix_models.tests.model.water.conftest import (
    setup_valid_basins,
    load_cool_tech_fixture,
    create_mock_scenario_info,
    MockScenario,
    FIXTURE_DIR,
)
from message_ix_models.util import package_data_path


@pytest.mark.usefixtures("ssp_user_data")
@pytest.mark.parametrize("RCP", ["no_climate", "7p0"])
def test_cool_tec(request, test_context, RCP):
    mp = test_context.get_platform()
    scenario_info = {
        "mp": mp,
        "model": f"{request.node.name}/test water model",
        "scenario": f"{request.node.name}/test water scenario",
        "version": "new",
    }
    s = Scenario(**scenario_info)
    s.add_horizon(year=[2020, 2030, 2040])
    s.add_set("technology", ["gad_cc", "coal_ppl"])
    s.add_set("node", ["R12_CPA"])
    s.add_set("year", [2020, 2030, 2040])
    s.add_set("mode", ["M1", "M2"])
    s.add_set("commodity", ["electricity", "gas"])
    s.add_set("level", ["secondary", "final"])
    s.add_set("time", ["year"])

    df_add = pd.DataFrame(
        {
            "node_loc": ["R12_CPA"],
            "technology": ["coal_ppl"],
            "year_vtg": [2020],
            "year_act": [2020],
            "mode": ["M1"],
            "node_origin": ["R12_CPA"],
            "commodity": ["electricity"],
            "level": ["secondary"],
            "time": "year",
            "time_origin": "year",
            "value": [1],
            "unit": "GWa",
        }
    )
    df_ha = pd.DataFrame(
        {
            "node_loc": ["R12_CPA"],
            "technology": ["coal_ppl"],
            "year_act": [2020],
            "mode": ["M1"],
            "time": "year",
            "value": [1],
            "unit": "GWa",
        }
    )
    df_hnc = pd.DataFrame(
        {
            "node_loc": ["R12_CPA"],
            "technology": ["coal_ppl"],
            "year_vtg": [2020],
            "value": [1],
            "unit": "GWa",
        }
    )
    s.add_par("input", df_add)
    s.add_par("historical_activity", df_ha)
    s.add_par("historical_new_capacity", df_hnc)

    s.commit(comment="basic water non_cooling_tec test model")

    test_context.set_scenario(s)
    test_context["water build info"] = ScenarioInfo(scenario_obj=s)
    test_context.type_reg = "global"
    test_context.regions = "R12"
    test_context.time = "year"
    test_context.nexus_set = "nexus"
    test_context.update(
        RCP=RCP,
        REL="med",
        ssp="SSP2",
    )

    setup_valid_basins(test_context, regions=test_context.regions)

    result = cool_tech(context=test_context)

    # Assert the results
    assert isinstance(result, dict)
    assert "input" in result
    assert not result["input"]["value"].isna().any(), (
        "Input DataFrame contains NaN values"
    )
    input_time_values = result["input"]["time"].unique()
    assert not any(len(str(val)) == 1 for val in input_time_values), (
        f"Input DataFrame contains time values: {input_time_values}. "
    )

    output_time_values = result["output"]["time"].unique()
    assert not any(len(str(val)) == 1 for val in output_time_values), (
        f"Output DataFrame contains time values: {output_time_values}. "
    )
    input_duplicates = result["input"].duplicated().sum()
    assert input_duplicates == 0, (
        f"Input DataFrame contains {input_duplicates} duplicate rows"
    )
    output_duplicates = result["output"].duplicated().sum()
    assert output_duplicates == 0, (
        f"Input DataFrame contains {output_duplicates} duplicate rows"
    )

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


def test_non_cooling_tec(request, test_context):
    mp = test_context.get_platform()
    scenario_info = {
        "mp": mp,
        "model": f"{request.node.name}/test water model",
        "scenario": f"{request.node.name}/test water scenario",
        "version": "new",
    }
    s = Scenario(**scenario_info)
    s.add_horizon(year=[2020, 2030, 2040])
    s.add_set("technology", ["tech1", "tech2"])
    s.add_set("node", ["loc1", "loc2"])
    s.add_set("year", [2020, 2030, 2040])

    s.commit(comment="basic water non_cooling_tec test model")

    test_context.set_scenario(s)
    test_context.nexus_set = "nexus"

    result = non_cooling_tec(context=test_context)

    assert isinstance(result, dict)
    assert "input" in result
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


def test_saline_allocation_regional_average():
    """Document that regional average shares prevent saline over-allocation.

    Per-parent-tech shares give nuclear plants 50-65% saline (coastal siting).
    Regional average shares (~12.5% saline) give more conservative allocation
    that better constrains model's tendency to expand saline cooling.
    """
    fixture_df = pd.read_parquet(FIXTURE_DIR / "cool_tech_fixture.parquet")
    cost_share = pd.read_csv(
        package_data_path("water", "ppl_cooling_tech", "cooltech_cost_and_shares_ssp_msg_R12.csv")
    )

    hist_act = fixture_df[fixture_df["_param"] == "historical_activity"]
    mix_cols = [c for c in cost_share.columns if c.startswith("mix_")]

    # Per-parent-tech shares
    saline_by_tech = cost_share[cost_share["cooling"] == "ot_saline"].set_index("utype")[mix_cols]
    total_saline_per_tech = 0
    for region in mix_cols:
        region_name = region.replace("mix_", "")
        for tech in hist_act[hist_act["node_loc"] == region_name]["technology"].unique():
            tech_act = hist_act[
                (hist_act["node_loc"] == region_name) & (hist_act["technology"] == tech)
            ]["value"].sum()
            if tech in saline_by_tech.index:
                total_saline_per_tech += tech_act * saline_by_tech.loc[tech, region]

    # Regional average shares
    avg_saline_share = cost_share[cost_share["cooling"] == "ot_saline"][mix_cols].mean()
    total_saline_regional = 0
    for region in mix_cols:
        region_name = region.replace("mix_", "")
        region_act = hist_act[hist_act["node_loc"] == region_name]["value"].sum()
        total_saline_regional += region_act * avg_saline_share[region]

    ratio = total_saline_per_tech / total_saline_regional
    print(f"\nSaline allocation comparison:")
    print(f"  Per-tech shares:     {total_saline_per_tech:.0f} GWa")
    print(f"  Regional avg shares: {total_saline_regional:.0f} GWa")
    print(f"  Ratio: {ratio:.2f}x")

    # Per-tech gives roughly 2x more saline than regional average
    assert ratio > 1.5, f"Expected per-tech to over-allocate saline, got ratio {ratio}"
    assert ratio < 2.5, f"Ratio {ratio} unexpectedly high"

    # Nuclear specifically should have high saline share
    nuc_saline = saline_by_tech.loc["nuc_lc"].mean() if "nuc_lc" in saline_by_tech.index else 0
    assert nuc_saline > 0.5, f"Expected nuclear saline share > 50%, got {nuc_saline:.1%}"


@pytest.mark.usefixtures("ssp_user_data")
@pytest.mark.parametrize("RCP", ["no_climate"])
def test_share_constraints(test_context, RCP):
    """Test that cool_tech correctly applies share constraints.

    Type 1: SSP YAML share_commodity_up (policy constraints on future shares)
    Type 2: Regional average shares for historical_activity allocation
    """
    import yaml

    try:
        fixture_df, meta = load_cool_tech_fixture()
    except FileNotFoundError:
        pytest.skip("Fixture data not available")

    mock_scen = MockScenario(fixture_df, firstmodelyear=meta["firstmodelyear"])
    info = create_mock_scenario_info(meta)

    test_context["water build info"] = info
    test_context.type_reg = "global"
    test_context.regions = "R12"
    test_context.time = "year"
    test_context.nexus_set = "nexus"
    test_context.update(RCP=RCP, REL="med", ssp="SSP2")
    setup_valid_basins(test_context, regions=test_context.regions)

    result = cool_tech(context=test_context, scenario=mock_scen)

    # === Type 1: SSP YAML share_commodity_up ===
    assert "share_commodity_up" in result, "Should generate share_commodity_up"
    share_df = result["share_commodity_up"]
    assert not share_df.empty, "share_commodity_up should not be empty for SSP2"

    yaml_path = package_data_path("water", "ssp.yaml")
    with open(yaml_path) as f:
        yaml_data = yaml.safe_load(f)

    ssp2_shares = yaml_data["scenarios"]["SSP2"]["cooling_tech"]["globalNorth"]["share_commodity_up"]

    for share_name, expected_value in ssp2_shares.items():
        share_rows = share_df[share_df["shares"] == share_name]
        assert not share_rows.empty, f"Missing share constraint: {share_name}"
        actual_values = share_rows["value"].unique()
        assert len(actual_values) == 1, f"Multiple values for {share_name}"
        assert actual_values[0] == expected_value, (
            f"{share_name}: got {actual_values[0]}, expected {expected_value}"
        )

    print(f"\nType 1 (SSP YAML) verified: {ssp2_shares}")

    # === Type 2: Regional average shares for historical allocation ===
    cost_share_df = pd.read_csv(
        package_data_path("water", "ppl_cooling_tech", "cooltech_cost_and_shares_ssp_msg_R12.csv")
    )
    mix_cols = [c for c in cost_share_df.columns if c.startswith("mix_")]
    expected_regional_avg = cost_share_df.groupby("cooling")[mix_cols].mean()

    if "historical_activity" not in result:
        pytest.skip("No historical_activity in result")

    hist_act = result["historical_activity"]

    test_regions = ["R12_AFR", "R12_NAM", "R12_WEU"]
    for region in test_regions:
        region_data = hist_act[hist_act["node_loc"] == region]
        if region_data.empty:
            continue

        region_data = region_data.copy()
        region_data["cooling"] = region_data["technology"].str.split("__").str[1]
        by_cooling = region_data.groupby("cooling")["value"].sum()

        if by_cooling.sum() == 0:
            continue

        actual_ratios = by_cooling / by_cooling.sum()

        mix_col = f"mix_{region.replace('R12_', '')}"
        if mix_col not in expected_regional_avg.columns:
            continue

        expected = expected_regional_avg[mix_col]
        expected_ratios = expected / expected.sum()

        for cooling_type in actual_ratios.index:
            if cooling_type in expected_ratios.index:
                actual = actual_ratios[cooling_type]
                expected_val = expected_ratios[cooling_type]
                assert abs(actual - expected_val) < 0.10, (
                    f"{region}/{cooling_type}: actual ratio {actual:.3f} != expected {expected_val:.3f}"
                )

        print(f"Type 2 ({region}) allocation ratios match regional averages")

    print("Share constraints test passed")
