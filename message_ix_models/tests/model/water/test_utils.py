import pandas as pd
import pytest

from message_ix_models.model.water.utils import (
    _select_by_stress,
    compute_basin_demand_ratio,
    filter_basins_by_region,
    get_vintage_and_active_years,
    read_config,
)


@pytest.fixture
def mock_scenario_info():
    """Mock ScenarioInfo with yv_ya property and year set."""

    class MockScenarioInfo:
        def __init__(self):
            self.yv_ya = pd.DataFrame(
                {
                    "year_vtg": [2010, 2010, 2010, 2020, 2020, 2030],
                    "year_act": [2010, 2020, 2030, 2020, 2030, 2030],
                }
            )
            self.set = {"year": [2010, 2020, 2030]}

    return MockScenarioInfo()


def test_read_config(test_context):
    # read_config() returns a reference to the current context
    context = read_config()
    assert context is test_context

    # config::'data files' have been read-in correctly
    assert context["water config"]["data files"] == [
        "cooltech_cost_and_shares_ssp_msg14",
        "tech_water_performance_ssp_msg",
    ]


@pytest.mark.parametrize(
    "technical_lifetime,expected_data",
    [
        (
            10,
            {
                "year_vtg": [2010, 2010, 2020, 2020, 2030],
                "year_act": [2010, 2020, 2020, 2030, 2030],
            },
        ),
        (
            20,
            {
                "year_vtg": [2010, 2010, 2010, 2020, 2020, 2030],
                "year_act": [2010, 2020, 2030, 2020, 2030, 2030],
            },
        ),
        (
            None,
            # Defaults to same_year when technical_lifetime not provided
            {
                "year_vtg": [2010, 2020, 2030],
                "year_act": [2010, 2020, 2030],
            },
        ),
    ],
)
def test_get_vintage_and_active_years(
    mock_scenario_info, technical_lifetime, expected_data
):
    """Test get_vintage_and_active_years function with different technical lifetimes."""
    result = get_vintage_and_active_years(mock_scenario_info, technical_lifetime)
    expected = pd.DataFrame(expected_data)
    pd.testing.assert_frame_equal(result, expected)


# --- Tests for stress-based basin selection ---


class TestComputeBasinDemandRatio:
    """Tests for compute_basin_demand_ratio()."""

    def test_r12_shape_and_columns(self):
        result = compute_basin_demand_ratio("R12")
        assert len(result) == 217
        expected_cols = {
            "BCU_name",
            "REGION",
            "supply_mcm",
            "demand_mcm",
            "demand_ratio",
        }
        assert expected_cols == set(result.columns)

    def test_no_nan_in_ratio(self):
        result = compute_basin_demand_ratio("R12")
        assert not result["demand_ratio"].isna().any()

    def test_all_regions_present(self):
        result = compute_basin_demand_ratio("R12")
        assert len(result["REGION"].unique()) == 12

    def test_high_stress_basins_exist(self):
        """At least some basins should have demand/supply > 10%."""
        result = compute_basin_demand_ratio("R12", demand_year=2050)
        high_stress = result[result["demand_ratio"] > 0.10]
        assert len(high_stress) > 0, "No high-stress basins found"


class TestSelectByStress:
    """Tests for _select_by_stress()."""

    @pytest.fixture
    def stress_df(self):
        return compute_basin_demand_ratio("R12")

    @pytest.mark.parametrize("n_per_region", [1, 2, 3, 5])
    def test_all_regions_covered(self, stress_df, n_per_region):
        selected = _select_by_stress(stress_df, n_per_region=n_per_region)
        assert len(selected) > 0
        selected_df = stress_df[stress_df["BCU_name"].isin(selected)]
        assert len(selected_df["REGION"].unique()) == 12

    def test_min_max_included_for_n2(self, stress_df):
        """For n=2, lowest and highest stress basins per region should be selected."""
        selected = _select_by_stress(stress_df, n_per_region=2)
        for _, group in stress_df.groupby("REGION"):
            sorted_g = group.sort_values("demand_ratio")
            assert sorted_g.iloc[0]["BCU_name"] in selected
            assert sorted_g.iloc[-1]["BCU_name"] in selected

    def test_n_per_region_respected(self, stress_df):
        selected = _select_by_stress(stress_df, n_per_region=2)
        selected_df = stress_df[stress_df["BCU_name"].isin(selected)]
        for _, group in selected_df.groupby("REGION"):
            assert len(group) <= 2


class TestFilterBasinsByRegionStress:
    """Test stress mode integration in filter_basins_by_region()."""

    def test_stress_mode_returns_valid_output(self, test_context):
        from message_ix_models.util import package_data_path

        df_basins = pd.read_csv(
            package_data_path("water", "delineation", "basins_by_region_simpl_R12.csv")
        )
        test_context.reduced_basin = True
        test_context.basin_selection = "stress"
        test_context.regions = "R12"
        test_context.ssp = "SSP2"

        filtered = filter_basins_by_region(df_basins, test_context, n_per_region=2)

        assert len(filtered) > 0
        assert len(filtered) < len(df_basins)
        assert len(filtered["REGION"].unique()) == 12
        assert not filtered["BCU_name"].isna().any()
