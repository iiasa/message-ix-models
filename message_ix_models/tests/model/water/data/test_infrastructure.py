import pytest
from message_ix import Scenario

from message_ix_models import ScenarioInfo
from message_ix_models.model.structure import get_codes
from message_ix_models.model.water.data.infrastructure import (
    add_desalination,
    add_infrastructure_techs,
)


@pytest.fixture(scope="function")
def infrastructure_test_data(test_context, request):
    """Common fixture for water infrastructure tests."""
    # Extract SDG parameter from the test request
    sdg_param = (
        getattr(request, "param", "baseline")
        if hasattr(request, "param")
        else "baseline"
    )

    # Setup test context
    test_context.SDG = sdg_param
    test_context.time = "year"
    test_context.type_reg = "country"
    test_context.regions = "R12"
    nodes = get_codes(f"node/{test_context.regions}")
    nodes = list(map(str, nodes[nodes.index("World")].child))
    test_context.map_ISO_c = {test_context.regions: nodes[0]}

    # Create scenario
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
    s.add_set("year", [2020, 2030, 2040])
    s.commit(comment="basic water add_infrastructure_techs test model")

    test_context.set_scenario(s)
    test_context["water build info"] = ScenarioInfo(s)

    # Call the function and return results
    result = add_infrastructure_techs(context=test_context)

    return {"result": result, "sdg": sdg_param, "context": test_context}


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================


def validate_data_quality(result, sdg):
    """Validate basic data structure, integrity, and configuration-specific behavior."""
    # Basic structure checks
    assert isinstance(result, dict), "Result must be a dictionary"
    assert "input" in result, "Result must contain 'input' key"
    assert "output" in result, "Result must contain 'output' key"

    # Required columns for input DataFrame
    input_required_cols = [
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
    assert all(col in result["input"].columns for col in input_required_cols), (
        f"Input DataFrame missing required columns: {set(input_required_cols) - set(result['input'].columns)}"
    )

    # Required columns for output DataFrame
    output_required_cols = [
        "technology",
        "value",
        "unit",
        "level",
        "commodity",
        "mode",
        "time",
        "time_dest",
        "node_loc",
        "node_dest",
        "year_vtg",
        "year_act",
    ]
    assert all(col in result["output"].columns for col in output_required_cols), (
        f"Output DataFrame missing required columns: {set(output_required_cols) - set(result['output'].columns)}"
    )

    # Data integrity checks
    assert not result["input"]["value"].isna().any(), (
        "Input DataFrame contains NaN values"
    )
    assert not result["output"]["value"].isna().any(), (
        "Output DataFrame contains NaN values"
    )

    # Time format validation (no single-character time values)
    input_time_values = result["input"]["time"].unique()
    invalid_input_times = [val for val in input_time_values if len(str(val)) == 1]
    assert not invalid_input_times, (
        f"Input DataFrame contains invalid time values: {invalid_input_times}"
    )

    output_time_values = result["output"]["time"].unique()
    invalid_output_times = [val for val in output_time_values if len(str(val)) == 1]
    assert not invalid_output_times, (
        f"Output DataFrame contains invalid time values: {invalid_output_times}"
    )

    # Duplicate detection
    input_duplicates = result["input"].duplicated().sum()
    assert input_duplicates == 0, (
        f"Input DataFrame contains {input_duplicates} duplicate rows"
    )

    output_duplicates = result["output"].duplicated().sum()
    assert output_duplicates == 0, (
        f"Output DataFrame contains {output_duplicates} duplicate rows"
    )

    # Configuration-specific validation (baseline mode should have both M1 and Mf modes)
    if sdg == "baseline":
        output_df = result["output"]
        tech_mode_counts = output_df.groupby("technology")["mode"].nunique()
        techs_with_both_modes = tech_mode_counts[tech_mode_counts == 2]

        dist_techs = {"rural_t_d", "urban_t_d"}
        dist_techs_in_output = set(output_df["technology"].unique()).intersection(
            dist_techs
        )

        if dist_techs_in_output:
            dist_with_both_modes = set(techs_with_both_modes.index).intersection(
                dist_techs_in_output
            )
            assert dist_with_both_modes, (
                f"Baseline mode data overwrite detected: No distribution technologies have both M1 and Mf modes. "
                f"Distribution techs: {dist_techs_in_output}, techs with both modes: {set(techs_with_both_modes.index)}"
            )


def check_completeness(result, sdg):
    """Check that all expected technologies and data pairings are complete."""
    expected_dist_techs = {
        "urban_t_d",
        "urban_unconnected",
        "industry_unconnected",
        "rural_t_d",
        "rural_unconnected",
    }

    # Technology completeness
    input_techs = set(result["input"]["technology"].unique())
    output_techs = set(result["output"]["technology"].unique())

    missing_dist_input = expected_dist_techs - input_techs
    assert not missing_dist_input, (
        f"Distribution technologies missing from input: {missing_dist_input}"
    )

    critical_dist_techs = {"urban_t_d", "rural_t_d"}
    missing_critical_output = critical_dist_techs - output_techs
    assert not missing_critical_output, (
        f"Critical distribution technologies missing from output: {missing_critical_output}"
    )

    # Electric/non-electric pairing
    elec_techs = set(
        result["input"][result["input"]["commodity"] == "electr"]["technology"].unique()
    )
    non_elec_techs = set(
        result["input"][result["input"]["commodity"] != "electr"]["technology"].unique()
    )
    missing_non_elec = elec_techs - non_elec_techs
    assert not missing_non_elec, (
        f"Technologies with electricity input missing non-electric input: {missing_non_elec}"
    )

    # Input/output mode pairing
    output_tech_modes = {
        (row["technology"], row["mode"]) for _, row in result["output"].iterrows()
    }
    input_tech_modes = {
        (row["technology"], row["mode"]) for _, row in result["input"].iterrows()
    }
    missing_inputs = output_tech_modes - input_tech_modes

    if missing_inputs:
        missing_by_mode = {}
        for tech, mode in missing_inputs:
            missing_by_mode.setdefault(mode, []).append(tech)

        error_details = "\n".join(
            [f"  {mode} mode: {techs}" for mode, techs in missing_by_mode.items()]
        )
        error_msg = f"Technologies with output modes missing corresponding input modes:\n{error_details}"

        if "Mf" in missing_by_mode:
            error_msg += f"\nCRITICAL: Missing Mf mode inputs detected in {sdg} mode: {missing_by_mode['Mf']}"

        assert False, error_msg

    # Even processing validation (early return bug detection)
    if sdg != "baseline" and len(expected_dist_techs.intersection(input_techs)) > 1:
        dist_tech_counts = result["input"]["technology"].value_counts()
        dist_counts = [
            dist_tech_counts.get(tech, 0)
            for tech in expected_dist_techs
            if tech in input_techs
        ]

        if len(dist_counts) > 1:
            min_count, max_count = min(dist_counts), max(dist_counts)
            if min_count > 0 and max_count > min_count * 2:
                tech_count_details = {
                    tech: dist_tech_counts.get(tech, 0) for tech in expected_dist_techs
                }
                assert False, (
                    f"Uneven distribution tech processing (early return bug): {tech_count_details}"
                )


def verify_parameter_consistency(result, sdg):
    """Verify parameter values are technology-specific and mode consistency is maintained."""
    input_techs = set(result["input"]["technology"].unique())
    output_techs = set(result["output"]["technology"].unique())
    common_techs = input_techs.intersection(output_techs)

    # Mode consistency between input and output
    expected_modes = {"M1", "Mf"} if sdg == "baseline" else {"Mf"}

    for tech in common_techs:
        input_modes = set(
            result["input"][result["input"]["technology"] == tech]["mode"].unique()
        )
        output_modes = set(
            result["output"][result["output"]["technology"] == tech]["mode"].unique()
        )

        assert input_modes == expected_modes, (
            f"Technology {tech} has incorrect input modes for {sdg} configuration. "
            f"Expected: {expected_modes}, Found: {input_modes}"
        )
        assert output_modes == expected_modes, (
            f"Technology {tech} has incorrect output modes for {sdg} configuration. "
            f"Expected: {expected_modes}, Found: {output_modes}"
        )

    # Variable cost consistency (technology-specific values)
    if "var_cost" in result:
        var_cost_df = result["var_cost"]
        tech_cost_patterns = {}

        for tech in var_cost_df["technology"].unique():
            tech_data = var_cost_df[var_cost_df["technology"] == tech].sort_values(
                ["node_loc", "year_act"]
            )
            cost_pattern = tuple(tech_data["value"].values)
            tech_cost_patterns.setdefault(cost_pattern, []).append(tech)

        shared_patterns = {
            pattern: techs
            for pattern, techs in tech_cost_patterns.items()
            if len(techs) > 1
        }

        if shared_patterns:
            max_shared = max(len(techs) for techs in shared_patterns.values())
            total_techs = len(var_cost_df["technology"].unique())

            assert max_shared / total_techs < 0.7, (
                f"Variable reference bug: {max_shared}/{total_techs} technologies have identical cost patterns"
            )

    # Technical lifetime consistency
    if "technical_lifetime" in result:
        lifetime_df = result["technical_lifetime"]
        dist_techs = {"rural_t_d", "urban_t_d"}
        available_dist_techs = dist_techs.intersection(
            set(lifetime_df["technology"].unique())
        )
        non_dist_techs = set(lifetime_df["technology"].unique()) - dist_techs

        if len(available_dist_techs) >= 2 and non_dist_techs:
            dist_lifetimes = [
                lifetime_df[lifetime_df["technology"] == tech]["value"].iloc[0]
                for tech in available_dist_techs
            ]
            non_dist_lifetime = lifetime_df[
                lifetime_df["technology"] == list(non_dist_techs)[0]
            ]["value"].iloc[0]

            identical_to_non_dist = sum(
                1 for lt in dist_lifetimes if lt == non_dist_lifetime
            )

            assert identical_to_non_dist < len(dist_lifetimes), (
                f"Stale variable bug: All distribution technologies have lifetime {non_dist_lifetime} "
                f"identical to non-distribution technology"
            )


# =============================================================================
# CONSOLIDATED TESTS
# =============================================================================


@pytest.mark.parametrize(
    "infrastructure_test_data", ["baseline", "not_baseline"], indirect=True
)
def test_infrastructure_data_quality(infrastructure_test_data):
    """Comprehensive test for data structure, integrity, and configuration-specific behavior."""
    result = infrastructure_test_data["result"]
    sdg = infrastructure_test_data["sdg"]

    validate_data_quality(result, sdg)


@pytest.mark.parametrize(
    "infrastructure_test_data", ["baseline", "not_baseline"], indirect=True
)
def test_infrastructure_completeness(infrastructure_test_data):
    """Comprehensive test for technology completeness and data pairing."""
    result = infrastructure_test_data["result"]
    sdg = infrastructure_test_data["sdg"]

    check_completeness(result, sdg)


@pytest.mark.parametrize(
    "infrastructure_test_data", ["baseline", "not_baseline"], indirect=True
)
def test_infrastructure_parameter_consistency(infrastructure_test_data):
    """Comprehensive test for parameter consistency and technology-specific values."""
    result = infrastructure_test_data["result"]
    sdg = infrastructure_test_data["sdg"]

    verify_parameter_consistency(result, sdg)


# =============================================================================
# LEGACY DESALINATION TEST (Preserved)
# =============================================================================


def test_add_desalination(test_context, request):
    # FIXME You probably want this to be part of a common setup rather than writing
    # something like this for every test
    test_context.time = "year"
    test_context.type_reg = "global"
    test_context.regions = "R12"
    test_context.map_ISO_c = {}

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
    s.add_set("year", [2020, 2030, 2040])

    s.commit(comment="basic water desalination test model")

    test_context.set_scenario(s)

    # FIXME same as above
    test_context["water build info"] = ScenarioInfo(s)

    # Call the function to be tested
    result = add_desalination(context=test_context)

    # Assert the results
    assert isinstance(result, dict)
    assert "output" in result
    assert "capacity_factor" in result
    assert "technical_lifetime" in result
    assert "inv_cost" in result
    assert "fix_cost" in result
    assert "input" in result
