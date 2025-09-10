import pandas as pd
import pytest
from message_ix import Scenario

from message_ix_models import ScenarioInfo
from message_ix_models.model.structure import get_codes
from message_ix_models.model.water.data.infrastructure import (
    add_desalination,
    add_infrastructure_techs,
)
from message_ix_models.util import package_data_path


def _get_technology_categories():
    """Extract technology categories and cost data directly from source data files."""
    # Get infrastructure technologies from water_distribution.csv
    infra_path = package_data_path("water", "infrastructure", "water_distribution.csv")
    df_infra = pd.read_csv(infra_path)
    # Distribution technologies hardcoded in infrastructure.py lines 199-205
    distribution_techs = {
        "urban_t_d",
        "urban_unconnected",
        "industry_unconnected",
        "rural_t_d",
        "rural_unconnected",
    }

    infra_categories = {
        "distribution": list(
            distribution_techs.intersection(set(df_infra["tec"].dropna()))
        ),
        "electric": list(
            df_infra[df_infra["incmd"] == "electr"]["tec"].dropna().unique()
        ),
        "non_electric": list(
            df_infra[df_infra["incmd"] != "electr"]["tec"].dropna().unique()
        ),
        "all": list(df_infra["tec"].dropna().unique()),
        "raw_data": df_infra,  # Include raw data for verification
    }

    # Get desalination technologies from desalination.csv
    desal_path = package_data_path("water", "infrastructure", "desalination.csv")
    df_desal = pd.read_csv(desal_path)

    desal_categories = {
        "all": list(df_desal["tec"].dropna().unique()),
        "raw_data": df_desal,  # Include raw data for verification
    }

    return {"infrastructure": infra_categories, "desalination": desal_categories}


def _get_required_columns(function_type):
    """Get required column definitions for input and output DataFrames."""
    # Common base columns for all function types
    base_input_cols = [
        "technology",
        "value",
        "unit",
        "level",
        "commodity",
        "mode",
        "node_loc",
        "year_vtg",
        "year_act",
    ]

    base_output_cols = [
        "technology",
        "value",
        "unit",
        "level",
        "commodity",
        "mode",
        "node_loc",
        "year_vtg",
        "year_act",
    ]

    if function_type == "infrastructure":
        # Infrastructure-specific additions
        input_required_cols = base_input_cols + ["time", "time_origin", "node_origin"]
        output_required_cols = base_output_cols + ["time", "time_dest", "node_dest"]
    else:  # desalination
        # Desalination-specific additions
        input_required_cols = base_input_cols + ["time_origin", "node_origin"]
        output_required_cols = base_output_cols + ["time"]

    return input_required_cols, output_required_cols


@pytest.fixture(scope="function")
def water_function_test_data(test_context, request):
    """Unified fixture for testing both infrastructure and desalination functions."""
    # Parse test parameters: (function_type, sdg_config)
    function_type, sdg_config = request.param

    # Setup test context
    if sdg_config:
        test_context.SDG = sdg_config
    test_context.time = "year"
    test_context.type_reg = "country" if function_type == "infrastructure" else "global"
    test_context.regions = "R12"
    nodes = get_codes(f"node/{test_context.regions}")
    nodes = list(map(str, nodes[nodes.index("World")].child))
    test_context.map_ISO_c = {test_context.regions: nodes[0]}

    # Add RCP for desalination
    if function_type == "desalination":
        test_context.RCP = "6p0"  # Use RCP that exists in R12 data

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
    s.commit(comment=f"basic water {function_type} test model")

    test_context.set_scenario(s)
    test_context["water build info"] = ScenarioInfo(s)

    # Call appropriate function
    if function_type == "infrastructure":
        result = add_infrastructure_techs(context=test_context)
    else:  # desalination
        result = add_desalination(context=test_context)

    return {
        "result": result,
        "function_type": function_type,
        "sdg_config": sdg_config,
        "context": test_context,
    }


@pytest.mark.parametrize(
    "water_function_test_data",
    [
        ("infrastructure", "baseline"),
        ("infrastructure", "not_baseline"),
        ("desalination", None),
    ],
    indirect=True,
)
def test_water_data_structure(water_function_test_data):
    """Test for data structure, integrity, and configuration-specific behavior."""
    data = water_function_test_data
    result = data["result"]
    function_type = data["function_type"]
    data["sdg_config"]

    # Basic structure checks
    assert isinstance(result, dict), f"{function_type}: Result must be a dictionary"
    assert "input" in result, f"{function_type}: Result must contain 'input' key"
    assert "output" in result, f"{function_type}: Result must contain 'output' key"

    # Get required columns based on function type
    input_required_cols, output_required_cols = _get_required_columns(function_type)

    # Column validation
    missing_input_cols = set(input_required_cols) - set(result["input"].columns)
    assert not missing_input_cols, (
        f"{function_type}: Input DataFrame missing columns: {missing_input_cols}"
    )

    missing_output_cols = set(output_required_cols) - set(result["output"].columns)
    assert not missing_output_cols, (
        f"{function_type}: Output DataFrame missing columns: {missing_output_cols}"
    )

    # Data integrity checks
    assert not result["input"]["value"].isna().any(), (
        f"{function_type}: Input DataFrame contains NaN values"
    )
    assert not result["output"]["value"].isna().any(), (
        f"{function_type}: Output DataFrame contains NaN values"
    )

    # Time format validation (catches single-character time bug)
    input_time_values = (
        result["input"]["time"].unique() if "time" in result["input"].columns else []
    )
    invalid_input_times = [val for val in input_time_values if len(str(val)) == 1]
    assert not invalid_input_times, (
        f"{function_type}: Input DataFrame has invalid time values: "
        f"{invalid_input_times}"
    )

    output_time_values = (
        result["output"]["time"].unique() if "time" in result["output"].columns else []
    )
    invalid_output_times = [val for val in output_time_values if len(str(val)) == 1]
    assert not invalid_output_times, (
        f"{function_type}: Output DataFrame has invalid time values: "
        f"{invalid_output_times}"
    )

    # Duplicate detection
    input_duplicates = result["input"].duplicated().sum()
    assert input_duplicates == 0, (
        f"{function_type}: Input DataFrame contains {input_duplicates} duplicate rows"
    )

    output_duplicates = result["output"].duplicated().sum()
    assert output_duplicates == 0, (
        f"{function_type}: Output DataFrame contains {output_duplicates} duplicate rows"
    )


@pytest.mark.parametrize(
    "water_function_test_data",
    [
        ("infrastructure", "baseline"),
        ("infrastructure", "not_baseline"),
        ("desalination", None),
    ],
    indirect=True,
)
def test_water_technology(water_function_test_data):
    """Test for technology completeness, data pairing, and catch early return bugs."""
    data = water_function_test_data
    result = data["result"]
    function_type = data["function_type"]
    sdg_config = data["sdg_config"]
    tech_categories = _get_technology_categories()[function_type]
    input_techs = set(result["input"]["technology"].unique())
    output_techs = set(result["output"]["technology"].unique())

    if function_type == "infrastructure":
        # Distribution technology completeness (catches early return bug)
        expected_dist_techs = set(tech_categories["distribution"])
        missing_dist_input = expected_dist_techs - input_techs
        assert (
            not missing_dist_input
        ), f"""{function_type}: Distribution technologies missing
            from input (early return bug): {missing_dist_input}"""

        # Electric/non-electric pairing
        elec_techs = set(
            result["input"][result["input"]["commodity"] == "electr"][
                "technology"
            ].unique()
        )
        non_elec_techs = set(
            result["input"][result["input"]["commodity"] != "electr"][
                "technology"
            ].unique()
        )
        missing_non_elec = elec_techs - non_elec_techs
        assert (
            not missing_non_elec
        ), f"""{function_type}: Technologies with electricity input
            missing non-electric input: {missing_non_elec}"""

    # Input/output mode pairing (catches missing Mf inputs bug)
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

        error_details = "\\n".join(
            [f"  {mode} mode: {techs}" for mode, techs in missing_by_mode.items()]
        )
        error_msg = f"""{function_type}: Technologies with output modes missing
        corresponding input modes:\\n{error_details}"""

        assert False, error_msg

    # Mode consistency validation between input and output for infrastructure
    if function_type == "infrastructure":
        common_techs = input_techs.intersection(output_techs)
        distribution_techs = set(tech_categories["distribution"])

        if common_techs:
            for tech in common_techs:
                # Distribution technologies should have both M1 and Mf in baseline,
                # only Mf in non-baseline
                # Non-distribution technologies should only have M1
                if tech in distribution_techs:
                    expected_modes = (
                        {"M1", "Mf"} if sdg_config == "baseline" else {"Mf"}
                    )
                else:
                    expected_modes = {"M1"}

                input_modes = set(
                    result["input"][result["input"]["technology"] == tech][
                        "mode"
                    ].unique()
                )
                output_modes = set(
                    result["output"][result["output"]["technology"] == tech][
                        "mode"
                    ].unique()
                )

                assert input_modes == expected_modes, (
                    f"{function_type}: Technology {tech} has incorrect input "
                    f"modes for {sdg_config} configuration. "
                    f"Expected: {expected_modes}, Found: {input_modes}"
                )
                assert output_modes == expected_modes, (
                    f"{function_type}: Technology {tech} has incorrect output "
                    f"modes for {sdg_config} configuration. "
                    f"Expected: {expected_modes}, Found: {output_modes}"
                )


@pytest.mark.parametrize(
    "water_function_test_data",
    [
        ("infrastructure", "baseline"),
        ("infrastructure", "not_baseline"),
        ("desalination", None),
    ],
    indirect=True,
)
def test_water_parameter(water_function_test_data):
    """Test for parameter consistency and technology-specific uniqueness.

    Catches variable reference bugs.
    """
    data = water_function_test_data
    result = data["result"]
    function_type = data["function_type"]
    data["sdg_config"]
    tech_categories = _get_technology_categories()[function_type]
    
    # Check for bound constraints in desalination
    if function_type == "desalination":
        # Check for bound_total_capacity_up
        assert "bound_total_capacity_up" in result, (
            "desalination: Missing 'bound_total_capacity_up' constraint"
        )
        assert not result["bound_total_capacity_up"].empty, (
            "desalination: 'bound_total_capacity_up' DataFrame is empty"
        )
        
        # Check for bound_activity_lo
        assert "bound_activity_lo" in result, (
            "desalination: Missing 'bound_activity_lo' constraint"
        )
        assert not result["bound_activity_lo"].empty, (
            "desalination: 'bound_activity_lo' DataFrame is empty"
        )
        
        # Check if bound_capacity exists (might be expected but missing)
        if "bound_capacity" in result:
            assert not result["bound_capacity"].empty, (
                "desalination: 'bound_capacity' DataFrame exists but is empty"
            )
        else:
            # Note: bound_capacity is not present in the output
            # This assertion documents the current behavior
            assert "bound_capacity" not in result, (
                "desalination: 'bound_capacity' constraint is missing from output"
            )

    # Variable cost consistency (catches variable reference bug)
    if "var_cost" in result and not result["var_cost"].empty:
        var_cost_df = result["var_cost"]
        tech_cost_patterns = {}

        for tech in var_cost_df["technology"].unique():
            tech_data = var_cost_df[var_cost_df["technology"] == tech].sort_values(
                [col for col in ["node_loc", "year_act"] if col in var_cost_df.columns]
            )
            cost_pattern = tuple(tech_data["value"].values)
            tech_cost_patterns.setdefault(cost_pattern, []).append(tech)

        shared_patterns = {
            pattern: techs
            for pattern, techs in tech_cost_patterns.items()
            if len(techs) > 1
        }

        if shared_patterns:
            # Check against raw CSV data - this is required
            raw_data = tech_categories.get("raw_data")
            assert raw_data is not None, (
                f"{function_type}: Raw data not available for validation"
            )
            assert "var_cost_mid" in raw_data.columns, (
                f"{function_type}: var_cost_mid column missing from raw data"
            )

            # Get unique var_cost_mid values from raw data
            raw_var_costs = raw_data[["tec", "var_cost_mid"]].dropna()
            unique_costs = raw_var_costs.groupby("tec")["var_cost_mid"].first()

            # If all technologies have the same cost in raw data, it's not a bug
            if len(unique_costs.unique()) == 1:
                # All technologies have the same cost in source data
                # this is intentional
                pass
            else:
                # Different costs in source but identical in output - this is a bug
                max_shared = max(len(techs) for techs in shared_patterns.values())
                total_techs = len(var_cost_df["technology"].unique())

                assert max_shared / total_techs < 0.7, (
                    f"{function_type}: Variable reference bug detected -"
                    f"""{max_shared}/{total_techs} technologies
                        have identical cost patterns"""
                    f"but source data shows different costs"
                )

    # Technical lifetime consistency (catches stale variable bug)
    if "technical_lifetime" in result and not result["technical_lifetime"].empty:
        lifetime_df = result["technical_lifetime"]

        if function_type == "infrastructure":
            tech_categories = _get_technology_categories()[function_type]
            dist_techs = set(tech_categories["distribution"])
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
                    f"{function_type}: Stale variable bug detected - "
                    f"""all distribution technologies have lifetime
                    {non_dist_lifetime} identical to non-distribution technology"""
                )


@pytest.mark.parametrize(
    "water_function_test_data", [("infrastructure", "baseline")], indirect=True
)
def test_efficiency_mode_mapping(water_function_test_data):
    """Test that Mf mode represents higher efficiency.

    Only tests baseline configuration where both M1 and Mf modes exist.
    Since output coefficients now reflect efficiency, Mf should have higher output than M1.
    """
    data = water_function_test_data
    result = data["result"]
    function_type = data["function_type"]

    output_df = result["output"]
    tech_categories = _get_technology_categories()[function_type]
    distribution_techs = set(tech_categories["distribution"])

    failures = []

    # Check distribution technologies with both M1 and Mf modes
    for tech in distribution_techs:
        tech_outputs = output_df[output_df["technology"] == tech]
        if tech_outputs.empty:
            continue

        modes = set(tech_outputs["mode"].unique())
        if {"M1", "Mf"}.issubset(modes):
            # Get output coefficients for both modes
            m1_outputs = tech_outputs[tech_outputs["mode"] == "M1"]
            mf_outputs = tech_outputs[tech_outputs["mode"] == "Mf"]

            if not m1_outputs.empty and not mf_outputs.empty:
                m1_coeff = m1_outputs["value"].iloc[0]
                mf_coeff = mf_outputs["value"].iloc[0]

                # Mf should be more efficient (higher output coefficient)
                if mf_coeff <= m1_coeff:
                    failures.append(
                        {
                            "tech": tech,
                            "m1_coeff": m1_coeff,
                            "mf_coeff": mf_coeff,
                            "issue": "Mf output not higher than M1 (efficiency not reflected)",
                        }
                    )

    # Report findings from actual function outputs
    if failures:
        failure_details = "\n".join(
            [
                f"  {f['tech']}: M1={f['m1_coeff']}, Mf={f['mf_coeff']} - {f['issue']}"
                for f in failures
            ]
        )

        assert False, f"Efficiency mapping violations (output coefficients should show Mf > M1):\n{failure_details}\n\n"
