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
    test_context.type_reg = "global"
    test_context.regions = "R12"
    nodes = get_codes(f"node/{test_context.regions}")
    nodes = list(map(str, nodes[nodes.index("World")].child))
    test_context.map_ISO_c = {test_context.regions: nodes[0]}

    # Add RCP for desalination
    if function_type == "desalination":
        test_context.RCP = "7p0"  # Shouldn't require a param for this

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
        ("desalination", "baseline"),
        ("desalination", "not_baseline"),
    ],
    indirect=True,
    ids=[
        "infrastructure-baseline",
        "infrastructure-sdg",
        "desalination-baseline",
        "desalination-sdg",
    ],
)
def test_data_quality(water_function_test_data):
    """Test basic data quality
    Validates that the output DataFrames have:
    - Required columns present
    - No NaN values in the value column
    - No duplicate rows
    - Valid time format (not single character)
    """
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
    ],
    indirect=True,
    ids=["infrastructure-baseline", "infrastructure-sdg"],
)
def test_infrastructure_modes(water_function_test_data):
    """Test all M1/Mf mode requirements for infrastructure technologies.

    Validates:
    - Distribution technologies have correct modes (M1+Mf for baseline, Mf only for SDG)
    - Non-distribution technologies only have M1 mode
    - All output modes have corresponding input modes
    - Mf mode is more efficient than M1 (lower input coefficients)
    - No early return bugs (all distribution techs present)
    """
    data = water_function_test_data
    result = data["result"]
    function_type = data["function_type"]
    sdg_config = data["sdg_config"]
    tech_categories = _get_technology_categories()[function_type]
    input_techs = set(result["input"]["technology"].unique())
    output_techs = set(result["output"]["technology"].unique())
    input_df = result["input"]

    # Distribution technology completeness
    expected_dist_techs = set(tech_categories["distribution"])
    missing_dist_input = expected_dist_techs - input_techs
    assert (
        not missing_dist_input
    ), f"""{function_type}: Distribution technologies missing
        from input (early return bug): {missing_dist_input}"""

    # Electric/non-electric pairing
    elec_techs = set(
        result["input"][result["input"]["commodity"] == "electr"]["technology"].unique()
    )
    non_elec_techs = set(
        result["input"][result["input"]["commodity"] != "electr"]["technology"].unique()
    )
    missing_non_elec = elec_techs - non_elec_techs
    assert (
        not missing_non_elec
    ), f"""{function_type}: Technologies with electricity input
        missing non-electric input: {missing_non_elec}"""

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

        error_details = "\\n".join(
            [f"  {mode} mode: {techs}" for mode, techs in missing_by_mode.items()]
        )
        error_msg = f"""{function_type}: Technologies with output modes missing
        corresponding input modes:\\n{error_details}"""

        assert False, error_msg

    # Mode consistency validation between input and output for infrastructure
    common_techs = input_techs.intersection(output_techs)
    distribution_techs = set(tech_categories["distribution"])

    if common_techs:
        # Single pass through technologies to get modes, coefficients, and validate
        tech_data = {}
        for tech in common_techs:
            tech_inputs = input_df[input_df["technology"] == tech]
            tech_outputs = result["output"][result["output"]["technology"] == tech]

            input_modes = set(tech_inputs["mode"].unique())
            output_modes = set(tech_outputs["mode"].unique())

            # Get M1/Mf coefficients if both modes exist
            m1_coeff = mf_coeff = None
            if {"M1", "Mf"}.issubset(input_modes):
                m1_data = tech_inputs[
                    (tech_inputs["mode"] == "M1")
                    & (tech_inputs["commodity"] != "electr")
                ]
                mf_data = tech_inputs[
                    (tech_inputs["mode"] == "Mf")
                    & (tech_inputs["commodity"] != "electr")
                ]
                if not m1_data.empty and not mf_data.empty:
                    m1_coeff = m1_data["value"].iloc[0]
                    mf_coeff = mf_data["value"].iloc[0]

            tech_data[tech] = {
                "input_modes": input_modes,
                "output_modes": output_modes,
                "m1_coeff": m1_coeff,
                "mf_coeff": mf_coeff,
            }

        # Validate all technologies using collected data
        for tech, data in tech_data.items():
            # Distribution technologies should have both M1 and Mf in baseline,
            # only Mf in non-baseline
            # Non-distribution technologies should only have M1
            if tech in distribution_techs:
                expected_modes = {"M1", "Mf"} if sdg_config == "baseline" else {"Mf"}
            else:
                expected_modes = {"M1"}

            assert data["input_modes"] == expected_modes, (
                f"{function_type}: Technology {tech} has incorrect input "
                f"modes for {sdg_config} configuration. "
                f"Expected: {expected_modes}, Found: {data['input_modes']}"
            )
            assert data["output_modes"] == expected_modes, (
                f"{function_type}: Technology {tech} has incorrect output "
                f"modes for {sdg_config} configuration. "
                f"Expected: {expected_modes}, Found: {data['output_modes']}"
            )

            # Test efficiency: Mf should be more efficient than
            # M1 for distribution techs in baseline
            if (
                tech in distribution_techs
                and sdg_config == "baseline"
                and data["m1_coeff"] is not None
                and data["mf_coeff"] is not None
            ):
                assert data["mf_coeff"] < data["m1_coeff"], (
                    f"{function_type}: Technology {tech} - Mf < efficient than M1. "
                    f"M1={data['m1_coeff']}, Mf={data['mf_coeff']}"
                )
