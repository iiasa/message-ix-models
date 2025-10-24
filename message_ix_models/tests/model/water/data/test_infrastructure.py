import pandas as pd
import pytest

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
    base_cols = [
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
        input_cols = base_cols + ["time", "time_origin", "node_origin"]
        output_cols = base_cols + ["time", "time_dest", "node_dest"]
    else:  # desalination
        input_cols = base_cols + ["time_origin", "node_origin"]
        output_cols = base_cols + ["time"]

    return input_cols, output_cols


def _validate_dataframe(df, df_name, function_type):
    """Validate a DataFrame for NaN values, duplicates, and time format."""
    # NaN check
    assert not df["value"].isna().any(), (
        f"{function_type}: {df_name} DataFrame contains NaN values"
    )

    # Time format validation
    if "time" in df.columns:
        time_values = df["time"].unique()
        invalid_times = [val for val in time_values if len(str(val)) == 1]
        assert not invalid_times, (
            f"{function_type}: {df_name} DataFrame has invalid time values: "
            f"{invalid_times}"
        )

    # Duplicate detection
    duplicates = df.duplicated().sum()
    assert duplicates == 0, (
        f"{function_type}: {df_name} DataFrame contains {duplicates} duplicate rows"
    )


# Assertion helper functions


def assert_data_structure(result, function_type):
    """Test for data structure, integrity, and configuration-specific behavior."""
    # Basic structure checks
    assert isinstance(result, dict), f"{function_type}: Result must be a dictionary"
    assert "input" in result, f"{function_type}: Result must contain 'input' key"
    assert "output" in result, f"{function_type}: Result must contain 'output' key"

    # Column validation
    input_cols, output_cols = _get_required_columns(function_type)

    missing_input = set(input_cols) - set(result["input"].columns)
    assert not missing_input, (
        f"{function_type}: Input DataFrame missing columns: {missing_input}"
    )

    missing_output = set(output_cols) - set(result["output"].columns)
    assert not missing_output, (
        f"{function_type}: Output DataFrame missing columns: {missing_output}"
    )

    # Data integrity validation
    _validate_dataframe(result["input"], "Input", function_type)
    _validate_dataframe(result["output"], "Output", function_type)


def assert_technology_completeness(result, function_type, sdg_config=None):
    """Test for technology completeness, data pairing, and mode consistency."""
    tech_categories = _get_technology_categories()[function_type]
    input_techs = set(result["input"]["technology"].unique())
    output_techs = set(result["output"]["technology"].unique())

    if function_type == "infrastructure":
        distribution_techs = set(tech_categories["distribution"])

        # Distribution technology completeness
        missing_dist = distribution_techs - input_techs
        assert not missing_dist, (
            f"{function_type}: Distribution technologies missing from input: "
            f"{missing_dist}"
        )

        # Electric/non-electric pairing
        input_df = result["input"]
        elec_filter = input_df["commodity"] == "electr"
        elec_techs = set(input_df[elec_filter]["technology"].unique())
        non_elec_filter = input_df["commodity"] != "electr"
        non_elec_techs = set(input_df[non_elec_filter]["technology"].unique())
        missing_non_elec = elec_techs - non_elec_techs
        assert not missing_non_elec, (
            f"{function_type}: Technologies with electricity input missing "
            f"non-electric input: {missing_non_elec}"
        )

    # Input/output mode pairing
    output_modes = {
        (r["technology"], r["mode"]) for _, r in result["output"].iterrows()
    }
    input_modes = {(r["technology"], r["mode"]) for _, r in result["input"].iterrows()}
    missing = {
        (t, m)
        for t, m in (output_modes - input_modes)
        if "extract_salinewater" not in t
    }

    if missing:
        by_mode = {}
        for tech, mode in missing:
            by_mode.setdefault(mode, []).append(tech)
        details = "\n".join([f"  {mode}: {techs}" for mode, techs in by_mode.items()])
        assert False, (
            f"{function_type}: Output modes missing corresponding inputs:\n{details}"
        )

    # Mode consistency for infrastructure
    if function_type == "infrastructure" and sdg_config is not None:
        distribution_techs = set(tech_categories["distribution"])
        for tech in input_techs & output_techs:
            is_dist = tech in distribution_techs
            is_baseline = sdg_config == "baseline"
            expected = (
                {"M1", "Mf"}
                if is_dist and is_baseline
                else {"Mf"}
                if is_dist
                else {"M1"}
            )

            inp_filter = result["input"]["technology"] == tech
            inp_modes = set(result["input"][inp_filter]["mode"].unique())
            out_filter = result["output"]["technology"] == tech
            out_modes = set(result["output"][out_filter]["mode"].unique())

            assert inp_modes == expected, (
                f"{function_type}: {tech} input modes {inp_modes} != "
                f"expected {expected} ({sdg_config})"
            )
            assert out_modes == expected, (
                f"{function_type}: {tech} output modes {out_modes} != "
                f"expected {expected} ({sdg_config})"
            )


def assert_parameter_consistency(result, function_type):
    """Test for parameter consistency and expected constraints."""
    if function_type == "desalination":
        for param in ["bound_total_capacity_up", "bound_activity_lo"]:
            assert param in result, f"desalination: Missing '{param}' constraint"
            assert not result[param].empty, (
                f"desalination: '{param}' DataFrame is empty"
            )


def assert_mode_mapping(result, function_type):
    """Test that Mf mode represents higher efficiency than M1.

    Baseline configuration has both M1 and Mf modes.
    Output coefficients should show Mf > M1 for distribution technologies.
    """
    output_df = result["output"]
    tech_categories = _get_technology_categories()[function_type]
    distribution_techs = set(tech_categories["distribution"])

    failures = []
    for tech in distribution_techs:
        tech_out = output_df[output_df["technology"] == tech]
        if tech_out.empty or not {"M1", "Mf"}.issubset(set(tech_out["mode"].unique())):
            continue

        m1_val = tech_out[tech_out["mode"] == "M1"]["value"].iloc[0]
        mf_val = tech_out[tech_out["mode"] == "Mf"]["value"].iloc[0]

        if mf_val <= m1_val:
            failures.append(f"{tech}: M1={m1_val}, Mf={mf_val}")

    if failures:
        details = "\n  ".join(failures)
        assert False, f"Mf output should exceed M1 (efficiency):\n  {details}"


# Test functions


@pytest.mark.parametrize(
    "water_scenario",
    [
        {"regions": "R12", "type_reg": "country", "time": "year", "SDG": "baseline"},
        {
            "regions": "R12",
            "type_reg": "country",
            "time": "year",
            "SDG": "not_baseline",
        },
    ],
    indirect=True,
)
def test_add_infrastructure_techs(water_scenario):
    """Test add_infrastructure_techs function with different SDG configurations."""
    context = water_scenario["context"]
    result = add_infrastructure_techs(context=context)

    sdg_config = context.SDG

    # Use helper assertions
    assert_data_structure(result, "infrastructure")
    assert_technology_completeness(result, "infrastructure", sdg_config)
    assert_parameter_consistency(result, "infrastructure")

    # Only test mode mapping for baseline configuration
    if sdg_config == "baseline":
        assert_mode_mapping(result, "infrastructure")


@pytest.mark.parametrize(
    "water_scenario",
    [
        {"regions": "R12", "type_reg": "global", "time": "year", "RCP": "7p0"},
    ],
    indirect=True,
)
def test_add_desalination(water_scenario):
    """Test add_desalination function."""
    context = water_scenario["context"]
    result = add_desalination(context=context)

    # Use helper assertions
    assert_data_structure(result, "desalination")
    assert_technology_completeness(result, "desalination")
    assert_parameter_consistency(result, "desalination")
