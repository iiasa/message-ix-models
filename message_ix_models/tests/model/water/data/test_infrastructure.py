import pytest

from message_ix_models.model.water.data.infrastructure import (
    add_desalination,
    add_infrastructure_techs,
)


@pytest.mark.parametrize(
    "water_context",
    [
        # Global R11
        {"regions": "R11", "type_reg": "global", "SDG": "baseline"},
        {"regions": "R11", "type_reg": "global", "SDG": "not_baseline"},
        # Global R12
        {"regions": "R12", "type_reg": "global", "SDG": "baseline"},
        {"regions": "R12", "type_reg": "global", "SDG": "not_baseline"},
        # Country ZMB
        {"regions": "ZMB", "type_reg": "country", "SDG": "baseline"},
        {"regions": "ZMB", "type_reg": "country", "SDG": "not_baseline"},
    ],
    indirect=True,
)
def test_add_infrastructure_techs(
    water_context, water_scenario, assert_message_params, assert_input_output_structure
):
    """Test add_infrastructure_techs with global and country model configurations.

    Also tests start_creating_input_dataframe() and prepare_input_dataframe()
    since they are called by add_infrastructure_techs().
    """
    result = add_infrastructure_techs(context=water_context)

    # Standard MESSAGE parameter validation
    assert_message_params(result, expected_keys=["input", "output"])
    assert_input_output_structure(result)


@pytest.mark.parametrize(
    "water_context",
    [
        # Global R11 (has 6p0)
        {"regions": "R11", "type_reg": "global", "RCP": "6p0"},
        # Global R12 (no 6p0, use 7p0)
        {"regions": "R12", "type_reg": "global", "RCP": "7p0"},
        # Country ZMB (no 6p0, use 7p0)
        {"regions": "ZMB", "type_reg": "country", "RCP": "7p0"},
    ],
    indirect=True,
)
def test_add_desalination(
    water_context, water_scenario, assert_message_params, assert_input_output_structure
):
    """Test add_desalination with global and country model configurations."""
    result = add_desalination(context=water_context)

    # Standard MESSAGE parameter validation
    assert_message_params(result, expected_keys=["input", "output"])
    assert_input_output_structure(result)
