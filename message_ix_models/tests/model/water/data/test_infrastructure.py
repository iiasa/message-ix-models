import pytest

from message_ix_models.model.water.data.infrastructure import (
    add_desalination,
    add_infrastructure_techs,
)


@pytest.mark.parametrize(
    "water_context",
    [
        {"regions": "ZMB", "type_reg": "country", "SDG": "baseline"},
        {"regions": "ZMB", "type_reg": "country", "SDG": "not_baseline"},
    ],
    indirect=True,
)
def test_add_infrastructure_techs(
    water_context, water_scenario, assert_message_params, assert_input_output_structure
):
    """Test add_infrastructure_techs with country model configuration.

    Also tests start_creating_input_dataframe() and prepare_input_dataframe()
    since they are called by add_infrastructure_techs().
    """
    result = add_infrastructure_techs(context=water_context)

    # Standard MESSAGE parameter validation
    assert_message_params(result, expected_keys=["input", "output"])
    assert_input_output_structure(result)


@pytest.mark.parametrize(
    "water_context",
    [{"regions": "R11", "type_reg": "global", "RCP": "7p0"}],
    indirect=True,
)
def test_add_desalination(
    water_context, water_scenario, assert_message_params, assert_input_output_structure
):
    """Test add_desalination with global model configuration."""
    result = add_desalination(context=water_context)

    # Standard MESSAGE parameter validation
    assert_message_params(result, expected_keys=["input", "output"])
    assert_input_output_structure(result)
