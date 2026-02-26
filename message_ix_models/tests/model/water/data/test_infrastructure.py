import pytest

from message_ix_models.model.water.data.infrastructure import (
    add_desalination,
    add_infrastructure_techs,
)
from message_ix_models.tests.model.water.conftest import water_params


@pytest.mark.parametrize(
    "water_context",
    [
        water_params("R11", SDG="baseline"),
        water_params("R11", SDG="not_baseline"),
        water_params("R12", SDG="baseline"),
        water_params("R12", SDG="not_baseline"),
        water_params("ZMB", SDG="baseline"),
        water_params("ZMB", SDG="not_baseline"),
        water_params("R12", reduced_basin=True, SDG="baseline"),
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
        water_params("R11", RCP="6p0"),
        water_params("R12", RCP="7p0"),
        water_params("ZMB", RCP="7p0"),
        water_params("R12", reduced_basin=True, RCP="7p0"),
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
