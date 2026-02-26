import pytest

from message_ix_models.model.water.data.irrigation import add_irr_structure
from message_ix_models.tests.model.water.conftest import water_params


@pytest.mark.parametrize(
    "water_context",
    [
        water_params("ZMB"),
        water_params("ZMB", reduced_basin=True),
    ],
    indirect=True,
)
def test_add_irr_structure(
    water_context, water_scenario, assert_input_output_structure
):
    """Test add_irr_structure with country model configurations."""
    result = add_irr_structure(water_context)

    assert isinstance(result, dict)
    assert_input_output_structure(result)

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
    assert all(
        col in result["output"].columns
        for col in [
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
    )
