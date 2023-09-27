from unittest.mock import patch

import pandas as pd

from message_ix_models.model.water.data.irrigation import add_irr_structure


def test_add_irr_structure():
    # Mock the context
    context = {
        "water build info": {"Y": [2020, 2030, 2040]},
        "type_reg": "country",
        "regions": "test_region",
        "map_ISO_c": {"test_region": "test_ISO"},
    }

    # Mock the DataFrame read from CSV
    df_node = pd.DataFrame({"BCU_name": ["1", "2"], "REGION": ["region1", "region2"]})

    # Mock the function 'private_data_path' to return the mocked DataFrame
    with patch(
        "message_ix_models.util.private_data_path", return_value="path/to/file"
    ), patch("pandas.read_csv", return_value=df_node):
        # Call the function to be tested
        result = add_irr_structure(context)

        # Assert the results
        assert isinstance(result, dict)
        assert "input" in result
        assert "output" in result
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
