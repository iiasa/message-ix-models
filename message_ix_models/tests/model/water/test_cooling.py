from unittest.mock import MagicMock, patch

import pandas as pd

from message_ix_models.model.water.data.water_for_ppl import non_cooling_tec


def test_non_cooling_tec():
    # Mock the context
    context = {
        "water build info": {"Y": [2020, 2030, 2040]},
        "type_reg": "country",
        "regions": "test_region",
        "map_ISO_c": {"test_region": "test_ISO"},
        "get_scenario": MagicMock(
            return_value=MagicMock(
                par=MagicMock(
                    return_value=pd.DataFrame(
                        {
                            "technology": ["tech1", "tech2"],
                            "node_loc": ["loc1", "loc2"],
                            "node_dest": ["dest1", "dest2"],
                            "year_vtg": ["2020", "2020"],
                            "year_act": ["2020", "2020"],
                        }
                    )
                )
            )
        ),
    }

    # Mock the DataFrame read from CSV
    df = pd.DataFrame(
        {
            "technology_group": ["cooling", "non-cooling"],
            "technology_name": ["cooling_tech1", "non_cooling_tech1"],
            "water_supply_type": ["freshwater_supply", "freshwater_supply"],
            "water_withdrawal_mid_m3_per_output": [1, 2],
        }
    )

    # Mock the function 'private_data_path' to return the mocked DataFrame
    with patch(
        "message_ix_models.util.private_data_path", return_value="path/to/file"
    ), patch("pandas.read_csv", return_value=df):
        # Call the function to be tested
        result = non_cooling_tec(context)

        # Assert the results
        assert isinstance(result, dict)
        assert "input" in result
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
