from unittest.mock import patch

import pandas as pd

import message_ix_models.project.digsy.data as digsy_data


def test_read_config() -> None:
    with patch(
        "message_ix_models.project.digsy.data.read_yaml_file",
        return_value={"key": "value"},
    ):
        with patch(
            "message_ix_models.project.digsy.data.package_data_path",
            return_value="dummy_path",
        ):
            config = digsy_data.read_config()
            assert config == {"key": "value"}


def test_read_industry_file() -> None:
    config = {"industry_input": {"file_name": "file.xlsx", "sheet_name": "Sheet1"}}
    dummy_df = pd.DataFrame(
        {"TRModified_agg_A": [1], "subsector": ["foo"], "Electric or thermal": ["bar"]}
    )
    with patch(
        "message_ix_models.project.digsy.data.package_data_path",
        return_value="dummy_path",
    ):
        with patch("pandas.read_excel", return_value=dummy_df):
            df = digsy_data.read_industry_file(config)
            assert "A" in df.columns or "subsector" in df.columns


def test_get_industry_modifiers() -> None:
    config = {
        "industry_input": {"file_name": "file.xlsx", "sheet_name": "Sheet1"},
        "subsector_message_map": {
            "subsector_b": {
                "par": "parameter",
                "technology": ["t"],
                "commodity": ["c"],
            }
        },
    }
    dummy_df = pd.DataFrame(
        {
            "subsector": ["subsector_b"],
            "Electric or thermal": [None],
            "region": ["reg_x"],
            "sector": ["sector_a"],
            "scenario": ["BEST"],
            "Variable": [None],
        }
    )
    with patch("message_ix_models.project.digsy.data.read_config", return_value=config):
        with patch(
            "message_ix_models.project.digsy.data.read_industry_file",
            return_value=dummy_df,
        ):
            df = digsy_data.get_industry_modifiers("BEST")
            assert "technology" in df.columns
            assert "commodity" in df.columns


def test_apply_industry_modifiers() -> None:
    mods = pd.DataFrame(
        {
            "par": ["parameter1"],
            "region": ["reg_x"],
            "sector": ["sector_a"],
            "technology": ["T"],
            "commodity": ["C"],
            2020: [-0.5],
        }
    )
    pars = {
        "parameter1": pd.DataFrame(
            {
                "node": ["R12_reg_x"],
                "technology": ["T"],
                "commodity": ["C"],
                "year": [2020],
                "value": [2.0],
            }
        )
    }
    result = digsy_data.apply_industry_modifiers(mods, pars)
    assert "parameter1" in result.keys()
    assert result["parameter1"].value[0] == 1


def test_read_ict_demand() -> None:
    dummy_dfs = {
        "2030": pd.DataFrame(
            {
                "Region": ["A"],
                "Year": [2030],
                "Parent_Region": ["X"],
                "Allocated_TWh": [1000],
            }
        ),
        "Lower Bound": pd.DataFrame(
            {
                "Scenario": ["SSP2"],
                "Region": ["A"],
                "Year": [2040],
                "Parent_Region": ["X"],
                "Source": ["Y"],
                "Allocated_TWh": [1000],
            }
        ),
    }
    with patch(
        "message_ix_models.project.digsy.data.package_data_path",
        return_value="dummy_path",
    ):
        with patch("pandas.read_excel", return_value=dummy_dfs):
            with patch(
                "message_ix_models.project.digsy.data.make_df",
                side_effect=lambda *args, **kwargs: pd.DataFrame(
                    {"node": ["A"], "year": [2030], "value": [10], "unit": ["GWa"]}
                ),
            ):
                df = digsy_data.read_ict_demand()
                assert "node" in df.columns
                assert "value" in df.columns
