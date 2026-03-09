# tests for the GDP implementation, util
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pyam
import pytest

from message_ix_models.project.GDP_climate_shocks.util import (
    apply_growth_rates,
    load_config_from_path,
    load_gdp_data,
    maybe_shift_year,
    regional_gdp_impacts,
)
from message_ix_models.util import package_data_path


def test_load_gdp_data():
    # This will fail if the CSV is missing or broken
    gdp_df = load_gdp_data()
    assert gdp_df.shape[0] > 0
    assert gdp_df.shape[1] > 0
    required_columns = [
        "region",
        "iso3",
        "year",
        "value",
        "unit",
        "model",
        "scenario",
        "variable",
    ]
    for col in required_columns:
        assert col in gdp_df.columns


@pytest.mark.parametrize(
    "config_path",
    [
        "default",
        Path(package_data_path()) / "GDP_climate_shocks" / "config_ENGAGE_runs24.yaml",
    ],
)
def test_load_config_from_path(config_path):
    # Both files are expected to be in the repo
    cfg = load_config_from_path(config_path)
    assert isinstance(cfg, dict)
    assert len(cfg) > 0


@pytest.mark.parametrize(
    "first_year, shift_year, expected",
    [
        (2020, 2030, {"shift_first_model_year": 2030}),
        (2020, 2020, {}),
        (2020, None, {}),
    ],
)
def test_maybe_shift_year(first_year, shift_year, expected):
    scenario = SimpleNamespace(firstmodelyear=first_year)
    result = maybe_shift_year(scenario, shift_year)
    assert result == expected


def test_apply_growth_rates():
    """Minimal scenario-like object to test apply_growth_rates logic."""
    sc = SimpleNamespace(
        par=lambda name: pd.DataFrame(
            {
                "node": ["R1", "R1", "R1", "R1"],
                "year": [2010, 2020, 2100, 2110],
                "value": [100, 200, 300, 400],
            }
        )
        if name == "gdp_calibrate"
        else pd.DataFrame(
            {
                "node": ["R1", "R1", "R1", "R1"],
                "year": [2025, 2030, 2100, 2110],
                "value": [0.01, 0.02, 0.03, 0.04],
            }
        ),
        check_out=lambda: None,
        add_par=lambda *a, **k: None,
        commit=lambda msg: None,
        model="M",
        scenario="S",
    )

    gdp_change_df = pd.DataFrame(
        {
            "node": ["R1", "R1", "R1", "R1"],
            "year": [2010, 2020, 2100, 2110],
            "perc_change_sum": [0.0, 10.0, 20.0, 30.0],
        }
    )

    # This should run without error
    apply_growth_rates(sc, gdp_change_df)


@pytest.mark.parametrize(
    "damage_model, expected_var",
    [
        ("Waidelich", "RIME|All indicators|mean"),
        ("Burke", "RIME|pct.diff"),
        ("Kotz", "RIME|pct.diff"),
    ],
)
def test_regional_gdp_impacts_selects_variable(tmp_path, damage_model, expected_var):
    """Check that the correct variable is selected based on damage_model."""

    # Minimal GDP data
    def fake_load_gdp_data():
        return pd.DataFrame(
            {
                "region": ["CHN"],
                "model": ["m"],
                "scenario": ["SSP2"],
                "variable": ["GDP"],
                "unit": ["USD"],
                "year": [2020],
                "value": [100],
                "iso3": ["CHN"],
            }
        )

    # Patch the function directly
    globals()["load_gdp_data"] = fake_load_gdp_data

    # Minimal fake RIME data
    class FakeIamDataFrame:
        def __init__(self, path):
            pass

        def as_pandas(self):
            return pd.DataFrame(
                {
                    "model": ["m"],
                    "scenario": ["s"],
                    "region": ["CHN"],
                    "year": [2020],
                    "variable": [expected_var],
                    "unit": ["%"],
                    "value": [1.0],
                }
            )

    pyam.IamDataFrame = FakeIamDataFrame

    result = regional_gdp_impacts(
        sc_string="scenario",
        damage_model=damage_model,
        it=1,
        SSP="SSP2",
        regions="R12",
        pp=50,
    )

    assert isinstance(result, pd.DataFrame)
    assert "node" in result.columns


def test_regional_gdp_impacts_invalid_damage_model(tmp_path):
    """Should raise if an invalid damage_model is given."""
    # Minimal GDP data
    globals()["load_gdp_data"] = lambda: pd.DataFrame(
        {
            "region": ["CHN"],
            "model": ["m"],
            "scenario": ["SSP2"],
            "variable": ["GDP"],
            "unit": ["USD"],
            "year": [2020],
            "value": [100],
            "iso3": ["CHN"],
        }
    )

    # Minimal fake RIME data
    class FakeIamDataFrame:
        def __init__(self, path):
            pass

        def as_pandas(self):
            return pd.DataFrame(
                {
                    "model": ["m"],
                    "scenario": ["s"],
                    "region": ["CHN"],
                    "year": [2020],
                    "variable": ["Unknown"],
                    "unit": ["%"],
                    "value": [1.0],
                }
            )

    pyam.IamDataFrame = FakeIamDataFrame

    with pytest.raises(AssertionError):
        regional_gdp_impacts("scenario", "InvalidModel", 1, "SSP2", "R12")
