"""Tests for :mod:`message_ix_models.project.sparccle.water.workflow`."""

import pandas as pd
import pytest

from message_ix_models.project.sparccle.water.workflow import (
    LEVERS,
    RENEWABLE_EXTRACTION,
    fossil_gw_cost_rows,
    growth_cap_rows,
    load_config,
    mode_ban_rows,
    platform_of,
    scenario_name,
)


def test_load_config_packaged() -> None:
    config = load_config()

    assert set(config["scenarios"]) == {"base", "BAU", "BAUfg", "maladapt", "adapt"}
    assert config["scenarios"]["BAUfg"]["fossil_gw_var_cost"] == pytest.approx(0.4)
    assert set(config["starters"]) == {"SSP2", "SSP3"}
    assert config["nexus"] == {"rcps": "7p0", "rels": "low"}
    assert config["scenarios"]["base"] == {"build": "nexus", "sdgs": "baseline"}
    assert config["scenarios"]["adapt"]["sdgs"] == "SDG"
    assert config["scenarios"]["adapt"]["label"] == "+adapt"
    assert config["scenarios"]["maladapt"]["ban_mode"] == "Mf"
    assert config["starters"]["SSP3"]["renewable_growth"] == pytest.approx(0.03)
    assert "~" not in config["local_db"]
    for spec in config["scenarios"].values():
        assert ("build" in spec) != ("from" in spec)
    assert set(LEVERS) == {
        "eflow",
        "renewable_growth",
        "ban_mode",
        "fossil_gw_var_cost",
    }


def test_load_config_rejects_build_and_from(tmp_path) -> None:
    bad = tmp_path / "c.yaml"
    bad.write_text("local_db: x\nscenarios:\n  a:\n    build: nexus\n    from: base\n")
    with pytest.raises(ValueError, match="exactly one"):
        load_config(bad)


def test_names() -> None:
    config = load_config()

    assert scenario_name(config, "SSP2", "BAU") == "CurrentPolicies_SSP2_W_BAU"
    assert scenario_name(config, "SSP3", "adapt") == "CurrentPolicies_SSP3_W_+adapt"
    assert platform_of(config, "SSP2") == (
        "sparccle_SSP_SSP2_v6.6_sp_NPiREF_GDP_CI_50_ensemble_2"
    )


def test_growth_cap_rows() -> None:
    growth = pd.DataFrame(
        {
            "node_loc": ["B1", "B1", "B2"],
            "technology": ["extract_surfacewater", "extract_groundwater", "other"],
            "year_act": 2035,
            "time": "year",
            "value": [0.02, 0.02, 0.02],
            "unit": "-",
        }
    )

    df = growth_cap_rows(growth, 0.03)

    assert set(df["technology"]) == set(RENEWABLE_EXTRACTION)
    assert (df["value"] == 0.03).all()
    assert list(df.columns) == list(growth.columns)


def test_mode_ban_rows() -> None:
    output = pd.DataFrame(
        {
            "node_loc": ["B1", "B1", "B1", "B2"],
            "technology": ["urban_t_d", "urban_t_d", "rural_t_d", "urban_t_d"],
            "year_vtg": 2030,
            "year_act": [2035, 2040, 2035, 2035],
            "mode": ["Mf", "Mf", "M1", "Mf"],
            "value": 1.0,
        }
    )

    df = mode_ban_rows(output, "Mf", [2035, 2040])

    assert len(df) == 2 * 2  # (B1, urban_t_d), (B2, urban_t_d) x 2 years
    assert set(df["mode"]) == {"Mf"}
    assert (df["value"] == 0).all()
    assert set(df["year_act"]) == {2035, 2040}
    assert "rural_t_d" not in set(df["technology"])
    assert set(df.columns) >= {
        "node_loc",
        "technology",
        "year_act",
        "mode",
        "time",
        "value",
        "unit",
    }


def test_fossil_gw_cost_rows() -> None:
    inp = pd.DataFrame(
        {
            "node_loc": ["B1", "B1", "B1", "B2"],
            "technology": "extract_gw_fossil",
            "year_vtg": [2030, 2030, 2035, 2035],
            "year_act": [2035, 2040, 2035, 2035],
            "mode": "M1",
            "node_origin": "R12_X",
            "commodity": "electr",
            "level": "final",
            "time": "year",
            "time_origin": "year",
            "value": [0.0001] * 4,
            "unit": "GWa/MCM",
        }
    )

    df = fossil_gw_cost_rows(inp, 0.4)

    assert len(df) == 4
    assert (df["value"] == 0.4).all() and (df["unit"] == "USD/MCM").all()
    assert set(df.columns) == {
        "node_loc",
        "technology",
        "year_vtg",
        "year_act",
        "mode",
        "time",
        "value",
        "unit",
    }
