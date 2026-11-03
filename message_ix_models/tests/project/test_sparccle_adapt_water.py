"""Tests for :mod:`message_ix_models.project.sparccle.adapt.water.workflow`."""

import pandas as pd
import pytest

from message_ix_models.project.sparccle.adapt.water.workflow import (
    DESAL_PLANTS,
    LEVERS,
    RENEWABLE_EXTRACTION,
    desal_potential_rows,
    desal_seed_rows,
    fossil_gw_cap_rows,
    fossil_gw_cost_rows,
    growth_cap_rows,
    load_config,
    mode_ban_rows,
    platform_of,
    scenario_name,
)


def test_load_config_packaged() -> None:
    config = load_config()

    assert set(config["scenarios"]) == {
        "base",
        "BAUfgd",
        "maladapt",
        "adapt",
    }
    for key in ("BAUfgd", "maladapt", "adapt"):
        assert config["scenarios"][key]["fossil_gw_cap"] == pytest.approx(1.0)
    assert "fossil_gw_cap" not in config["scenarios"]["base"]
    for spec in config["scenarios"].values():
        assert "desal_potential" not in spec  # lever available, unused
    assert config["scenarios"]["BAUfgd"]["desal_free"] is True
    assert set(config["starters"]) == {"SSP2", "SSP3"}
    assert config["nexus"] == {"rcps": "7p0", "rels": "low"}
    assert config["scenarios"]["base"] == {"build": "nexus", "sdgs": "baseline"}
    assert config["scenarios"]["adapt"]["sdgs"] == "SDG"
    assert config["scenarios"]["adapt"]["label"] == "+adapt"
    assert config["scenarios"]["maladapt"]["ban_mode"] == "Mf"
    assert config["scenarios"]["adapt"]["fossil_gw_var_cost"] == pytest.approx(0.4)
    assert config["scenarios"]["maladapt"]["fossil_gw_var_cost"] == pytest.approx(0.7)
    assert config["scenarios"]["maladapt"]["desal_free"] is True
    assert "desal_free" not in config["scenarios"]["adapt"]
    for key in ("BAUfgd", "maladapt"):
        assert config["scenarios"][key]["ban_mode"] == "Mf"
    assert "ban_mode" not in config["scenarios"]["adapt"]
    assert config["starters"]["SSP3"]["renewable_growth"] == pytest.approx(0.03)
    assert "~" not in config["local_db"]
    for spec in config["scenarios"].values():
        assert ("build" in spec) != ("from" in spec)
    assert set(LEVERS) == {
        "eflow",
        "renewable_growth",
        "ban_mode",
        "fossil_gw_var_cost",
        "desal_free",
        "fossil_gw_cap",
        "desal_potential",
    }
    assert DESAL_PLANTS == ["membrane", "distillation"]


def test_load_config_rejects_build_and_from(tmp_path) -> None:
    bad = tmp_path / "c.yaml"
    bad.write_text("local_db: x\nscenarios:\n  a:\n    build: nexus\n    from: base\n")
    with pytest.raises(ValueError, match="exactly one"):
        load_config(bad)


def test_names() -> None:
    config = load_config()

    assert scenario_name(config, "SSP2", "BAUfgd") == "CurrentPolicies_SSP2_W_BAUfgd"
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


def test_desal_seed_rows() -> None:
    hist = pd.DataFrame(
        {
            "node_loc": ["B1", "B1", "B2"],
            "technology": ["membrane", "membrane", "distillation"],
            "year_vtg": [2020, 2025, 2025],
            "value": [1.0, 2.0, 3.0],
            "unit": "MCM/year",
        }
    )

    df = desal_seed_rows(hist, 2025, 2030)

    assert len(df) == 2
    assert set(df["year_vtg"]) == {2030}
    assert sorted(df["value"]) == [2.0, 3.0]
    assert list(df.columns) == list(hist.columns)


def test_fossil_gw_cap_rows() -> None:
    hist = pd.DataFrame(
        {
            "node_loc": ["B1", "B1", "B2"],
            "technology": "extract_gw_fossil",
            "year_act": [2025, 2030, 2030],
            "mode": "M1",
            "time": "year",
            "value": [5.0, 10.0, 4.0],
            "unit": "MCM/year",
        }
    )
    inp = pd.DataFrame(
        {
            "node_loc": ["B1", "B1", "B2", "B3"],
            "technology": "extract_gw_fossil",
            "year_vtg": [2030, 2035, 2035, 2035],
            "year_act": [2035, 2035, 2035, 2035],
            "mode": "M1",
            "time": "year",
            "value": 0.0001,
            "unit": "GWa/MCM",
        }
    )

    df = fossil_gw_cap_rows(hist, inp, [2035, 2040], 1.0)

    assert len(df) == 2 * 2  # two basins with history x two years
    assert set(df["year_act"]) == {2035, 2040}
    by_node = df[df["year_act"] == 2035].set_index("node_loc")["value"]
    assert by_node["B1"] == pytest.approx(10.0)  # last historical year, not 2025
    assert by_node["B2"] == pytest.approx(4.0)
    assert "B3" not in by_node.index  # no history -> no cap, stays unbounded
    assert (df["unit"] == "MCM/year").all()
    assert (fossil_gw_cap_rows(hist, inp, [2035], 1.5)["value"].max()) == pytest.approx(
        15.0
    )


def test_desal_potential_rows() -> None:
    bound = pd.DataFrame(
        {
            "node_loc": ["B1|X", "B1|X", "B2|X", "B2|X"],
            "technology": "extract_salinewater_basin",
            "year_act": [2035, 2050, 2035, 2050],
            "value": [1.0, 2.0, 3.0, 4.0],
            "unit": "MCM/year",
        }
    )
    proj = pd.DataFrame(
        {
            "BCU_name": ["1|X", "1|X", "1|X", "2|X", "2|X"],
            "ssp": ["SSP1", "SSP3", "SSP3", "SSP3", "SSP3"],
            "year": [2035, 2035, 2050, 2035, 2065],
            "cap_km3_year": [0.9, 0.5, -0.1, 0.25, 9.0],
        }
    )

    df = desal_potential_rows(bound, proj, "SSP3")

    assert list(df.columns) == list(bound.columns)
    assert len(df) == len(bound)
    got = df.set_index(["node_loc", "year_act"])["value"]
    assert got[("B1|X", 2035)] == pytest.approx(500.0)  # km3 -> MCM
    assert got[("B1|X", 2050)] == pytest.approx(0.0)  # negative clipped
    assert got[("B2|X", 2035)] == pytest.approx(250.0)
    assert got[("B2|X", 2050)] == pytest.approx(0.0)  # missing year -> zero
    with pytest.raises(ValueError, match="SSP9"):
        desal_potential_rows(bound, proj, "SSP9")
