"""Tests for :mod:`message_ix_models.project.sparccle.adapt.workflow`."""

from pathlib import Path

import pandas as pd
import pytest
import yaml

from message_ix_models.project.sparccle.adapt.workflow import (
    COOLING_TYPES,
    cooling_share_sets,
    decline_floor_rows,
    load_config,
    near_zero_growth,
    no_new_capacity,
)

COMBOS = pd.DataFrame(
    {"node_loc": ["R12_WEU", "R12_WEU", "R12_NAM"], "technology": ["a", "b", "a"]}
)


def test_load_config_packaged() -> None:
    config = load_config()

    assert set(config["scenarios"]) == {
        "BAU",
        "malad35",
        "adapt35",
        "adapt35h40",
        "aironly35",
        "nofresh35",
    }
    assert config["scenarios"]["aironly35"]["cooling_ban_types"] == [
        "ot_fresh",
        "ot_saline",
        "cl_fresh",
    ]
    assert config["scenarios"]["nofresh35"]["cooling_ban_types"] == [
        "ot_fresh",
        "cl_fresh",
    ]
    assert config["scenarios"]["adapt35h40"]["half_in"] == 2035
    assert config["scenarios"]["malad35"]["suffix"] == "_malad35"


def test_load_config_half_after_full_raises(tmp_path: Path) -> None:
    config = load_config()
    config["scenarios"] = {"x": {"half_in": 2040, "full_from": 2035}}
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(config))

    with pytest.raises(ValueError, match="half_in"):
        load_config(path)


def test_cooling_share_sets() -> None:
    output = pd.DataFrame(
        {
            "node_loc": ["R12_WEU", "R12_WEU", "R12_NAM", "R12_NAM"],
            "technology": [
                "coal_ppl__ot_fresh",
                "coal_ppl__air",
                "coal_ppl__ot_fresh",
                "x",
            ],
            "commodity": ["ot_fresh", "air", "ot_fresh", "electr"],
        }
    )

    sets = cooling_share_sets(output)

    cat = sets["cat_tec"]
    # one "_share" row per cooling tech, one "_tot" row per tech per cooling type
    assert len(cat) == 2 + 2 * len(COOLING_TYPES)
    assert "x" not in set(cat["technology"])
    assert set(cat.query("technology == 'coal_ppl__air'")["type_tec"]) == {
        "share_cooling_air_share",
        *(f"share_cooling_{c}_tot" for c in COOLING_TYPES),
    }
    share = sets["map_shares_commodity_share"]
    assert len(share) == len(COOLING_TYPES) * 2
    assert (share["node"] == share["node_share"]).all()
    total = sets["map_shares_commodity_total"]
    assert len(total) == len(COOLING_TYPES) ** 2 * 2
    assert set(total["commodity"]) == set(COOLING_TYPES)


def test_cooling_share_sets_empty_raises() -> None:
    with pytest.raises(ValueError, match="No cooling"):
        cooling_share_sets(
            pd.DataFrame({"node_loc": [], "technology": [], "commodity": []})
        )


def test_no_new_capacity_full_only() -> None:
    df = no_new_capacity(COMBOS, [2035, 2040])

    assert len(df) == 6
    assert (df["value"] == 0).all()
    assert set(df["year_vtg"]) == {2035, 2040}


def test_no_new_capacity_half_year_uses_reference() -> None:
    ref = pd.DataFrame(
        {
            "node_loc": ["R12_WEU", "R12_WEU"],
            "technology": ["a", "a"],
            "year_vtg": [2035, 2040],
            "lvl": [4.0, 9.0],
        }
    )

    df = no_new_capacity(COMBOS, [2040], half_year=2035, ref_cap_new=ref)

    half = df[df["year_vtg"] == 2035].set_index(["node_loc", "technology"])["value"]
    assert half[("R12_WEU", "a")] == 2.0
    assert half[("R12_WEU", "b")] == 0.0  # absent from the reference solution
    assert half[("R12_NAM", "a")] == 0.0
    assert (df[df["year_vtg"] == 2040]["value"] == 0).all()
    assert len(df) == 6


def test_no_new_capacity_half_year_requires_reference() -> None:
    with pytest.raises(ValueError, match="ref_cap_new"):
        no_new_capacity(COMBOS, [2040], half_year=2035)


def test_near_zero_growth_full_only() -> None:
    out = near_zero_growth(COMBOS, [2035, 2040], near_zero=0.001)

    growth, initial = out["growth_activity_up"], out["initial_activity_up"]
    assert len(growth) == len(initial) == 6
    assert (growth["value"] == 0.001).all()
    assert (initial["value"] == 0).all()
    assert set(growth["time"]) == {"year"}


def test_near_zero_growth_half_year() -> None:
    ref_growth = pd.DataFrame(
        {
            "node_loc": ["R12_WEU"],
            "technology": ["a"],
            "year_act": [2035],
            "value": [0.08],
        }
    )
    ref_initial = pd.DataFrame(
        {
            "node_loc": ["R12_WEU"],
            "technology": ["a"],
            "year_act": [2035],
            "value": [0.5],
        }
    )

    out = near_zero_growth(
        COMBOS,
        [2040],
        near_zero=0.001,
        half_year=2035,
        ref_growth=ref_growth,
        ref_initial=ref_initial,
        ref_default=0.05,
    )

    g = out["growth_activity_up"]
    g35 = g[g["year_act"] == 2035].set_index(["node_loc", "technology"])["value"]
    assert g35[("R12_WEU", "a")] == pytest.approx(0.04)
    assert g35[("R12_WEU", "b")] == pytest.approx(0.025)  # half of ref_default
    i = out["initial_activity_up"]
    i35 = i[i["year_act"] == 2035].set_index(["node_loc", "technology"])["value"]
    assert i35[("R12_WEU", "a")] == pytest.approx(0.25)
    assert i35[("R12_NAM", "a")] == 0.0
    assert (g[g["year_act"] == 2040]["value"] == 0.001).all()


def test_load_config_adapt_drops_decline_floor() -> None:
    config = load_config()

    assert config["scenarios"]["adapt35"]["drop_decline_floor"] is True
    assert config["scenarios"]["adapt35h40"]["drop_decline_floor"] is True
    assert "drop_decline_floor" not in config["scenarios"]["malad35"]


def test_decline_floor_rows() -> None:
    par = pd.DataFrame(
        {
            "node_loc": ["R12_LAM"] * 4,
            "technology": ["gas_cc", "gas_cc", "gas_cc", "nuc_lc"],
            "year_act": [2030, 2035, 2040, 2035],
            "time": "year",
            "value": [-0.05, -0.05, -1.0, -0.05],
            "unit": "-",
        }
    )

    rows = decline_floor_rows(par, ["gas_cc"], [2035, 2040])

    assert list(rows["year_act"]) == [2035, 2040]
    assert set(rows["technology"]) == {"gas_cc"}
    assert decline_floor_rows(par, ["coal_ppl"], [2035]).empty
