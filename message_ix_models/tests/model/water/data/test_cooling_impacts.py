"""Tests for model.water.data.cooling_impacts -- wet-cooling constraints."""

import numpy as np
import pandas as pd

from message_ix_models.model.water.data.cooling_impacts import (
    _ratios_to_long,
    build_dry_cooling_factors,
    build_wet_cooling_constraints,
    compute_degradation_ratios,
)


def test_ratio_at_baseline() -> None:
    gmt = np.array([1.0])
    ratios = compute_degradation_ratios(gmt, [2020], baseline_gwl=1.0)
    np.testing.assert_allclose(ratios.values, 1.0, atol=0.01)


def test_ratio_changes_with_warming() -> None:
    gmt = np.array([1.0, 2.0, 3.0, 4.0])
    ratios = compute_degradation_ratios(gmt, [2020, 2030, 2040, 2050], baseline_gwl=1.0)
    # Warming should move the regional ratios away from the baseline value.
    assert (ratios[2050] - ratios[2020]).abs().gt(0.01).any()


def test_ratio_shape_matches_input() -> None:
    gmt = np.linspace(1.0, 3.0, 6)
    ratios = compute_degradation_ratios(gmt, [2020, 2030, 2040, 2050, 2060, 2070])
    assert ratios.shape == (12, 6)


_ADDON_DF = pd.DataFrame(
    {
        "type_addon": [
            "cooling__coal_ppl",
            "cooling__coal_ppl",
            "cooling__gas_ppl",
        ],
        "node": ["R12_AFR", "R12_WEU", "R12_AFR"],
        "technology": ["coal_ppl", "coal_ppl", "gas_ppl"],
        "year_vtg": [2020, 2020, 2020],
        "year_act": [2020, 2020, 2020],
        "mode": ["M1", "M1", "M1"],
        "time": ["year", "year", "year"],
        "value": [1.5, 1.5, 1.2],
        "unit": ["-", "-", "-"],
    }
)

_TECHS = {
    "coal_ppl",
    "coal_ppl__cl_fresh",
    "coal_ppl__ot_fresh",
    "coal_ppl__air",
    "gas_ppl",
    "gas_ppl__cl_fresh",
    "gas_ppl__ot_fresh",
}


def test_wet_cooling_constraint_structure() -> None:
    wet_cf = pd.DataFrame(
        [[0.95, 0.90], [0.95, 0.90]],
        index=pd.Index(["AFR", "WEU"], name="region"),
        columns=[2050, 2060],
    )
    result = build_wet_cooling_constraints(
        _ADDON_DF, _TECHS, wet_cf, model_years=[2050, 2060]
    )

    assert "relation_activity" in result
    assert "relation_upper" in result
    assert "relation_names" in result

    rel_act = result["relation_activity"]
    rel_up = result["relation_upper"]

    assert not rel_act.empty
    assert not rel_up.empty
    # All upper bounds should be zero
    assert (rel_up["value"] == 0.0).all()


def test_wet_cooling_coefficient_signs() -> None:
    wet_cf = pd.DataFrame(
        [[0.95]],
        index=pd.Index(["AFR"], name="region"),
        columns=[2050],
    )
    result = build_wet_cooling_constraints(
        _ADDON_DF, _TECHS, wet_cf, model_years=[2050]
    )
    rel_act = result["relation_activity"]

    # Freshwater variants should have positive coefficients
    fresh = rel_act[rel_act["technology"].str.contains("fresh")]
    assert (fresh["value"] > 0).all()

    # Parent technologies should have negative coefficients
    parents = rel_act[~rel_act["technology"].str.contains("__")]
    assert (parents["value"] < 0).all()


def test_wet_cooling_min_year_filtering() -> None:
    wet_cf = pd.DataFrame(
        [[0.98, 0.95]],
        index=pd.Index(["AFR"], name="region"),
        columns=[2030, 2050],
    )
    result = build_wet_cooling_constraints(
        _ADDON_DF,
        _TECHS,
        wet_cf,
        model_years=[2030, 2050],
        min_year=2045,
    )
    rel_act = result["relation_activity"]
    # 2030 should be excluded
    assert 2030 not in rel_act["year_act"].values
    assert 2050 in rel_act["year_act"].values


_CF_AIR = pd.DataFrame(
    {
        "node_loc": ["R12_AFR", "R12_AFR", "R12_WEU"],
        "technology": ["coal_ppl__air", "coal_ppl__air", "gas_ppl__air"],
        "year_act": [2030, 2050, 2050],
        "value": [0.90, 0.80, 0.75],
        "unit": ["-", "-", "-"],
    }
)

_DRY_RATIOS = pd.DataFrame(
    {2050: [0.50, 0.80]},
    index=pd.Index(["AFR", "WEU"], name="region"),
)


def test_dry_cooling_scales_values_and_preserves_structure() -> None:
    old_cf, new_cf = build_dry_cooling_factors(
        _CF_AIR, _DRY_RATIOS, model_years=[2030, 2050], min_year=2045
    )

    assert old_cf["year_act"].tolist() == [2050, 2050]
    assert new_cf.columns.tolist() == old_cf.columns.tolist()
    np.testing.assert_allclose(old_cf["value"], [0.80, 0.75])
    np.testing.assert_allclose(new_cf["value"], [0.40, 0.60])


def test_dry_cooling_min_year_filtering() -> None:
    old_cf, new_cf = build_dry_cooling_factors(
        _CF_AIR, _DRY_RATIOS, model_years=[2030], min_year=2045
    )

    assert old_cf.empty
    assert new_cf.empty


def test_ratios_to_long_reshapes_and_prefixes_region() -> None:
    wide = pd.DataFrame(
        {2030: [0.9, 0.8], 2050: [0.7, 0.6]},
        index=pd.Index(["AFR", "WEU"], name="region"),
    )

    long = _ratios_to_long(wide, regions="R12")

    assert set(long.columns) == {"region", "year", "value"}
    assert set(long["region"]) == {"R12_AFR", "R12_WEU"}
    assert set(long["year"]) == {2030, 2050}
    assert len(long) == 4
