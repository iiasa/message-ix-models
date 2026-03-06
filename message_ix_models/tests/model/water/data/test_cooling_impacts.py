"""Tests for model.water.data.cooling_impacts -- Jones cooling constraints."""

from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from message_ix_models.tools.impacts import impacts_data_path

_RIME_DIR = impacts_data_path("rime")
_HAS_RIME_DATA = (_RIME_DIR / "r12_capacity_gwl_ensemble.nc").exists()

skip_no_rime = pytest.mark.skipif(
    not _HAS_RIME_DATA, reason="RIME cooling NetCDF not available"
)


@skip_no_rime
class TestPredictCoolingCf:
    def test_shape_and_range(self):
        from message_ix_models.model.water.data.cooling_impacts import (
            predict_cooling_cf,
        )

        gmt = np.linspace(1.0, 3.0, 8)
        result = predict_cooling_cf(gmt)
        assert result.shape == (12, 8)
        assert (result.values > 0).all()
        assert (result.values <= 1).all()

    def test_ensemble(self):
        from message_ix_models.model.water.data.cooling_impacts import (
            predict_cooling_cf,
        )

        rng = np.random.default_rng(42)
        gmt_2d = np.clip(rng.normal(2.0, 0.3, size=(5, 8)), 0.8, 6.0)
        result = predict_cooling_cf(gmt_2d)
        assert result.shape == (12, 8)

    def test_region_index(self):
        from message_ix_models.model.water.data.cooling_impacts import (
            predict_cooling_cf,
        )

        gmt = np.array([1.5, 2.0])
        result = predict_cooling_cf(gmt)
        assert result.index.name == "region"
        assert "AFR" in result.index


@skip_no_rime
class TestComputeJonesRatios:
    def test_ratio_at_baseline(self):
        from message_ix_models.model.water.data.cooling_impacts import (
            compute_jones_ratios,
        )

        gmt = np.array([1.0])
        ratios = compute_jones_ratios(gmt, baseline_gwl=1.0)
        np.testing.assert_allclose(ratios.values, 1.0, atol=0.01)

    def test_ratio_decreases_with_warming(self):
        from message_ix_models.model.water.data.cooling_impacts import (
            compute_jones_ratios,
        )

        gmt = np.array([1.0, 2.0, 3.0, 4.0])
        ratios = compute_jones_ratios(gmt, baseline_gwl=1.0)
        # CF decreases with warming -> ratios decrease monotonically
        for i in range(12):
            row = ratios.iloc[i].values
            assert row[0] >= row[-1], f"Region {ratios.index[i]}: ratio not decreasing"

    def test_ratio_shape_matches_input(self):
        from message_ix_models.model.water.data.cooling_impacts import (
            compute_jones_ratios,
        )

        gmt = np.linspace(1.0, 3.0, 6)
        ratios = compute_jones_ratios(gmt)
        assert ratios.shape == (12, 6)


@skip_no_rime
class TestFreshwaterReferenceShares:
    def test_loads_and_sums_to_reasonable_range(self):
        from message_ix_models.model.water.data.cooling_impacts import (
            _freshwater_reference_shares,
        )

        shares = _freshwater_reference_shares()
        assert len(shares) == 12
        # Freshwater share should be between 0 and 1 per region
        assert (shares >= 0).all()
        assert (shares <= 1).all()


class TestBuildCoolingConstraints:
    """Unit tests with mock data (no RIME data or DB needed)."""

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

    def test_constraint_structure(self):
        from message_ix_models.model.water.data.cooling_impacts import (
            build_cooling_constraints,
        )

        jones = pd.DataFrame(
            [[0.95, 0.90], [0.95, 0.90]],
            index=pd.Index(["AFR", "WEU"], name="region"),
            columns=[2050, 2060],
        )
        result = build_cooling_constraints(
            self._ADDON_DF, self._TECHS, jones, model_years=[2050, 2060]
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

    def test_coefficient_signs(self):
        from message_ix_models.model.water.data.cooling_impacts import (
            build_cooling_constraints,
        )

        jones = pd.DataFrame(
            [[0.95]],
            index=pd.Index(["AFR"], name="region"),
            columns=[2050],
        )
        result = build_cooling_constraints(
            self._ADDON_DF, self._TECHS, jones, model_years=[2050]
        )
        rel_act = result["relation_activity"]

        # Freshwater variants should have positive coefficients
        fresh = rel_act[rel_act["technology"].str.contains("fresh")]
        assert (fresh["value"] > 0).all()

        # Parent technologies should have negative coefficients
        parents = rel_act[~rel_act["technology"].str.contains("__")]
        assert (parents["value"] < 0).all()

    def test_min_year_filtering(self):
        from message_ix_models.model.water.data.cooling_impacts import (
            build_cooling_constraints,
        )

        jones = pd.DataFrame(
            [[0.98, 0.95]],
            index=pd.Index(["AFR"], name="region"),
            columns=[2030, 2050],
        )
        result = build_cooling_constraints(
            self._ADDON_DF,
            self._TECHS,
            jones,
            model_years=[2030, 2050],
            min_year=2045,
        )
        rel_act = result["relation_activity"]
        # 2030 should be excluded
        assert 2030 not in rel_act["year_act"].values
        assert 2050 in rel_act["year_act"].values

    def test_empty_when_no_qualifying_years(self):
        from message_ix_models.model.water.data.cooling_impacts import (
            build_cooling_constraints,
        )

        jones = pd.DataFrame(
            [[0.99]],
            index=pd.Index(["AFR"], name="region"),
            columns=[2030],
        )
        result = build_cooling_constraints(
            self._ADDON_DF,
            self._TECHS,
            jones,
            model_years=[2030],
            min_year=2045,
        )
        assert result["relation_activity"].empty
        assert result["relation_names"] == []

    def test_model_years_not_in_jones_skipped(self):
        """Years in model_years but absent from jones_ratios columns are skipped."""
        from message_ix_models.model.water.data.cooling_impacts import (
            build_cooling_constraints,
        )

        # jones only has 2050; model_years additionally requests 2060
        jones = pd.DataFrame(
            [[0.95], [0.95]],
            index=pd.Index(["AFR", "WEU"], name="region"),
            columns=[2050],
        )
        result = build_cooling_constraints(
            self._ADDON_DF, self._TECHS, jones, model_years=[2050, 2060]
        )
        rel_act = result["relation_activity"]
        assert 2060 not in rel_act["year_act"].values
        assert 2050 in rel_act["year_act"].values


class TestPredictCoolingCfClipBounds:
    """clip_gmt must use annual bounds (0.6, 0.9), not seasonal (0.8, 1.2)."""

    def test_annual_clip_bounds_used(self):
        from message_ix_models.model.water.data.cooling_impacts import (
            predict_cooling_cf,
        )

        gmt = np.array([1.0, 2.0])
        fake_raw = np.ones((12, 2))

        with (
            patch(
                "message_ix_models.model.water.data.cooling_impacts.clip_gmt",
                return_value=gmt,
            ) as mock_clip,
            patch(
                "message_ix_models.model.water.data.cooling_impacts.predict_rime",
                return_value=fake_raw,
            ),
            patch(
                "message_ix_models.model.water.data.cooling_impacts._region_codes",
                return_value=[f"R{i}" for i in range(12)],
            ),
        ):
            predict_cooling_cf(gmt)

        mock_clip.assert_called_once_with(gmt, gmt_min=0.6, gmt_ceil=0.9)
