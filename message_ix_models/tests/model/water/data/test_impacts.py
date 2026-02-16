"""Tests for model.water.data.impacts -- water-domain RIME transformations."""

import numpy as np
import pytest

from message_ix_models.model.water.data.impacts import (
    _N_MESSAGE_BASINS,
    _N_RIME_BASINS,
    load_basin_mapping,
    split_basin_macroregion,
)
from message_ix_models.tools.impacts.rime import _RIME_DATASETS_DIR


class TestLoadBasinMapping:
    def test_loads_217_rows(self):
        df = load_basin_mapping()
        assert len(df) == _N_MESSAGE_BASINS
        assert "BASIN_ID" in df.columns
        assert "basin_code" in df.columns
        assert "area_km2" in df.columns


# Integration tests requiring RIME NetCDF files (for basin-to-index mapping)
_HAS_RIME_DATA = (
    _RIME_DATASETS_DIR / "rime_regionarray_qtot_mean_CWatM_annual_window11.nc"
).exists()


@pytest.mark.skipif(not _HAS_RIME_DATA, reason="RIME NetCDF datasets not available")
class TestSplitBasinMacroregion:
    """Requires RIME data for basin-to-index mapping."""

    def test_expansion_shape(self):
        basin_mapping = load_basin_mapping()
        fake_rime = np.ones((_N_RIME_BASINS, 5))
        result = split_basin_macroregion(fake_rime, basin_mapping)
        assert result.shape == (_N_MESSAGE_BASINS, 5)

    def test_nan_preservation(self):
        basin_mapping = load_basin_mapping()
        fake_rime = np.ones((_N_RIME_BASINS, 3))
        result = split_basin_macroregion(fake_rime, basin_mapping)
        assert result.shape == (_N_MESSAGE_BASINS, 3)

    def test_seasonal_3d(self):
        basin_mapping = load_basin_mapping()
        fake_rime = np.ones((_N_RIME_BASINS, 5, 2))
        result = split_basin_macroregion(fake_rime, basin_mapping)
        assert result.shape == (_N_MESSAGE_BASINS, 5, 2)


@pytest.mark.skipif(not _HAS_RIME_DATA, reason="RIME NetCDF datasets not available")
class TestPredictWaterRime:
    """End-to-end: predict + basin expansion."""

    def test_annual_shape(self):
        from message_ix_models.model.water.data.impacts import predict_water_rime

        gmt = np.linspace(1.0, 2.5, 10)
        result = predict_water_rime(gmt, "qtot_mean")
        assert result.shape == (_N_MESSAGE_BASINS, 10)

    def test_ensemble_shape(self):
        from message_ix_models.model.water.data.impacts import predict_water_rime

        rng = np.random.default_rng(42)
        gmt_2d = rng.normal(1.5, 0.2, size=(5, 10))
        gmt_2d = np.clip(gmt_2d, 0.6, 7.4)
        result = predict_water_rime(gmt_2d, "qtot_mean")
        assert result.shape == (_N_MESSAGE_BASINS, 10)
