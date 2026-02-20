"""Climate impact prediction toolkit.

Provides GMT-to-impact prediction via RIME emulators, ensemble risk metrics
(CVaR), and supporting utilities for GMT array extraction and year
resampling.

Domain-specific transformation formulas (water GW share, Jones cooling
ratios, building energy decomposition) are **not** part of this toolkit —
they belong with their respective domain modules. Callers consume
predictions and apply domain logic.

RIME prediction (``rime`` submodule) is an adapted reimplementation of
Werning et al. (2024), https://github.com/iiasa/rime. See ``rime.py``
docstring for full attribution.

Usage::

    from message_ix_models.tools.impacts import (
        impacts_data_path,
        predict_rime,
        clip_gmt,
        gmt_ensemble,
        GmtArray,
    )

    ds = "rime_regionarray_qtot_mean_CWatM_annual_window11.nc"
    path = impacts_data_path("rime", ds)
    qtot = predict_rime(gmt_1d, path, "qtot_mean")
"""

from functools import partial

from message_ix_models.util import package_data_path

from .climate import GmtArray, gmt_ensemble, gmt_expectation
from .rime import check_emulator_linearity, clip_gmt, predict_rime
from .risk import cvar_coherent, cvar_pointwise
from .temporal import sample_to_model_years

impacts_data_path = partial(package_data_path, "impacts")


__all__ = [
    "GmtArray",
    "check_emulator_linearity",
    "clip_gmt",
    "cvar_coherent",
    "cvar_pointwise",
    "gmt_ensemble",
    "gmt_expectation",
    "impacts_data_path",
    "predict_rime",
    "sample_to_model_years",
]
