"""Climate impact prediction toolkit.

Provides GMT-to-impact prediction via RIME emulators, ensemble risk metrics
(CVaR), and supporting utilities for GMT input parsing, caching, and year
resampling.

Domain-specific transformation formulas (water GW share, Jones cooling
ratios, building energy decomposition) are **not** part of this toolkit —
they belong with their respective domain modules. Callers consume
predictions and apply domain logic.

Usage::

    from message_ix_models.tools.impacts import (
        predict_rime,
        load_gmt,
        compute_cvar,
        cached_prediction,
        sample_to_model_years,
    )

    # Load GMT from MAGICC output
    gmt_2d, years = load_gmt("magicc_all_runs.xlsx", n_runs=100)

    # Predict water availability
    qtot = predict_rime(gmt_2d, "qtot_mean")  # (217, n_years)

    # Or with caching for repeated calls
    qtot = cached_prediction(gmt_2d, "qtot_mean")
"""

from .cache import cached_prediction, clear_cache
from .climate import load_gmt, load_magicc_ensemble, load_magicc_percentiles
from .rime import (
    check_emulator_linearity,
    load_basin_mapping,
    predict_rime,
    split_basin_macroregion,
)
from .risk import compute_cvar, compute_cvar_single, validate_cvar_monotonicity
from .temporal import extract_region_code, sample_to_model_years

__all__ = [
    "cached_prediction",
    "check_emulator_linearity",
    "clear_cache",
    "compute_cvar",
    "compute_cvar_single",
    "extract_region_code",
    "load_basin_mapping",
    "load_gmt",
    "load_magicc_ensemble",
    "load_magicc_percentiles",
    "predict_rime",
    "sample_to_model_years",
    "split_basin_macroregion",
    "validate_cvar_monotonicity",
]
