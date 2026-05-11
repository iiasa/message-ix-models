"""Climate impact prediction toolkit.

GMT-to-impact prediction via RIME emulators and year helpers.
Domain-specific transforms live with their domain modules; this package only
handles emulator lookup and ensemble reduction. RIME prediction is an adapted
reimplementation of `iiasa/rime
<https://github.com/iiasa/rime>`_; see :mod:`.rime` for attribution.
"""

from functools import partial

from message_ix_models.util import package_data_path

from .climate import (
    GmtArray,
    gmt_ensemble,
    gmt_expectation,
    load_magicc_gmt,
    persist_gmt_mean,
)
from .rime import check_emulator_linearity, clip_gmt, open_rime_dataset, predict_rime
from .temporal import sample_to_model_years

impacts_data_path = partial(package_data_path, "impacts")


__all__ = [
    "GmtArray",
    "check_emulator_linearity",
    "clip_gmt",
    "gmt_ensemble",
    "gmt_expectation",
    "impacts_data_path",
    "load_magicc_gmt",
    "open_rime_dataset",
    "persist_gmt_mean",
    "predict_rime",
    "sample_to_model_years",
]
