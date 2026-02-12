"""Disk-cached prediction wrapper.

RIME predictions are expensive (~seconds per variable per ensemble).
``functools.cache`` / ``lru_cache`` provide in-memory caching; this module
adds persistence across sessions via ``diskcache.FanoutCache``.

Cache keys hash the GMT array itself (not MAGICC scenario names), since
parsing is decoupled from prediction.
"""

import hashlib
import logging
from pathlib import Path

import numpy as np

log = logging.getLogger(__name__)

_CACHE_DIR = Path(__file__).parent / ".cache" / "rime_predictions"
_cache = None  # Lazy-initialized


def _get_cache():
    """Lazy-initialize the diskcache FanoutCache."""
    global _cache
    if _cache is None:
        try:
            from diskcache import FanoutCache

            _CACHE_DIR.mkdir(parents=True, exist_ok=True)
            _cache = FanoutCache(str(_CACHE_DIR), shards=8)
        except ImportError:
            log.warning("diskcache not installed; caching disabled")
            _cache = False  # sentinel: tried and failed
    return _cache


def _make_key(
    gmt_array: np.ndarray,
    variable: str,
    temporal_res: str,
    percentile: str | None,
    hydro_model: str,
) -> str:
    """Deterministic cache key from GMT array content and parameters."""
    array_hash = hashlib.sha256(np.ascontiguousarray(gmt_array).tobytes()).hexdigest()[
        :16
    ]
    pct = percentile or "none"
    return f"{variable}_{temporal_res}_{hydro_model}_{pct}_{array_hash}"


def cached_prediction(
    gmt_array: np.ndarray,
    variable: str,
    temporal_res: str = "annual",
    percentile: str | None = None,
    sel: dict | None = None,
    hydro_model: str = "CWatM",
):
    """Disk-cached wrapper around :func:`~.rime.predict_rime`.

    Accepts and forwards all :func:`~.rime.predict_rime` parameters.
    The cache key incorporates ``variable``, ``temporal_res``,
    ``percentile``, ``hydro_model``, and the GMT array content.

    .. note::
        The ``sel`` parameter is **not** included in the cache key
        because dict hashing is unreliable. Calls with different ``sel``
        values but identical other parameters will collide. If you use
        ``sel``, prefer calling :func:`~.rime.predict_rime` directly.

    Parameters
    ----------
    gmt_array
        GMT values, shape ``(n_years,)`` or ``(n_runs, n_years)``.
    variable
        RIME variable name.
    temporal_res
        ``"annual"`` or ``"seasonal2step"``.
    percentile
        Uncertainty percentile suffix.
    sel
        Dimension selections (not cached — see note).
    hydro_model
        Hydrological model name.

    Returns
    -------
    np.ndarray or tuple
        Same as :func:`~.rime.predict_rime`.
    """
    cache = _get_cache()
    key = _make_key(gmt_array, variable, temporal_res, percentile, hydro_model)

    if cache and key in cache:
        log.debug(f"Cache hit: {variable} ({temporal_res})")
        return cache[key]

    log.info(f"Cache miss: {variable} ({temporal_res}) — computing predictions")

    from .rime import predict_rime

    result = predict_rime(
        gmt_array,
        variable,
        temporal_res,
        percentile=percentile,
        sel=sel,
        hydro_model=hydro_model,
    )

    if cache:
        cache[key] = result

    return result


def clear_cache():
    """Invalidate all cached predictions."""
    cache = _get_cache()
    if cache:
        cache.clear()
        log.info("Prediction cache cleared")
    else:
        log.info("No cache to clear")
