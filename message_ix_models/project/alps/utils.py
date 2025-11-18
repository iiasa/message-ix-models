"""Utility functions for ALPS project.

Includes cached lognormal distribution fitting for emulator uncertainty propagation.
Adapted from rimeX.stats to add caching for ALPS-specific use cases.
"""

import numpy as np
import scipy.stats as stat
from functools import lru_cache


class VectorizedDist:
    """Vectorized distribution object for efficient sampling across multiple distributions.

    Stores parameters for N distributions and can sample all at once.
    """

    def __init__(self, mus, sigmas, locs, reverse, valid, mid_rev, mid_original):
        self.mus = mus
        self.sigmas = sigmas
        self.locs = locs
        self.reverse = reverse
        self.valid = valid
        self.mid_rev = mid_rev  # For fallback when invalid
        self.mid_original = mid_original

    def ppf(self, quantiles):
        """Sample quantiles from all distributions.

        Mimics ReverseDist.ppf behavior for reversed distributions:
        - Sample at (1-q) in reversed space
        - Negate result to return to original space

        Args:
            quantiles: Array of quantile levels (e.g., [0.05, 0.25, 0.50, 0.75, 0.95])

        Returns:
            Array of shape (len(quantiles), *self.mus.shape)
        """
        quantiles = np.asarray(quantiles)
        if quantiles.ndim == 0:
            quantiles = np.array([quantiles])

        # Broadcast quantiles to match distribution shape
        n_dims = len(self.mus.shape)
        for _ in range(n_dims):
            quantiles = quantiles[..., np.newaxis]

        # For reversed distributions: sample at (1-quantile) in reversed space
        quantile_to_sample = np.where(self.reverse, 1 - quantiles, quantiles)

        # Sample from lognormal (locs are in reversed space for reversed dists)
        sampled_in_lognorm_space = np.where(
            self.valid,
            stat.lognorm.ppf(
                quantile_to_sample, self.sigmas, loc=self.locs, scale=np.exp(self.mus)
            ),
            self.mid_rev,  # Fallback for invalid fits
        )

        # Negate samples for reversed distributions to return to original space
        sampled_values = np.where(
            self.reverse, -sampled_in_lognorm_space, sampled_in_lognorm_space
        )

        return sampled_values

    def ppf_per_year(self, quantiles):
        """Sample using different quantile for each (basin, year).

        Used for RIME-style shuffled sampling where emulator uncertainty is
        independent across time (breaks time-coherence).

        Args:
            quantiles: Array of shape (*self.mus.shape) with quantile per element

        Returns:
            Array of shape (*self.mus.shape) with samples
        """
        quantiles = np.asarray(quantiles)
        assert (
            quantiles.shape == self.mus.shape
        ), f"quantiles shape {quantiles.shape} must match distribution shape {self.mus.shape}"

        # For reversed distributions: sample at (1-quantile) in reversed space
        quantile_to_sample = np.where(self.reverse, 1 - quantiles, quantiles)

        # Sample from lognormal (locs are in reversed space for reversed dists)
        sampled_in_lognorm_space = np.where(
            self.valid,
            stat.lognorm.ppf(
                quantile_to_sample, self.sigmas, loc=self.locs, scale=np.exp(self.mus)
            ),
            self.mid_rev,  # Fallback for invalid fits
        )

        # Negate samples for reversed distributions to return to original space
        sampled_values = np.where(
            self.reverse, -sampled_in_lognorm_space, sampled_in_lognorm_space
        )

        return sampled_values


def _fit_dist_vectorized_impl(
    mid_bytes: bytes,
    lo_bytes: bytes,
    hi_bytes: bytes,
    shape: tuple,
    dtype_str: str,
) -> VectorizedDist:
    """Cached vectorized lognormal fitting implementation.

    Converts bytes back to arrays and fits lognormal distributions across all elements.
    Uses @lru_cache to avoid refitting identical distributions when GMT values overlap.

    Args:
        mid_bytes: Bytes representation of median percentile array
        lo_bytes: Bytes representation of lower percentile array
        hi_bytes: Bytes representation of upper percentile array
        shape: Original array shape
        dtype_str: String representation of dtype

    Returns:
        VectorizedDist object that can sample all distributions at once
    """
    dtype = np.dtype(dtype_str)
    mid = np.frombuffer(mid_bytes, dtype=dtype).reshape(shape)
    lo = np.frombuffer(lo_bytes, dtype=dtype).reshape(shape)
    hi = np.frombuffer(hi_bytes, dtype=dtype).reshape(shape)

    # Detect reversed distributions (left-skewed)
    reverse = (hi - mid) < (mid - lo)

    # Reverse where needed
    mid_rev = np.where(reverse, -mid, mid)
    lo_rev = np.where(reverse, -hi, lo)
    hi_rev = np.where(reverse, -lo, hi)

    # Compute location parameter (vectorized)
    locs = (mid_rev**2 - hi_rev * lo_rev) / (2 * mid_rev - lo_rev - hi_rev)

    # Check validity
    valid = (lo_rev - locs) > 0

    # Compute log-transformed values (where valid)
    log_mid = np.where(valid, np.log(np.maximum(mid_rev - locs, 1e-10)), 0.0)
    log_lo = np.where(valid, np.log(np.maximum(lo_rev - locs, 1e-10)), 0.0)
    log_hi = np.where(valid, np.log(np.maximum(hi_rev - locs, 1e-10)), 0.0)

    # Estimate parameters (vectorized)
    sigmas = ((log_hi - log_mid) + (log_mid - log_lo)) / 2 / stat.norm.ppf(0.95)
    mus = log_mid

    return VectorizedDist(mus, sigmas, locs, reverse, valid, mid_rev, mid)


# Apply lru_cache to the implementation
_fit_dist_vectorized_cached = lru_cache(maxsize=None)(_fit_dist_vectorized_impl)


def fit_dist(values, quantiles=None, dist_name="lognorm"):
    """Fit lognormal distribution to percentile data (vectorized, cached).

    Cached version of rimeX.stats.fit_dist for ALPS use case. When multiple runs
    have identical GMT values, the same distribution parameters are reused from cache.

    Args:
        values: List/tuple of [mid, lo, hi] percentiles as arrays
        quantiles: Expected [0.5, 0.05, 0.95] (for validation)
        dist_name: Must be 'lognorm' (only supported distribution)

    Returns:
        VectorizedDist object for sampling
    """
    if quantiles is not None:
        assert np.array(quantiles).tolist() == [
            0.5,
            0.05,
            0.95,
        ], f"Expected [0.5, 0.05, 0.95], got {quantiles}"

    if dist_name != "lognorm":
        raise NotImplementedError(
            f"fit_dist only supports 'lognorm', got '{dist_name}'"
        )

    mid, lo, hi = values

    # Validate inputs are arrays
    if not isinstance(mid, np.ndarray):
        raise TypeError(f"Expected numpy arrays, got {type(mid)}")

    # Convert arrays to bytes for caching
    shape = mid.shape
    dtype_str = str(mid.dtype)

    return _fit_dist_vectorized_cached(
        mid.tobytes(), lo.tobytes(), hi.tobytes(), shape, dtype_str
    )
