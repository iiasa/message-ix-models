.. currentmodule:: message_ix_models.tools.impacts

Climate impact prediction toolkit (:mod:`.tools.impacts`)
**********************************************************

.. contents::
   :local:

Overview
========

:mod:`.tools.impacts` provides GMT-to-impact prediction via RIME regional
emulators, ensemble risk metrics (CVaR), and supporting utilities for GMT
array construction and model-year resampling.

The toolkit is domain-agnostic: it predicts at native emulator resolution and
returns NumPy arrays. Domain modules (water, cooling, buildings) own the
transformation to MESSAGE-compatible parameters.

RIME prediction is an adapted reimplementation of the GWL-binned
nearest-neighbor lookup from Werning et al. (2024),
`https://github.com/iiasa/rime <https://github.com/iiasa/rime>`_ (GPL-3.0).
See :mod:`.tools.impacts.rime` for full attribution.

Usage
=====

.. code-block:: python

   from message_ix_models.tools.impacts import (
       impacts_data_path,
       predict_rime,
       clip_gmt,
       gmt_ensemble,
       GmtArray,
   )

   # Locate a RIME NetCDF dataset shipped with message-ix-models
   ds = "rime_regionarray_qtot_mean_CWatM_annual_window11.nc"
   path = impacts_data_path("rime", ds)

   # Single GMT trajectory: shape (n_years,)
   gmt_1d = np.linspace(1.0, 2.5, 10)
   qtot = predict_rime(gmt_1d, path, "qtot_mean")  # (157, 10)

   # Ensemble: shape (n_runs, n_years) -> expectation E[f(GMT)]
   gmt_2d = gmt_ensemble(magicc_df, model_years=[2025, 2030, ..., 2100])
   qtot_mean = predict_rime(gmt_2d, path, "qtot_mean")  # (157, n_years)

   # Full ensemble for CVaR
   from message_ix_models.tools.impacts import cvar_coherent
   worst = cvar_coherent(gmt_2d, path, "qtot_mean", alpha=10)  # (157, n_years)

RIME emulators (:mod:`.tools.impacts.rime`)
===========================================

RIME datasets use GWL-binned nearest-neighbor lookup.
Each emulator covers a GMT range of 0.6–7.4 °C (ISIMIP3b-derived).
Values outside this range return NaN; values below the minimum are clipped
with skewed Beta(2,5) noise via :func:`.clip_gmt`.

For ensemble input ``(n_runs, n_years)``, :func:`.predict_rime` computes a
Monte Carlo estimate of ``E_{P(GMT)}[f(GMT)]`` — meaningful only when the
emulator response is approximately linear. Use
:func:`.check_emulator_linearity` to verify before interpreting ensemble means.

Risk metrics (:mod:`.tools.impacts.risk`)
=========================================

Both CVaR functions call :func:`.predict_rime` with ``aggregate="none"``
to obtain the full ``(n_runs, n_spatial, n_years)`` ensemble, then reduce
along the run axis.

- :func:`.cvar_pointwise` — independent at each (spatial, year) cell.
  Maximally pessimistic: can compound worst-case across timesteps.
- :func:`.cvar_coherent` — selects worst alpha% of full trajectories.
  Temporally coherent: represents persistently unlucky but realizable paths.

Climate inputs (:mod:`.tools.impacts.climate`)
===============================================

:func:`.gmt_ensemble` constructs a ``(n_runs, n_years)`` array from a MAGICC
ensemble DataFrame. :func:`.gmt_expectation` collapses to a 1D trajectory
for single-trajectory prediction.

Temporal resampling (:mod:`.tools.impacts.temporal`)
=====================================================

:func:`.sample_to_model_years` maps MAGICC output years (annual, 2010–2100)
to MESSAGE model years using either point lookup or period averaging.

Code reference
==============

.. currentmodule:: message_ix_models.tools

.. autosummary::
   :toctree: _autosummary
   :template: autosummary-module.rst
   :recursive:

   impacts
