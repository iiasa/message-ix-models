.. currentmodule:: message_ix_models.tools.impacts

Climate impact prediction toolkit (:mod:`.tools.impacts`)
**********************************************************

.. contents::
   :local:

Overview
========

:mod:`.tools.impacts` provides GMT-to-impact prediction via RIME regional
emulators, with helpers for assembling GMT ensembles from MAGICC output and
resampling sparse calibration inputs onto MESSAGE model years.

The toolkit is domain-agnostic: it predicts at native emulator resolution and
returns NumPy arrays. Domain modules under :mod:`.model.buildings`,
:mod:`.model.water.data`, and :mod:`.project.sparrcle` own the transformation
to MESSAGE-compatible parameters.

RIME prediction is an adapted reimplementation of the GWL-binned
nearest-neighbor lookup from Byers et al. (2025), *Environ. Res.: Climate*
**4** 035011, `doi:10.1088/2752-5295/adee3d
<https://doi.org/10.1088/2752-5295/adee3d>`_;
upstream code at `iiasa/rime <https://github.com/iiasa/rime>`_ (GPL-3.0).
See :mod:`.tools.impacts.rime` for full attribution.

Usage
=====

.. code-block:: python

   import numpy as np

   from message_ix_models.tools.impacts import (
       GmtArray,
       gmt_ensemble,
       gmt_expectation,
       load_magicc_gmt,
       predict_rime,
   )

   # Read a MAGICC climate-assessment Excel run-ensemble
   gmt = load_magicc_gmt("/path/to/magicc_output_dir", n_runs=600)
   #   gmt.values: (n_runs, n_years); gmt.years: (n_years,)

   # Single-trajectory prediction (ensemble-mean GMT)
   mean = gmt_expectation(gmt)               # values shape (n_years,)
   qtot_1d = predict_rime(
       mean.values,
       "rime_regionarray_qtot_mean_CWatM_annual_window11.nc",
       "qtot_mean",
   )                                          # (157, n_years)

   # Ensemble prediction with mean reduction
   qtot_2d = predict_rime(
       gmt.values,
       "rime_regionarray_qtot_mean_CWatM_annual_window11.nc",
       "qtot_mean",
       aggregate="mean",
   )                                          # (157, n_years)

RIME emulators (:mod:`.tools.impacts.rime`)
===========================================

RIME datasets use GWL-binned nearest-neighbor lookup. Each emulator covers a
GMT range of 0.6–7.4 °C (ISIMIP3b-derived). Values outside this range return
NaN; values below the minimum are clipped with skewed Beta(2, 5) noise via
:func:`.clip_gmt`.

For ensemble input ``(n_runs, n_years)``, :func:`.predict_rime` returns a
Monte Carlo estimate of ``E_{P(GMT)}[f(GMT)]`` when called with
``aggregate="mean"``. This is meaningful only when the emulator response is
approximately linear over the GMT range present in the ensemble; for
single-run input on a non-linear emulator it raises. Use
:func:`.check_emulator_linearity` to probe a dataset before relying on
ensemble means. :func:`.open_rime_dataset` opens a packaged NetCDF by
filename and caches the result.

Climate inputs (:mod:`.tools.impacts.climate`)
===============================================

:func:`.load_magicc_gmt` reads a ``*_IAMC_climateassessment.xlsx`` file and
returns a :class:`.GmtArray` of per-run GSAT trajectories.
:func:`.gmt_ensemble` builds a :class:`.GmtArray` from any wide DataFrame with
year columns. :func:`.gmt_expectation` reduces an ensemble to its per-year
``nanmean``. :func:`.persist_gmt_mean` writes the ensemble-mean trajectory
onto a scenario as a ``Physical Climate Impact|Surface Temperature (GSAT)``
timeseries.

Temporal resampling (:mod:`.tools.impacts.temporal`)
=====================================================

:func:`.sample_to_model_years` maps annual or sparse calibration inputs to
MESSAGE model years via ``method="point"`` (lookup), ``"average"`` (period
mean), or ``"interpolate"`` (linear infill between sparse anchors with
forward-fill beyond the last input).

Code reference
==============

.. currentmodule:: message_ix_models.tools

.. autosummary::
   :toctree: _autosummary
   :template: autosummary-module.rst
   :recursive:

   impacts
