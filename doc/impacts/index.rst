Physical impacts
****************

.. contents::
   :local:

Architecture
============

Physical-impact support is split across three layers. Generic prediction
utilities live in :mod:`message_ix_models.tools.impacts`. They evaluate
packaged RIME emulators and return impact arrays at the emulator's native
resolution. The current file reader ingests MAGICC ensembles in the
``climate-assessment`` workbook format; the downstream prediction utilities
only require an ensemble realization of global mean temperature and can be
fed by other simple climate models through :class:`~message_ix_models.tools.impacts.GmtArray`.

Domain application modules translate those predictions into MESSAGE scenario
edits. The current application modules are
:mod:`message_ix_models.model.buildings.impacts` for buildings demand and
:mod:`message_ix_models.model.water.data.cooling_impacts` for thermoelectric
cooling. Basin-level water transformations live in
:mod:`message_ix_models.model.water.data.impacts`.

Project workflows, such as :mod:`message_ix_models.project.sparccle`, choose
starter scenarios, input files, variants, and clone names. They do not define
the domain-specific MESSAGE parameters used for an impact.

Buildings
=========

Buildings impacts replace the climate-sensitive part of aggregate
``rc_spec`` and ``rc_therm`` demand. The replacement demand is calibrated in
three layers.

The correction-coefficient files contain archetype-level ``gamma`` terms.
STURM encodes stock turnover, access, cooling-area, and other internal
dynamics. RIME/CHILLED provides climate-responsive energy intensity. Energy
intensity multiplied by floor area does not by itself reproduce the STURM
time series, so ``gamma`` is constructed so the reference-temperature product
matches STURM baseline energy demand:

.. math::

   \mathrm{raw}_{r,a,t} =
   \gamma_{r,a,t} \cdot EI_{r,a}(GSAT_t) \cdot F_{r,a,t}

where ``F`` is STURM floor area. In the packaged CSV files, ``gamma`` is the
``correction_coeff`` column.

The ``theta`` files are a second calibration layer. The raw STURM trajectory
is not the same object as the calibrated residential/commercial demand that
enters MESSAGE. For each SSP, ``theta`` scales the node-year aggregate so the
reference-GWL result matches the calibrated buildings heating and cooling
demand from the Shared Socioeconomic Pathways 2023 / WP-RC demand workflow.

The ``rc_sector_fractions`` files identify the residential and commercial
buildings components already present in aggregate ``rc_spec`` and
``rc_therm`` so they can be removed before the replacement demand is added.

Thermoelectric cooling
======================

Thermoelectric cooling impacts use
:file:`message_ix_models/data/impacts/rime/r12_thermoelectric_gwl.nc`. The
file is R12-coded and has two cooling categories: ``wet`` for freshwater
once-through and closed-loop cooling, and ``dry`` for air cooling. Ratios are
computed relative to the 1.0 °C GWL capacity factor.

Wet and dry cooling use different MESSAGE-side levers. Wet cooling limits
freshwater-cooled activity, so the application writes relation bounds on
freshwater once-through and closed-loop cooling technologies. Dry cooling
reduces air-cooled plant performance, so the application derates
``capacity_factor`` rows for ``__air`` technologies. Saline cooling is not
represented in the packaged RIME cooling dataset and is left unchanged.

The wet-cooling relation is constructed so that, given the regional freshwater
share, total freshwater-cooled activity from a parent power technology cannot
exceed the warming-impaired capacity factor times that parent's activity.
Dry cooling is represented directly as a multiplicative derating of air-cooled
capacity factors.

The impact kernels are based on:

- Wet cooling: Li et al. (2025), "Global hydroclimatic risks and strategic
  decommissioning pathways for thermal power units." *Nature Sustainability*.
  doi:10.1038/s41893-025-01692-9
- Dry cooling: Qin et al. (2023), "Global assessment of the carbon-water
  tradeoff of dry cooling for thermal power generation." *Nature Water*.
  doi:10.1038/s44221-023-00120-6

Water availability
==================

Basin-level water impacts use RIME datasets at native 157-basin resolution.
The packaged water datasets store GWL-indexed conditional summaries for
``qtot_mean`` and ``qr`` from CWatM ISIMIP3b realizations. The stored
variables are the expected value plus selected spread summaries
(``std``, ``p10``, ``p50``, and ``p90``). The source realizations span five GCMs
(``gfdl-esm4``, ``ipsl-cm6a-lr``, ``mpi-esm1-2-hr``, ``mri-esm2-0``,
``ukesm1-0-ll``) and three SSP-RCP climate scenarios (``ssp126``,
``ssp370``, ``ssp585``). The future CWatM runs use
``2015soc-from-histsoc`` direct human forcing, so runs differ by climate
forcing rather than by changing socioeconomic water-use assumptions. The
RIME datasets are binned with an 11-year centered time window.

The water application expands those predictions to the MESSAGE basin-region
rows used by the water module, including transboundary basin splits. This is
also a domain application layer: :mod:`message_ix_models.tools.impacts`
predicts emulator values, while :mod:`message_ix_models.model.water.data.impacts`
owns the basin mapping and water-specific output shapes.

The current transform preserves the RIME prediction values while changing the
spatial index from native emulator basins to MESSAGE basin-region rows. Future
water-CID application work is expected to add the MESSAGE-side hydrology
semantics on top of that mapping, including groundwater-share construction,
km3-to-MCM conversion, and the sign convention needed when water availability
is represented through MESSAGE demand-like parameters.
