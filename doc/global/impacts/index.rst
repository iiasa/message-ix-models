Physical impacts
****************

.. contents::
   :local:

Requirements
============

Reading the packaged RIME datasets requires :mod:`netCDF4`.
Install it with the ``impacts`` extra: :program:`pip install message-ix-models[impacts]`.
See :doc:`/install`.

Architecture
============

Physical-impact support is split across three layers.
Generic prediction utilities live in :mod:`message_ix_models.tools.impacts`.
They evaluate packaged RIME emulators and return impact arrays at the emulator's native resolution.
The current file reader ingests MAGICC ensembles in the ``climate-assessment`` workbook format; the downstream prediction utilities only require an ensemble realization of global mean temperature and can be fed by other simple climate models through :class:`~message_ix_models.tools.impacts.GmtArray`.

Domain application modules translate those predictions into MESSAGE scenario edits.
The current application modules are :mod:`message_ix_models.model.buildings.impacts` and :mod:`message_ix_models.model.buildings.space_split` for buildings demand, and :mod:`message_ix_models.model.water.data.cooling_impacts` for thermoelectric cooling.

Project workflows, such as :mod:`message_ix_models.project.sparccle`, choose starter scenarios, input files, variants, and clone names.
They do not define the domain-specific MESSAGE parameters used for an impact.

Buildings
=========

Buildings impacts act on the climate-sensitive part of the aggregate ``rc_spec`` and ``rc_therm`` demand.
``rc_spec`` and ``rc_therm`` are MACRO sectors, so a climate-driven increment written there would be traded away by MACRO against energy prices, which is wrong for a physical impact.
The buildings step therefore works in two stages.

First, :func:`~message_ix_models.model.buildings.space_split.apply_space_demand_split` splits space conditioning out of ``rc_spec`` and ``rc_therm`` onto two new demand commodities, ``space_cooling`` and ``space_heating``.
These are not MACRO sectors, so the MESSAGE-MACRO iteration never sees them.
The split adds one free identity bridge technology per new commodity, so that total supply capability is unchanged, and rebases ``aeei`` so that MACRO's target level for each parent sector falls to the non-space residual.
``rc_spec`` and ``rc_therm`` keep that residual and remain elastic.
See the module docstring of :mod:`message_ix_models.model.buildings.space_split` for the full description.

Second, :func:`~message_ix_models.model.buildings.space_split.apply_space_cids` writes the RIME-driven buildings impact onto ``space_cooling`` and ``space_heating``, so MACRO cannot trade the climate increment away.

The replacement demand is calibrated in three layers.

The correction-coefficient files contain archetype-level ``gamma`` terms.
STURM encodes stock turnover, access, cooling-area, and other internal dynamics.
RIME/CHILLED provides climate-responsive energy intensity.
Energy intensity multiplied by floor area does not by itself reproduce the STURM time series, so ``gamma`` is constructed so the reference-temperature product matches STURM baseline energy demand:

.. math::

   \mathrm{raw}_{r,a,t} =
   \gamma_{r,a,t} \cdot EI_{r,a}(GSAT_t) \cdot F_{r,a,t}

where ``F`` is STURM floor area.
In the packaged CSV files, ``gamma`` is the ``correction_coeff`` column.

The ``theta`` files are a second calibration layer.
The raw STURM trajectory is not the same object as the calibrated residential/commercial demand that enters MESSAGE.
For each SSP, ``theta`` scales the node-year aggregate so the reference-GWL result matches the calibrated buildings heating and cooling demand from the Shared Socioeconomic Pathways 2023 / WP-RC demand workflow.

The ``rc_sector_fractions`` files give the residential and commercial space-conditioning share of aggregate ``rc_spec`` and ``rc_therm``, which sets how much demand the split carves out onto ``space_cooling`` and ``space_heating``.

.. _impacts-ssp-coverage:

SSP coverage
------------

Packaged SSP coverage differs across the calibration layers.
The files are in :file:`message_ix_models/data/buildings/`.

- ``theta`` (:file:`theta_cool_SSP{1,2,3}.csv`, :file:`theta_heat_SSP{1,2,3}.csv`) and ``rc_sector_fractions`` (:file:`rc_sector_fractions_SSP{1,2,3}.csv`) are SSP-specific and ship for SSP1, SSP2, and SSP3.
- ``gamma`` (the :file:`correction_coefficients/` files) and the STURM floor-area projections (:file:`sturm_floor_area_R12_resid.csv`, :file:`sturm_floor_area_R12_comm.csv`) are SSP2-only, pending additional STURM runs.

Buildings impacts under SSP1 or SSP3 therefore use SSP-specific ``theta`` and ``rc_sector_fractions`` on top of an SSP2 STURM baseline and SSP2 ``gamma``.
Cooling impacts do not depend on the buildings calibration.

Thermoelectric cooling
======================

Thermoelectric cooling impacts use :file:`message_ix_models/data/impacts/rime/r12_thermoelectric_gwl.nc`.
The file is R12-coded and has two cooling categories: ``wet`` for freshwater once-through and closed-loop cooling, and ``dry`` for air cooling.
Ratios are computed relative to the 1.0 °C GWL capacity factor.

Wet and dry cooling use different MESSAGE-side levers.
Wet cooling limits freshwater-cooled activity, so the application writes relation bounds on freshwater once-through and closed-loop cooling technologies.
Dry cooling reduces air-cooled plant performance, so the application derates ``capacity_factor`` rows for ``__air`` technologies.
Saline cooling is not represented in the packaged RIME cooling dataset and is left unchanged.

The wet-cooling relation is constructed so that, given the regional freshwater share, total freshwater-cooled activity from a parent power technology cannot exceed the warming-impaired capacity factor times that parent's activity.
Dry cooling is represented directly as a multiplicative derating of air-cooled capacity factors.

The impact kernels are based on:

- Wet cooling: Li et al. (2025), "Global hydroclimatic risks and strategic decommissioning pathways for thermal power units." *Nature Sustainability*.
  doi:10.1038/s41893-025-01692-9
- Dry cooling: Qin et al. (2023), "Global assessment of the carbon-water tradeoff of dry cooling for thermal power generation." *Nature Water*.
  doi:10.1038/s44221-023-00120-6
