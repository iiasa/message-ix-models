Physical impacts
****************

.. contents::
   :local:

Architecture
============

Physical-impact support is split across three layers. Generic prediction
utilities live in :mod:`message_ix_models.tools.impacts`. They read MAGICC
GSAT trajectories, evaluate packaged RIME emulators, and return impact arrays
at the emulator's native resolution.

Domain application modules translate those predictions into MESSAGE scenario
edits. The current application modules are
:mod:`message_ix_models.model.buildings.impacts` for buildings demand and
:mod:`message_ix_models.model.water.data.cooling_impacts` for thermoelectric
cooling.

Project workflows, such as :mod:`message_ix_models.project.sparccle`, choose
starter scenarios, input files, variants, and clone names. They do not define
the domain-specific MESSAGE parameters used for an impact.

Buildings
=========

Buildings impacts replace the climate-sensitive part of aggregate
``rc_spec`` and ``rc_therm`` demand. The replacement demand is calibrated in
three layers.

The correction-coefficient files contain archetype-level ``gamma`` terms.
Together with RIME/CHILLED energy intensity and STURM floor area, these
reproduce STURM baseline energy demand:

.. math::

   \mathrm{raw}_{r,a,t} =
   \gamma_{r,a,t} \cdot EI_{r,a}(GSAT_t) \cdot F_{r,a,t}

where ``F`` is STURM floor area. In the packaged CSV files, ``gamma`` is the
``correction_coeff`` column.

The ``theta`` files then scale the node-year aggregate so the reference-GWL
result matches the calibrated buildings baseline for each SSP. The
``rc_sector_fractions`` files identify the residential and commercial
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
