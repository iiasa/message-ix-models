SPARCCLE
********

"Socioeconomic Pathways, Adaptation and Resilience to Changing CLimate in Europe"

- Project lead: :gh-user:`byersiiasa`
- Lead modeler: :gh-user:`adrivinca`

Overview
========

The SPARCCLE workflow applies physical climate-impact damage drivers to
solved MESSAGE starter scenarios. It reads starter-specific MAGICC GSAT
ensembles, evaluates the packaged RIME impact emulators, and writes
buildings and cooling impacts to project-side scenario clones.

Prerequisites
=============

Each starter needs a MAGICC ``*_IAMC_climateassessment.xlsx`` file in the
directory configured by
:file:`message_ix_models/project/sparccle/scenario_config.yaml`. Buildings
impacts operate on calibrated ``rc_spec`` and ``rc_therm`` demand. Cooling
impacts require the water-cooling module; the workflow builds this prep clone
before applying cooling CIDs.

SSP coverage
============

Buildings calibration data ships for SSP1, SSP2, and SSP3. Cooling impacts
do not depend on SSP-specific buildings calibration.

Cooling data
============

Cooling impacts use
:file:`message_ix_models/data/impacts/rime/r12_thermoelectric_gwl.nc`. The
file is R12-coded and has two cooling categories: ``wet`` for freshwater
once-through and closed-loop cooling, and ``dry`` for air cooling. The CID is
applied as a ratio to the 1.0 °C GWL capacity factor.

Running the pipeline
====================

The CLI builds a workflow graph from
:file:`message_ix_models/project/sparccle/scenario_config.yaml` and runs
the requested target step:

.. code-block:: bash

   # Buildings only
   mix-models sparccle run "<MODEL>/<SCENARIO> CI_b" --go

   # Cooling only (wet + dry, on a cooling-built prep clone)
   mix-models sparccle run "<MODEL>/<SCENARIO> CI_p" --go

   # Combined buildings + cooling
   mix-models sparccle run "<MODEL>/<SCENARIO> CI_bp" --go

Each target writes a suffixed clone:

- ``<scenario>_CI_b`` — buildings demand replaced under
  ``Final Energy|Residential and Commercial|{Cooling,Heating}``.
- ``<scenario>_cooling_CI_p`` — wet-cooling activity bounds and dry-cooling
  capacity-factor derating applied; ratios persisted as
  ``Physical Climate Impact|Thermoelectric Cooling|*`` timeseries.
- ``<scenario>_cooling_CI_bp`` — both, in one workflow step.

See :doc:`/api/tools-impacts` for the underlying GMT-to-impact toolkit.
