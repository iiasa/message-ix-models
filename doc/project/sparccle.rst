SPARCCLE
********

“Socioeconomic Pathways, Adaptation and Resilience to Changing CLimate in Europe”

- Project lead: :gh-user:`byersiiasa`
- Lead modeler: :gh-user:`adrivinca`

Overview
========

The SPARCCLE protocol combines socioeconomic starter scenarios with climate-impact variants and project-specific clone naming.

Economic impacts
================

Economic and GDP-based impacts are part of the broader SPARCCLE protocol.
They are not implemented by the physical-impact workflow described below.

Physical climate impacts
========================

The physical-impact workflow applies climate-impact damage drivers to solved MESSAGE starter scenarios.
It reads starter-specific GSAT ensembles, evaluates the packaged RIME impact emulators, and writes buildings and cooling impacts to project-side scenario clones.

For buildings, the workflow first splits space conditioning out of the MACRO sectors ``rc_spec`` and ``rc_therm`` onto the non-MACRO demands ``space_cooling`` and ``space_heating``, then writes the impact onto those demands.
See :doc:`/global/impacts/index` for how this works.

Prerequisites
=============

Reading the packaged RIME datasets requires :mod:`netCDF4`, which is provided by the ``impacts`` extra: :program:`pip install message-ix-models[impacts]`.

Each starter needs a MAGICC ``*_IAMC_climateassessment.xlsx`` file in the directory configured by :file:`message_ix_models/project/sparccle/scenario_config.yaml`.
The default paths are on the IIASA P drive (:file:`/pdrive/...`).
:program:`mix-models sparccle run` validates its inputs (MAGICC files, RIME data, buildings calibration) while generating the workflow, so even a dry run without ``--go`` fails without access to that drive, unless ``--magicc-root`` or ``--magicc-file`` point elsewhere.

Buildings impacts operate on calibrated ``rc_spec`` and ``rc_therm`` demand.
Cooling impacts require the water-cooling module; the workflow builds this prep clone before applying cooling CIDs.

Phase 1 calls :program:`mix-models water-ix cooling` as a subprocess, and that command also solves the ``_cooling`` clone, a solve the downstream clones discard.
This is a known limitation: an in-process build deadlocks on a locked scenario, so the subprocess stays until that is resolved.

SSP coverage
============

The calibration layers are not all SSP-specific.
See :ref:`impacts-ssp-coverage` for which buildings files ship for which SSPs.

Running the pipeline
====================

The CLI builds a workflow graph from :file:`message_ix_models/project/sparccle/scenario_config.yaml` and runs the requested target step:

.. code-block:: bash

   # Buildings only
   mix-models sparccle run "<MODEL>/<SCENARIO> CI_b" --go

   # Cooling only (wet + dry, on a cooling-built prep clone)
   mix-models sparccle run "<MODEL>/<SCENARIO> CI_p" --go

   # Combined buildings + cooling
   mix-models sparccle run "<MODEL>/<SCENARIO> CI_bp" --go

Each target writes a suffixed clone:

- ``<scenario>_CI_b`` — buildings demand written onto ``space_cooling`` and ``space_heating``, with ``Final Energy|Residential and Commercial|{Cooling,Heating}`` timeseries.
- ``<scenario>_cooling_CI_p`` — wet-cooling activity bounds and dry-cooling capacity-factor derating applied; ratios persisted as ``Physical Climate Impact|Thermoelectric Cooling|*`` timeseries.
- ``<scenario>_cooling_CI_bp`` — both, in one workflow step.

MAGICC location flags
=====================

The per-starter ``magicc_output_dir`` in :file:`scenario_config.yaml` can be overridden at invocation:

- ``--magicc-root <path>`` composes ``<root>/<model><suffix>/<scenario without _PHY>/`` per starter.
- ``--magicc-model-suffix <str>`` appends to the model component when composing under ``--magicc-root`` (e.g. ``_p95`` for the p95 envelope tree).
- ``--magicc-file <path>`` is a single-xlsx override for one-starter test runs.

Precedence: ``--magicc-file`` > ``--magicc-root`` > YAML.
This lets one packaged YAML serve multiple delivery variants whose only difference is the MAGICC tree location.

See :doc:`/global/impacts/index` for the impact application modules and :doc:`/api/tools-impacts` for the underlying GMT-to-impact toolkit.
