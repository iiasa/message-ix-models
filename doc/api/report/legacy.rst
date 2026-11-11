.. currentmodule:: message_ix_models.report.legacy
.. automodule:: message_ix_models.report.legacy

.. _report-legacy:

‘Legacy’ reporting (:mod:`.report.legacy`)
******************************************

:mod:`.report.legacy` contains ‘legacy’ reporting code transferred from :doc:`message_data <m-data:reference/tools/post_processing>` (:external+m-data:mod:`message_data.tools.post_processing.iamc_report_hackathon`).

This code:

- is only tested to *run* against :doc:`the global model snapshot </api/model-snapshot>`, specifically snapshot ID :py:`1`.
  This implies that the *outputs*—specific numerical values, labels, etc.—of the code are not tested or validated in any way.
  Users of the code should carefully validate output data, especially when the code is run against any scenario other than snapshot :py:`1`.
- is provided primarily for reference by users of snapshot :py:`1`.
- is not currently used in IIASA ECE program workflows or publications.
  As of 2024-04-04, all MESSAGEix-GLOBIOM outputs that appear in public and closed databases, associated with peer-reviewed works, etc. have been produced with other code from various branches of :mod:`message_data`.
- predates :mod:`genno` and the stack of tools built on it (:ref:`described here <report-intro>`); these were designed to avoid issues with performance and extensibility in the older code. [1]_
  It is intended that :mod:`.report` will eventually replace :mod:`.report.legacy`, which in the meantime serves as a reference for that code (such as :mod:`.report.compat`).

.. [1] See a (non-public) `“Reporting” project board <https://github.com/orgs/iiasa/projects/3>`_ on GitHub for details of the initial implementation of these features.

Usage
-----

Set :attr:`.report.Config.legacy` to include :py:`use=True` and any other keyword arguments to :func:`.iamc_report_hackathon.report`, then call :func:`message_ix_models.report.report`:

.. code-block:: python

   from message_ix_models import Context
   from message_ix_models.report import Config, report

   # Configure to use .report.legacy
   context = Context.get_instance()
   context.report.legacy.update(
       use=True,
       # Only this exact set of keyword arguments is
       # tested and known to work:
       merge_hist=True,
       ref_sol="True",
       run_config="ENGAGE_SSP2_v417_run_config.yaml",
   )

   # Invoke
   report(context)

Or, call :func:`.iamc_report_hackathon.report` directly.

From the command line, give :program:`mix-models report` the option :program:`--legacy` (:program:`-L`), or :program:`--legacy-config` with the path or stem of a YAML file whose keys and values update :attr:`.report.Config.legacy`.
Besides the keyword arguments of the :py:`report()` function, :attr:`.report.Config.legacy` can select another implementation of that function (key "module"), such as the one in :mod:`message_data`, and steps to run before and after it; see there.
With :program:`--legacy` or :program:`--legacy-config`, :program:`--output` (:program:`-o`) gives the directory for output, and :program:`--dest` reports a clone of the scenario, leaving the scenario itself untouched.

.. _report-legacy-country:

Models of one country
---------------------

A model of one country built from a MESSAGEix-GLOBIOM R12 SSP scenario, by removing every node but one, is reported with the file :file:`data/report/legacy/country.yaml`:

.. code-block:: shell

   mix-models \
     --url="ixmp://PLATFORM/MESSAGEix-ALB_SSP2_v1.8/baseline" \
     report --legacy-config=country \
     --dest="ixmp://PLATFORM/MESSAGEix-ALB_SSP2_v1.8_rep/baseline" \
     --output=./report

This repeats the reporting recipe of the R12 SSP (v6.5) scenarios:

1. Remove time series data from the first model year on.
2. Run :func:`.material.report.run_reporting.run`, summing all nodes into the region R12_GLB, and store its results as time series data.
3. Run the legacy :py:`report()` of :mod:`message_data` with :py:`merge_hist=True, merge_ts=True, run_config="materials_daccs_run_config.yaml"`.

It needs :mod:`message_data` installed, at a version that handles a scenario without the land-use emulator (empty :py:`LAND`, no :py:`land_scenario`) or without regional ‘fil’ files (factors for HFC emissions), and that does not connect to the default :mod:`ixmp` platform on import.
The materials reporting skips technologies that the model lacks.

In addition, for a model of one country:

- Nodes that the platform does not know are registered as regions, since :meth:`ixmp.TimeSeries.add_timeseries` refuses data for unknown regions.
- **Statistical differences.**
  A country model may contain technologies named :py:`stat_diff_{commodity}_{level}` that are active only in the calibration year.
  Each supplies (mode M1, output) or takes up (mode M2, input) a gap between the observed supply and use of a commodity at a level, or a gap in a relation (:py:`stat_diff_{relation}`).
  The legacy reporting selects technologies by name, so these appear in none of its variables; for instance, Final Energy, reported from the input of the users, includes energy that a statistical difference supplies, while Primary and Secondary Energy do not.
  They are reported as :py:`Statistical Difference|{commodity}|{level}` in EJ/yr, positive for a supply and negative for a use, and :py:`Statistical Difference|{relation}` in GWa (activity times the coefficient in the relation), at each node and summed into R12_GLB.
- The scenario's time series data after reporting are written to :file:`timeseries.csv`, and the materials reporting to :file:`materials.csv`, in a directory named :file:`{model}_{scenario}` under the :program:`--output` path; the legacy reporting writes an Excel file there too.
- Closure checks are logged for each node: Emissions|CO2 against the activity of CO2_TCE, and Emissions|Kyoto Gases against EMISS of TCE (both times 44/12); Final Energy against the sum of its sectors and of its fuels; and Primary Energy against the sum of its fuels.

.. automodule:: message_ix_models.report.legacy.country
   :members:

Reference
---------

.. currentmodule:: message_ix_models.report.legacy.iamc_report_hackathon

.. autofunction:: report
