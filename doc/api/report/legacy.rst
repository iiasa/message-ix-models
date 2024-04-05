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

Reference
---------

.. currentmodule:: message_ix_models.report.legacy.iamc_report_hackathon

.. autofunction:: report
