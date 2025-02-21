Reporting (:mod:`~.message_ix_models.report`)
*********************************************

On this page:

.. contents::
   :local:

Elsewhere:

- ``global.yaml``, the :doc:`default-config`.
- Documentation for :mod:`genno` (:doc:`genno:index`), :mod:`ixmp.report`, and :mod:`message_ix.report`.
- Reporting for specific model variants:

  - :mod:`.water.reporting`
  - :doc:`transport/output` of :mod:`.model.transport`

- :doc:`‘Legacy’ reporting <legacy>`.

.. toctree::
   :hidden:

   default-config
   legacy

.. _report-intro:

Introduction
============

See :doc:`the discussion in the MESSAGEix docs <message-ix:reporting>` about the stack.
In short, for instance:

- :mod:`message_ix` **must not** contain reporting code that references :py:`technology="coal_ppl"`, because not every model built on the MESSAGE framework will have a technology with this name.
- Any model in the MESSAGEix-GLOBIOM family—built with :mod:`message_ix_models` and/or :mod:`message_data`—**should**, with few exceptions, have a :py:`technology="coal_ppl"`, since this appears in the common list of :ref:`technology-yaml`.
  Reporting specific to this technology ID, *as it is represented* in this model family, should be in :mod:`message_ix_models` or user code.

The basic **design pattern** of :mod:`message_ix_models.report` is:

- :func:`~.report.prepare_reporter` populates a new :class:`~.message_ix.Reporter` for a given :class:`.Scenario` with many keys to report all quantities of interest in a MESSAGEix-GLOBIOM–family model.
- This function relies on *callbacks* defined in multiple submodules to add keys and tasks for general or tailored reporting calculations and actions.
  Additional modules **should** define callback functions and register them with :func:`~report.register` when they are to be used.
  For example:

  1. The module :mod:`message_ix_models.report.plot` defines :func:`.plot.callback` that adds standard plots to the Reporter.
  2. The module :mod:`message_data.model.transport.report` defines :func:`~.message_data.model.transport.report.callback` that adds tasks specific to MESSAGEix-Transport.
  3. The module :mod:`message_data.projects.navigate.report` defines :func:`~.message_data.projects.navigate.report.callback` that add tasks specific to the ‘NAVIGATE’ research project.

  The callback (1) is always registered, because these plots are always applicable and can be expected to function correctly for all models in the family. In contrast, (2) and (3) **should** only be registered and run for the specific model variants for which they are developed/intended.

  Modules with tailored reporting configuration **may** also be indicated on the :ref:`command line <report-cli>` by using the :program:`-m/--modules` option: :program:`mix-models report -m model.transport`.

- A file :file:`global.yaml` file (in `YAML <https://en.wikipedia.org/wiki/YAML#Example>`_ format) contains a description of some of the reporting computations needed for a MESSAGE-GLOBIOM model.
  :func:`~.report.prepare_reporter` uses the :doc:`configuration handlers <genno:config>` built into :mod:`genno` (and some extensions specific to :mod:`message_ix_models`) to handle the different sections of the file.

Features
========

By combining these genno, ixmp, message_ix, and message_ix_models features, the following functionality is provided.

.. note:: If any of this does not appear to work as advertised, file a bug!

Units
-----

- Are read automatically for ixmp parameters.
- Pass through calculations/are derived automatically.
- Are recognized based on the definitions of non-SI units from `IAMconsortium/units <https://github.com/IAMconsortium/units/>`_.
- Are discarded when inconsistent.
- Can be overridden for entire parameters:

  .. code-block:: yaml

     units:
       apply:
         inv_cost: USD

- Can be set explicitly when converting data to IAMC format:

  .. code-block:: yaml

     iamc:
     # 'value' will be in kJ; 'units' will be the string 'kJ'
     - variable: Variable Name
       base: example_var:a-b-c
       units: kJ

API reference
=============

.. currentmodule:: message_ix_models.report

.. automodule:: message_ix_models.report
   :members:

   .. autosummary::

      Config
      prepare_reporter
      register
      report

.. currentmodule:: message_ix_models.report.plot

Plots
-----

.. automodule:: message_ix_models.report.plot
   :members:

.. currentmodule:: message_ix_models.report.operator

Operators
---------

.. automodule:: message_ix_models.report.operator
   :members:
   :exclude-members: add_par_data

   :mod:`message_ix_models.report.operator` provides the following:

   .. autosummary::

      codelist_to_groups
      compound_growth
      exogenous_data
      filter_ts
      from_url
      get_ts
      gwp_factors
      make_output_path
      model_periods
      remove_ts
      share_curtailment

   The following functions, defined elsewhere, are exposed through :mod:`.operator` and so can also be referenced by name:

   .. autosummary::

      message_ix_models.util.add_par_data

   Other operators or genno-compatible functions are provided by:

   - Upstream packages:

     - :mod:`message_ix.report.operator`
     - :mod:`ixmp.report.operator`
     - :mod:`genno.operator`

   - Other submodules:

     - :mod:`.model.emissions`: :func:`.get_emission_factors`.

   Any of these can be made available for a :class:`.Computer` instance using :meth:`~.genno.Computer.require_compat`, for instance:

   .. code-block::

      # Indicate that a certain module contains functions to
      # be referenced by name
      c.require_compat("message_ix_models.model.emissions")

      # Add computations to the graph by referencing functions
      c.add("ef:c", "get_emission_factors", units="t C / kWa")

Utilities
---------

.. currentmodule:: message_ix_models.report.util
.. automodule:: message_ix_models.report.util
   :members:

   .. autosummary::

      add_replacements
      collapse
      collapse_gwp_info
      copy_ts


.. currentmodule:: message_ix_models.report.compat

Compatibility with :mod:`.report.legacy`
----------------------------------------

.. automodule:: message_ix_models.report.compat
   :members:

   :mod:`.report.compat` prepares a Reporter to perform the same calculations as :mod:`.report.legacy`, except using :mod:`genno`.

   .. warning:: This code is **under development** and **incomplete**.
      It is not yet a full or exact replacement for :mod:`.report.legacy`.
      Use with caution.

   Main API:

   .. autosummary::
      TECH_FILTERS
      callback
      prepare_techs
      get_techs

   Utility functions:

   .. autosummary::
      inp
      eff
      emi
      out

.. _report-cli:

Command-line interface
======================

.. currentmodule:: message_ix_models.report.cli
.. automodule:: message_ix_models.report.cli
   :members:


.. code-block::

   $ mix-models report --help

   Usage: mix-models report [OPTIONS] [KEY]

     Postprocess results.

     KEY defaults to the comprehensive report 'message::default', but may also be
     the name of a specific model quantity, e.g. 'output'.

     --config can give either the absolute path to a reporting configuration
     file, or the stem (i.e. name without .yaml extension) of a file in
     data/report.

     With --from-file, read multiple Scenario identifiers from FILE, and report
     each one. In this usage, --output-path may only be a directory.

   Options:
     --dry-run             Only show what would be done.
     --config TEXT         Path or stem for reporting config file.  [default:
                           global]
     -L, --legacy          Invoke legacy reporting.
     -m, --module MODULES  Add extra reporting for MODULES.
     -o, --output PATH     Write output to file instead of console.
     --from-file FILE      Report multiple Scenarios listed in FILE.
     --help                Show this message and exit.

Testing
=======

.. currentmodule:: message_ix_models.report.sim
.. automodule:: message_ix_models.report.sim
   :members:

Continuous reporting
--------------------

As part of the :ref:`test-suite`, reporting is run on the same events (pushes and daily schedule) on publicly-available :doc:`model snapshots </api/model-snapshot>`.
One goal of these tests *inter alia* is to ensure that adjustments and improvements to the reporting code do not disturb manually-verified model outputs.

As part of the (private) :mod:`message_data` test suite, multiple workflows run on regular schedules; some of these include a combination of :mod:`message_ix_models`-based and :ref:`‘legacy’ reporting <report-legacy>`.
These workflows:

- Operate on specific scenarios within IIASA databases.
- Create files in CSV, Excel, and/or PDF formats that are that are preserved and made available as 'build artifacts' via the GitHub Actions web interface and API.
