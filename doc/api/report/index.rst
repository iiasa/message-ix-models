Reporting (:mod:`~.message_ix_models.report`)
*********************************************

.. contents::
   :local:

See also:

- ``global.yaml``, the :doc:`default-config`.
- Documentation for :mod:`genno` (:doc:`genno:index`), :mod:`ixmp.reporting`, and :mod:`message_ix.reporting`.

.. toctree::
   :hidden:

   default-config

Not public:

- `“Reporting” project board <https://github.com/orgs/iiasa/projects/3>`_ on GitHub for the initial implementation of these features.
- :doc:`m-data:/reference/tools/post_processing`, still in use.
- Documentation for reporting specific to certain model variants:

  - :doc:`m-data:/reference/model/transport/report`

Introduction
============

See :doc:`the discussion in the MESSAGEix docs <message_ix:reporting>` about the stack.
In short, :mod:`message_ix` must not contain reporting code that references ``coal_ppl``, because not every model built on the MESSAGE framework will have a technology with this name.
Any reporting specific to ``coal_ppl`` must be in :mod:`message_ix_models`, since all models in the MESSAGEix-GLOBIOM family will have this technology.

The basic **design pattern** of :mod:`message_ix_models.report` is:

- A ``global.yaml`` file (i.e. in `YAML <https://en.wikipedia.org/wiki/YAML#Example>`_ format) that contains a *concise* yet *explicit* description of the reporting computations needed for a MESSAGE-GLOBIOM model.
- :func:`~.report.prepare_reporter` reads the file and a Scenario object, and uses it to populate a new Reporter.
  This function mostly relies on the :doc:`configuration handlers <genno:config>` built in to Genno to handle the different sections of the file.

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

Continuous reporting
====================

.. note:: This section is no longer current.

The IIASA TeamCity build server is configured to automatically run the full (:file:`global.yaml`) reporting on the following scenarios:

.. literalinclude:: ../../ci/report.yaml
   :caption: :file:`ci/report.yaml`
   :language: yaml

This takes place:

- every morning at 07:00 IIASA time, and
- for every commit on every pull request branch, *if* the branch name includes ``report`` anywhere, e.g. ``feature/improve-reporting``.

The results are output to Excel files that are preserved and made available as 'build artifacts' via the TeamCity web interface.

API reference
=============

.. currentmodule:: message_ix_models.report

.. automodule:: message_ix_models.report
   :members:

   .. autosummary::

      prepare_reporter
      register
      report

Operators
---------

.. currentmodule:: message_ix_models.report.computations
.. automodule:: message_ix_models.report.computations
   :members:

   :mod:`message_ix_models.report.computations` provides the following:

   .. autosummary::

      from_url
      get_ts
      gwp_factors
      make_output_path
      model_periods
      remove_ts
      share_curtailment

   Other operators or genno-compatible functions are provided by:

   - Upstream packages:

     - :mod:`message_ix.reporting.computations`
     - :mod:`ixmp.reporting.computations`
     - :mod:`genno.computations`

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
      as_quantity
      collapse
      collapse_gwp_info
      copy_ts


Command-line interface
----------------------

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
