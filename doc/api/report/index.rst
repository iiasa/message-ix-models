Reporting
*********

.. contents::
   :local:

See also:

- `“Reporting” project board <https://github.com/orgs/iiasa/projects/3>`_ on GitHub, tracking ongoing development.
- ``global.yaml``, the :doc:`reporting/default-config`.

.. toctree::
   :hidden:

   reporting/default-config

Introduction
============

:mod:`message_data.reporting` is developed on the basis of :doc:`message_ix <message_ix:reporting>`, and in turn :doc:`ixmp <ixmp:reporting>` features.
Each layer of the stack provides reporting features that match the framework features at the corresponding level:

.. list-table::
   :header-rows: 1

   * - Stack level
     - Role
     - Core feature
     - Reporting feature
   * - ``ixmp``
     - Optimization models & data
     - N-dimensional parameters
     - :class:`~ixmp.reporting.Reporter`,
       :class:`~ixmp.reporting.Key`,
       :class:`~ixmp.reporting.quantity.Quantity`
   * - ``message_ix``
     - Generalized energy model
     - Specific sets/parameters (``output``)
     - Derived quantities (``tom``)
   * - ``message_data``
     - MESSAGEix-GLOBIOM models
     - Specific set members (``coal_ppl`` in ``t``)
     - Calculations for M-G tech groups

For example: ``message_ix`` cannot contain reporting code that references ``coal_ppl``, because not every model built on the MESSAGE framework will have a technology with this name.
Any reporting specific to ``coal_ppl`` must be in ``message_data``, since all models in the MESSAGEix-GLOBIOM family will have this technology.

The basic **design pattern** of :mod:`message_data.reporting` is:

- A ``global.yaml`` file (i.e. in `YAML <https://en.wikipedia.org/wiki/YAML#Example>`_ format) that contains a *concise* yet *explicit* description of the reporting computations needed for a MESSAGE-GLOBIOM model.
- :func:`.prepare_reporter` reads the file and a Scenario object, and uses it to populate a new Reporter…
- …by calling methods like :func:`.add_aggregate` that process atomic chunks of the file.

Features
========

By combining these ixmp, message_ix, and message_data features, the following functionality is provided.

.. note:: If any of this does not appear to work as advertised, file a bug!

Units
-----

- read automatically for ixmp parameters.
- pass through calculations/are derived automatically.
- are recognized based on the definitions of non-SI units from `IAMconsortium/units <https://github.com/IAMconsortium/units/>`_.
- are discarded when inconsistent.
- can be overridden for entire parameters:

  .. code-block:: yaml

     units:
       apply:
         inv_cost: USD

- can be set explicitly when converting data to IAMC format:

  .. code-block:: yaml

     iamc:
     # 'value' will be in kJ; 'units' will be the string 'kJ'
     - variable: Variable Name
       base: example_var:a-b-c
       units: kJ


Continous reporting
===================

The IIASA TeamCity build server is configured to automatically run the full (:file:`global.yaml`) reporting on the following scenarios:

.. literalinclude:: ../../ci/report.yaml
   :language: yaml

This takes place:

- every morning at 07:00 IIASA time, and
- for every commit on every pull request branch, *if* the branch name includes ``report`` anywhere, e.g. ``feature/improve-reporting``.

The results are output to Excel files that are preserved and made available as 'build artifacts' via the TeamCity web interface.


API reference
=============

.. currentmodule:: message_data.reporting


Core
----

.. currentmodule:: message_data.reporting.core

.. autosummary::

   prepare_reporter
   add_aggregate
   add_combination
   add_general
   add_iamc_table
   add_report

.. autofunction:: message_data.reporting.core.prepare_reporter

.. automodule:: message_data.reporting.core
   :members:
   :exclude-members: prepare_reporter


Computations
------------

.. currentmodule:: message_data.reporting.computations
.. automodule:: message_data.reporting.computations
   :members:


Utilities
---------

.. currentmodule:: message_data.reporting.util
.. automodule:: message_data.reporting.util
   :members:


.. currentmodule:: message_data.reporting.cli
.. automodule:: message_data.reporting.cli
   :members:
