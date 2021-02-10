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

:mod:`message_data.reporting` is developed on the basis of :doc:`message_ix <message_ix:reporting>`, :doc:`ixmp <ixmp:reporting>`, and :mod:`genno`.
Each layer of the stack provides reporting features that match the framework features at the corresponding level:

.. list-table::
   :header-rows: 1

   * - Package
     - Role
     - Core feature
     - Reporting feature
   * - ``genno``
     - Structured computations
     - :class:`~genno.Computer`,
       :class:`~genno.Key`,
       :class:`~genno.Quantity`
     - —
   * - ``ixmp``
     - Optimization models & data
     - Sets, parameters, variables
     - Auto-populated :class:`~ixmp.Reporter`
   * - ``message_ix``
     - Generalized energy model
     - Specific sets/parameters (``output``)
     - Derived quantities (``tom``)
   * - ``message_data``
     - MESSAGEix-GLOBIOM models
     - Specific structure (``coal_ppl`` in ``t``)
     - Calculations for M-G tech groups

For example: :mod:`message_ix` cannot contain reporting code that references ``coal_ppl``, because not every model built on the MESSAGE framework will have a technology with this name.
Any reporting specific to ``coal_ppl`` must be in :mod:`message_data`, since all models in the MESSAGEix-GLOBIOM family will have this technology.

The basic **design pattern** of :mod:`message_data.reporting` is:

- A ``global.yaml`` file (i.e. in `YAML <https://en.wikipedia.org/wiki/YAML#Example>`_ format) that contains a *concise* yet *explicit* description of the reporting computations needed for a MESSAGE-GLOBIOM model.
- :func:`.prepare_reporter` reads the file and a Scenario object, and uses it to populate a new Reporter…
- …by calling :doc:`configuration handlers <genno:config>` that process sections or items from the file.

Features
========

By combining these genno, ixmp, message_ix, and message_data features, the following functionality is provided.

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

.. automodule:: message_data.reporting
   :members:


Core
----

.. currentmodule:: message_data.reporting.core

.. automodule:: message_data.reporting.core
   :members:


Computations
------------

.. currentmodule:: message_data.reporting.computations
.. automodule:: message_data.reporting.computations
   :members:

   :mod:`message_data` provides the following:

   .. autosummary::

      gwp_factors
      share_curtailment

   Other computations are provided by:

   - :mod:`message_ix.reporting.computations`
   - :mod:`ixmp.reporting.computations`
   - :mod:`genno.computations`

Utilities
---------

.. currentmodule:: message_data.reporting.util
.. automodule:: message_data.reporting.util
   :members:


.. currentmodule:: message_data.reporting.cli
.. automodule:: message_data.reporting.cli
   :members:
