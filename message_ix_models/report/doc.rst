Reporting
=========

.. contents::
   :local:

Introduction
------------

:mod:`message_data.reporting` is developed on the basis of :doc:`message_ix <message_ix:reporting>`, and in turn :doc:`ixmp <ixmp:reporting>` features.
Each layer of the stack provides reporting features that match the framework features at the corresponding level:

.. list-table::
   :header-rows: 0

   * - Stack level
     - Role
     - Core feature
     - Reporting feature
   * - ``ixmp``
     - Optimization models
     - N-D parameters
     - :class:`Reporter <ixmp.reporting.Reporter>`,
       :class:`Key <ixmp.reporting.Key>`,
       :class:`Quantity <ixmp.reporting.utils.Quantity>`.
   * - ``message_ix``
     - Generalized energy model framework
     - Specific parameters (``output``)
     - Auto derivatives (``tom``)
   * - ``message_data``
     - MESSAGE-GLOBIOM model family
     - Specific set members (``coal_ppl`` in ``t``)
     - Calculations for tec groups

For instance, ``message_ix`` cannot contain reporting code that references ``coal_ppl``

The basic **design pattern** of :mod:`message_data.reporting` is:

- A ``global.yaml`` file (i.e. in `YAML <https://en.wikipedia.org/wiki/YAML#Example>`_ format) that contains a *concise* yet *explicit* description of the reporting computations needed for a MESSAGE-GLOBIOM model.
- :meth:`prepare_reporter <message_data.reporting.core.prepare_reporter>` reads the file and a Scenario object, and uses it to populate a new Reporter
- …by calling methods like :meth:`add_aggregate <message_data.reporting.code.add_aggregate>` that process atomic chunks of the file.

API reference
-------------

.. currentmodule:: message_data.reporting


Core
~~~~

.. currentmodule:: message_data.reporting.core

.. autosummary::

   prepare_reporter
   add_aggregate
   add_combination
   add_general
   add_iamc_table
   add_report

.. automodule:: message_data.reporting.core
   :members:


Computations
~~~~~~~~~~~~

.. automodule:: message_data.reporting.computations
   :members:


Utilities
~~~~~~~~~

.. automodule:: message_data.reporting.util
   :members:


.. automethod:: message_data.reporting.cli


Default configuration
---------------------

.. literalinclude:: ../../../data/report/global.yaml
   :language: yaml
