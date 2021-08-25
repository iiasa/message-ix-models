Reporting
*********

.. contents::
   :local:

See also:

- ``global.yaml``, the :doc:`reporting/default-config`.
- Documentation for :mod:`genno`, :mod:`ixmp.reporting`, and :mod:`message_ix.reporting`.
- `“Reporting” project board <https://github.com/orgs/iiasa/projects/3>`_ on GitHub for the initial development of these features.

.. toctree::
   :hidden:

   reporting/default-config

Introduction
============

See :doc:`the discussion in the MESSAGEix docs <message_ix:reporting>` about the stack.
In short, :mod:`message_ix` cannot contain reporting code that references ``coal_ppl``, because not every model built on the MESSAGE framework will have a technology with this name.
Any reporting specific to ``coal_ppl`` must be in :mod:`message_data`, since all models in the MESSAGEix-GLOBIOM family will have this technology.

The basic **design pattern** of :mod:`message_data.reporting` is:

- A ``global.yaml`` file (i.e. in `YAML <https://en.wikipedia.org/wiki/YAML#Example>`_ format) that contains a *concise* yet *explicit* description of the reporting computations needed for a MESSAGE-GLOBIOM model.
- :func:`~.reporting.prepare_reporter` reads the file and a Scenario object, and uses it to populate a new Reporter.
  This function mostly relies on the :doc:`configuration handlers <genno:config>` built in to Genno to handle the different sections of the file.

Features
========

By combining these genno, ixmp, message_ix, and message_data features, the following functionality is provided.

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


Continous reporting
===================

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

.. currentmodule:: message_data.reporting

.. automodule:: message_data.reporting
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
