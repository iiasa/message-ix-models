.. currentmodule:: message_ix_models.project.ssp

Shared Socioeconomic Pathways (:mod:`.project.ssp`)
***************************************************

Structure
=========

The enumerations :obj:`SSP_2017` and :obj:`SSP_2024` contain one member from the corresponding SDMX code lists.
These can be used to uniquely identify both an SSP narrative *and* the set in which it occurs, in applications where this distinction is meaningful:

.. code-block:: py

   >>> from message_ix_models.project.ssp import SSP_2017, SSP_2024
   >>> x = SSP_2017["2"]
   >>> y = SSP_2024["2"]
   >>> str(y)
   "ICONICS:SSP(2024).2"
   >>> x == y
   False

.. automodule:: message_ix_models.project.ssp
   :members:

Data
====

.. automodule:: message_ix_models.project.ssp.data
   :members:

   Although free of charge, neither the 2017 or 2024 SSP data can be downloaded automatically.
   Both sources require that users first submit personal information to register before being able to retrieve the data.
   :mod:`message_ix_models` does not circumvent this requirement.
   Thus:

   - A copy of the data are stored in :mod:`message_data`.
   - :mod:`message_ix_models` contains only a ‘fuzzed’ version of the data (same structure, random values) for testing purposes.

   .. todo:: Allow users without access to :mod:`message_data` to read a local copy of this data from a :attr:`.Config.local_data` subdirectory.

   .. autosummary::
      SSPOriginal
      SSPUpdate

2024 update
===========

Transport
---------

.. currentmodule:: message_ix_models.project.ssp.transport

.. automodule:: message_ix_models.project.ssp.transport
   :members:

   Use :program:`mix-models ssp transport --help in.xlsx out.xlsx` to invoke :func:`.main`.
