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
   Data are read from PATH_IN, in :file:`.xlsx` or :file:`.csv` format.
   If :file:`.xlsx`, the data are first temporarily converted to :file:`.csv`.
   Data are written to PATH_OUT; if not given, this defaults to the same path and suffix as PATH_IN, with "_out" added to the stem.

   For example:

   .. code-block:: shell

      mix-models ssp transport --method=B \
        SSP_SSP2_v2.1_baseline.xlsx

   …produces a file :file:`SSP_SSP2_v2.1_baseline_out.xlsx` in the same directory.

   As of 2025-01-25:

   - Method 'B' (that is, :func:`.prepare_method_B`; see its documentation) is the preferred method.
   - The code is tested on :file:`.xlsx` files in the (internal) directories under `SharePoint > ECE > Documents > SharedSocioeconomicPathways2023 > Scenario_Vetting <https://iiasahub.sharepoint.com/sites/eceprog/Shared%20Documents/Forms/AllItems.aspx?csf=1&web=1&e=APKv0Z&CID=23fa0a51%2Dc303%2D4381%2D8c6d%2D143305cbc5a1&FolderCTID=0x012000AA9481BF7BE9264E85B14105F7F082FF&id=%2Fsites%2Feceprog%2FShared%20Documents%2FSharedSocioEconomicPathways2023%2FScenario%5FVetting&viewid=956acd8a%2De1e7%2D4ae9%2Dab1b%2D0506911bae11>`_, for example :file:`v2.1_Internal_version_Dec13_2024/Reporting_output/SSP_SSP2_v2.1_baseline.xlsx`.
