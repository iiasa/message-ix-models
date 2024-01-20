General purpose modeling tools
******************************

“Tools” can include, *inter alia*:

- Codes for retrieving data from specific data sources and adapting it for use with :mod:`message_ix_models`.
- Codes for modifying scenarios; although tools for building models should go in :mod:`message_ix_models.model`.

On this page:

.. contents::
   :local:
   :backlinks: none

.. currentmodule:: message_ix_models.tools

.. automodule:: message_ix_models.tools
   :members:

.. currentmodule:: message_ix_models.tools.exo_data

Exogenous data (:mod:`.tools.exo_data`)
=======================================

.. automodule:: message_ix_models.tools.exo_data
   :members:
   :exclude-members: ExoDataSource, prepare_computer

   .. autosummary::

      MEASURES
      SOURCES
      DemoSource
      ExoDataSource
      iamc_like_data_for_query
      prepare_computer
      register_source

.. autofunction:: prepare_computer

   The first returned key, like ``{measure}:n-y``, triggers the following computations:

   1. Load data by invoking a :class:`ExoDataSource`.
   2. Aggregate on the |n| (node) dimension according to :attr:`.Config.regions`.
   3. Interpolate on the |y| (year) dimension according to :attr:`.Config.years`.

   Additional key(s) include:

   - ``{measure}:n-y:y0 indexed``: same as ``{measure}:n-y``, indexed to values as of |y0| (the first model year).

   See particular data source classes, like :class:`.SSPOriginal`, for particular examples of usage.

   .. todo:: Extend to also prepare to compute values indexed to a particular |n|.

.. autoclass:: ExoDataSource
   :members:
   :special-members: __init__, __call__

.. currentmodule:: message_ix_models.tools.advance

ADVANCE data (:mod:`.tools.advance`)
====================================

.. deprecated:: 2023.11
   Use :mod:`.project.advance` instead.

.. autosummary::
   get_advance_data
   advance_data

.. autodata:: LOCATION

This is a location relative to a parent directory.
The specific parent directory depends on whether :mod:`message_data` is available:

Without :mod:`message_data`:
   The code finds the data within :ref:`local-data` (see discussion there for how to configure this location).
   Users should:

   1. Visit https://tntcat.iiasa.ac.at/ADVANCEWP2DB/dsd?Action=htmlpage&page=about and register for access to the data.
   2. Log in.
   3. Download the snapshot with the file name given in :data:`LOCATION` to a subdirectory :file:`advance/` within their local data directory.

With :mod:`message_data`:
   The code finds the data within :ref:`private-data`.
   The snapshot is stored directly in the repository using Git LFS.

.. automodule:: message_ix_models.tools.advance
   :members:
   :exclude-members: LOCATION
   :private-members:

.. currentmodule:: message_ix_models.tools.iamc

IAMC data structures (:mod:`.tools.iamc`)
=========================================

.. automodule:: message_ix_models.tools.iamc
   :members:

.. _tools-iea:

International Energy Agency (IEA) data and structure (:mod:`.tools.iea`)
========================================================================

.. currentmodule:: message_ix_models.tools

Documentation for all module contents:

.. autosummary::
   :toctree: _autosummary
   :template: autosummary-module.rst
   :recursive:

   iea

(Extended) World Energy Balances (:mod:`.tools.iea.web`)
--------------------------------------------------------

These data are proprietary and require a paid subscription.

Up until 2023, the EWEB data were available from the OECD iLibrary with DOI `10.1787/enestats-data-en <https://doi.org/10.1787/enestats-data-en>`__.
These files were characterized by:

- Single ZIP archives with names like :file:`cac5fa90-en.zip`; typically ~850 MiB compressed size,
- …containing a single CSV file with a name like :file:`WBIG_2022-2022-1-EN-20230406T100006.csv`, typically >20 GiB uncompressed,
- …with a particular list of columns like: "MEASURE", "Unit", "COUNTRY", "Country", "PRODUCT", "Product", "FLOW", "Flow", "TIME", "Time", "Value", "Flag Codes", "Flags",
- …with contents that duplicated code IDs—for instance, in the "FLOW" column—with human-readable labels—for instance in the "Flow" column.

This source is now discontinued.

From 2023 (or earlier), the data are also available directly from the IEA website at https://www.iea.org/data-and-statistics/data-product/world-energy-balances.
This source is available in two formats; ‘IVT’ or “Beyond 20/20” format (not supported by this module) or fixed-width text files.
The latter are characterized by:

- Multiple ZIP archives with names like :file:`WBIG[12].zip`, each containing a portion of the data and typically 110–130 MiB compressed size
- …each containing a single, fixed-with TXT file with a name like :file:`WORLDBIG[12].TXT`, typically 3–4 GiB uncompressed,
- …with no column headers, but data resembling::

    WORLD  HARDCOAL  1960  INDPROD  KTOE ..

  …that appear to correspond to, respectively, the COUNTRY, PRODUCT, TIME, FLOW, and MEASURE dimensions and "Value" column of the above data, respectively.

This source comes with documentation (`2023 edition <https://iea.blob.core.windows.net/assets/0acb1453-1221-421b-9131-632ce71a4c1a/WORLDBAL_Documentation.pdf>`__) that, unlike the data, *is* publicly accessible.

The module :mod:`message_ix_models.tools.iea.web` attempts to detect and support both formats.
The approach to handling proprietary data is the same as in :mod:`.project.advance` and :mod:`.project.ssp`:

- A copy of the data are stored in :mod:`message_data`.
  Non-IIASA users must obtain their own license to access and use the data.
- :mod:`message_ix_models` contains only a ‘fuzzed’ version of the data (same structure, random values) for testing purposes.

.. _tools-wb:

World Bank structures (:mod:`.tools.wb`)
========================================

.. automodule:: message_ix_models.tools.wb
   :members:
