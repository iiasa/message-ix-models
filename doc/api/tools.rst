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
   :exclude-members: ExoDataSource

   .. autosummary::

      MEASURES
      SOURCES
      DemoSource
      ExoDataSource
      prepare_computer
      register_source

.. autoclass:: ExoDataSource
   :members:
   :special-members: __init__, __call__

.. currentmodule:: message_ix_models.tools.advance

ADVANCE data (:mod:`.tools.advance`)
====================================

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
