General purpose modeling tools (:mod:`.tools`)
**********************************************

“Tools” can include, *inter alia*:

- Codes for retrieving data from specific data sources and adapting it for use with :mod:`message_ix_models`.
- Codes for modifying scenarios; although tools for building models should go in :mod:`message_ix_models.model`.

On other pages:

- :doc:`tools-costs`

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
   :private-members: _where
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

.. _tools-wb:

World Bank structures (:mod:`.tools.wb`)
========================================

.. automodule:: message_ix_models.tools.wb
   :members:


Tools for scenario manipulation
===============================

.. currentmodule:: message_ix_models.tools

.. autosummary::
   :toctree: _autosummary
   :template: autosummary-module.rst
   :recursive:

   add_AFOLU_CO2_accounting
   add_CO2_emission_constraint
   add_FFI_CO2_accounting
   add_alternative_TCE_accounting
   add_budget
   add_emission_trajectory
   add_tax_emission
   remove_emission_bounds
   update_h2_blending
