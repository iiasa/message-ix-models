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

   The tools in this module support use of data from arbitrary sources and formats in model-building code.
   For each source/format, a subclass of :class:`.ExoDataSource` adds tasks to a :class:`genno.Computer`
   that retrieve/load and transform the source data into :class:`genno.Quantity`.

   An example using one such class, :class:`message_ix_models.project.advance.data.ADVANCE`.

   .. code-block:: python

      from genno import Computer

      from message_ix_models.project.advance.data import ADVANCE

      # Keyword arguments corresponding to ADVANCE.Options
      kw = dict(
          measure="Transport|Service demand|Road|Passenger|LDV",
          model="MESSAGE",
          scenario="ADV3TRAr2_Base",
      )

      # Add tasks to retrieve and transform data
      c = Computer()
      keys = c.apply(ADVANCE, context=context, **kw)

      # Retrieve some of the data
      q_result = c.get(keys[0])

      # Pass the data into further calculations
      c.add("derived", "mul", keys[1], k_other)

   .. autosummary::

      MEASURES
      SOURCES
      BaseOptions
      DemoSource
      ExoDataSource
      add_structure
      iamc_like_data_for_query
      prepare_computer
      register_source

.. autoclass:: ExoDataSource
   :members:
   :private-members: _where
   :special-members: __init__

   As an abstract class ExoDataSource **must** be subclassed to be used.
   Concrete subclasses **must** implement at least the :meth:`~ExoDataSource.get` method
   that performs the loading of the raw data when executed,
   and **may** override others, as described below.

   The class method :meth:`.ExoDataSource.add_tasks` adds tasks to a :class:`genno.Computer`.
   It returns a :class:`genno.Key` that refers to the loaded and transformed data.
   This method usually **should not** be modified for subclasses.

   The behaviour of a subclass can be customized in these ways:

   1. Create a subclass of :class:`.BaseOptions`
      and set it as the :attr:`~.ExoDataSource.Options` class attribute.
   2. Override :meth:`~.ExoDataSource.__init__`,
      which receives keyword arguments via :meth:`.add_tasks`.
   3. Override :meth:`~.ExoDataSource.transform`,
      which is called to add further tasks which will transform the data.

   See the documentation for these methods and attributes for further details.

.. autofunction:: prepare_computer

   .. deprecated:: 2025-06-06
      Use :py:`c.apply(SOURCE.add_tasks, …)` as shown above.

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
