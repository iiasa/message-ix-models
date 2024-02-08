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

.. _tools-wb:

World Bank structures (:mod:`.tools.wb`)
========================================

.. automodule:: message_ix_models.tools.wb
   :members:


Technoeconomic investment and fixed O&M costs projection (:mod:`.tools.costs`)
==============================================================================

:mod:`.tools.costs` contains functions for projection investment and fixed costs for technologies in MESSAGEix.

The main function to use is :func:`.create_cost_projections`, which calls the other functions in the module in the correct order.
The default settings for the function are contained in the config file: :file:`tools/costs/config.py`.

The general breakdown of the module is as follows:

1. :mod:`tools.costs.regional_differentiation` calculates the regional differentiation of costs for technologies.
2. :mod:`tools.costs.learning` projects the costs of technologies in a reference region with only a cost reduction rate applied.
3. :mod:`tools.costs.gdp` adjusts the regional differentiation of costs for technologies based on the GDP per capita of the region.
4. :mod:`tools.costs.splines` applies a polynomial regression (degrees = 3) to each technology's projected costs in the reference region and applies a spline after a convergence year.
5. :mod:`tools.costs.projections` combines all the above steps and returns a class object with the projected costs for each technology in each region.

.. currentmodule:: message_ix_models.tools.costs.regional_differentiation

Regional differentiation of costs (:mod:`.tools.costs.regional_differentiation`)
---------------------------------------------------------------------------------

.. automodule:: message_ix_models.tools.costs.regional_differentiation
   :members:

   .. autosummary::

      get_weo_data
      get_intratec_data
      get_raw_technology_mapping
      subset_materials_map
      adjust_technology_mapping
      get_weo_regional_differentiation
      get_intratec_regional_differentiation
      apply_regional_differentiation


.. currentmodule:: message_ix_models.tools.costs.learning

Cost reduction of technologies over time (:mod:`.tools.costs.learning`)
------------------------------------------------------------------------

.. automodule:: message_ix_models.tools.costs.learning
   :members:

   .. autosummary::

      get_cost_reduction_data
      get_technology_learning_scenarios_data
      project_ref_region_inv_costs_using_learning_rates

.. currentmodule:: message_ix_models.tools.costs.gdp

GDP-adjusted costs and regional differentiation (:mod:`.tools.costs.gdp`)
--------------------------------------------------------------------------

.. automodule:: message_ix_models.tools.costs.gdp
   :members:

   .. autosummary::

      default_ref_region
      process_raw_ssp_data
      adjust_cost_ratios_with_gdp


.. currentmodule:: message_ix_models.tools.costs.splines

Spline costs after convergence (:mod:`.tools.costs.splines`)
------------------------------------------------------------

.. automodule:: message_ix_models.tools.costs.splines
   :members:

   .. autosummary::

      apply_splines_to_convergence


.. currentmodule:: message_ix_models.tools.costs.projections 

Projection of costs given input parameters (:mod:`.tools.costs.projections`)
----------------------------------------------------------------------------

.. automodule:: message_ix_models.tools.costs.projections
   :members:

   .. autosummary::

      create_projections_learning
      create_projections_gdp
      create_projections_converge
      create_message_outputs
      create_iamc_outputs
      create_cost_projections
