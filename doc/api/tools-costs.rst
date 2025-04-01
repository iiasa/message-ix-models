.. currentmodule:: message_ix_models.tools.costs

Investment and fixed costs (:mod:`.tools.costs`)
************************************************

:mod:`.tools.costs` implements methods to **project investment and fixed costs of technologies** [1]_ in MESSAGEix-GLOBIOM family models.

.. contents::
   :local:

.. [1] Fixed costs are also referred to as “operation and maintenance (O&M)” or “fixed O&M” costs.
   Investment and fixed costs are also collectively referred to as “techno-economic costs” or “techno-economic parameters”.

Methods
=======

The tool creates distinct projected cost values for different regions, technologies, and scenarios.
The costs are projected based on historical (mostly a base year) data and assumptions about future cost reductions.

The projections use the concept of a **reference region** [2]_ and apply distinct methods to the reference and non-reference regions:

Reference region
   Costs in the reference region are projected based on the following assumption: given a cost reduction rate, the cost of the technology in the reference region experiences an exponential decay over time.

Non-reference regions
   Costs for each technology in all non-reference regions may be calculated using one of three methods, specified using :attr:`.Config.method`:

   1. Constant cost reduction rate (:attr:`.Config.method` = "constant"): the regional cost ratio (versus the reference region) that is calculated in the base year is held constant and used to project regionally-differentiated costs across all years.
   2. Convergence to reference region costs by a certain year (:attr:`.Config.method` = "convergence"): all other regions' costs exponentially decay until they become they same as the reference region's cost by a specified year.
   3. GDP-adjusted cost reduction rate (:attr:`.Config.method` = "gdp"): this method assumes that regional costs converge not based on a specified year but based on GDP per capita.
      All non-reference regions' costs are adjusted based on the ratio of the GDP per capita of the region to the GDP per capita of the reference region.

.. [2] In :mod:`message_ix`, these are elements of the ``node`` set.
   The term ‘region’ is used in this documentation to mean the same thing.

Modules and model variants
==========================

Within the context of the tool, the term **module** (specified by :attr:`.Config.module`) is used to mean input data for particular *sets of technologies*.
These correspond to subsets of all the technologies in MESSAGEix-GLOBIOM models—either the base model or model variants. [3]_
Currently, :mod:`.tools.costs` supports two module :attr:`~.Config.module` settings:

"energy"
   Mostly electric power technologies, as well as a few other supply-side technologies.

   This can be considered the "base" module, corresponding to the "base" version of MESSAGEix-GLOBIOM, as it contains the most technologies.

"materials"
   Technologies conceived as part of the materials and industry sectors.

"cooling"
   Cooling technologies for power plants.

Data and files for a particular module can refer to other modules.
This allows for values or settings for "materials" and other technologies to be assumed to match the values and settings used for the referenced "energy"-module technologies.

.. [3] This usage of “module” differs from the meaning of a “Python module”.
   For instance, :mod:`message_ix_models.model.water` is a *Python module* for MESSAGEix-Nexus.
   If the setting :py:`.costs.Config.module = "water"` were added, this *might* refer to input data for projecting investment and fixed costs of water technologies that are defined in :mod:`message_ix_models.model.water`—but not necessarily.

To add a new module, the following steps are required:

- In :file:`message_ix_models/data/costs/`, create another subdirectory with the name of the new module, for instance :file:`message_ix_models/data/costs/[module]/`.
- Add the following files to the new directory:

  :file:`first_year_[module].csv`
     A file with a list of technologies and the corresponding first year that the respective technology can start being deployed/modeled.
     The file should have the following columns:

     - "message_technology": the technology name.
     - "first_year_original": the first year the technology can start being deployed.

  :file:`tech_map_[module].csv`
     A file with the mapping of technologies to a source of base year cost data.
     The file should have the following columns:

     - "message_technology": the technology name.
     - "reg_diff_source" and "reg_diff_technology": the source data for the regional differentiation of costs and the corresponding technology to map to.

       - If "reg_diff_source" is "energy", then "reg_diff_technology" should be a technology that is present in the "energy" module.
       - If "reg_diff_source" is "weo", then "reg_diff_technology" should be a technology that is present in the WEO data (refer to :file:`tech_map_energy.csv` for the names of WEO technologies available, as all energy technologies are mapped to a WEO technology).
       - You can also add another source of regional differentiation (in the case of :py:`module="materials"`, a newly created source called "intratec" is used).
         However, this method is a little more involved as it requires extending the code to read in new source data.
     - "base_year_reference_region_cost": the base year cost for the technology in the reference region.
     - "fix_ratio": the ratio of fixed O&M costs to investment costs for the technology.

- Add the new module to the allowed values of :attr:`.Config.module`.

Please note that the following assumptions are made in technology costs mapping:

- If a technology is mapped to a technology in the "energy" module, then the cost reduction across scenarios is the same as the cost reduction of the mapped technology.
- If a non-"energy" module (such as "materials" or "cooling") technology has :py:`reg_diff_source="energy"` and the "base_year_reference_region_cost" is not empty, then the "base_year_reference_region_cost" in :file:`tech_map_[module].csv` is used as the base year cost for the technology in the reference region.
  If the "base_year_reference_region_cost" is empty, then the cost reduction across scenarios is the same as the cost reduction of the mapped technology.
- If using a non-"energy" module (such as "materials" or "cooling"), if a technology that is specified in :file:`tech_map_materials.csv` already exists in :file:`tech_map_energy.csv`, then the reference region cost is taken from :file:`tech_map_materials.csv`.
- If a technology in a module is not mapped to any source of regional differentiation, then no cost reduction over the years is applied to the technology.
- If a technology has a non-empty "base_year_reference_region_cost" but is not mapped to any source of regional differentiation, then assume no regional differentiation and use the reference region base year cost as the base year cost for all regions.

Data sources
============

The tool uses the following data sources for the regional differentiation of costs:

- WEO: the World Energy Outlook data from the International Energy Agency (IEA).
- Intratec: the Intratec data, which is a database of production costs for chemicals and other materials.

The tool also uses :mod:`.ssp.data` (via :func:`.exo_data.prepare_computer`) to adjust the costs of technologies based on GDP per capita.

.. _costs-usage:

Usage
=====

:func:`.create_cost_projections` is the top-level entry point.

This function takes a single :class:`.costs.Config` object as an argument. The object carries all the settings understood by :func:`.create_cost_projections` and other functions.
Those settings include the following; click each for the full description, allowable values, and defaults:

   :attr:`~.Config.module`,
   :attr:`~.Config.method`,
   :attr:`~.Config.node`,
   :attr:`~.Config.ref_region`,
   :attr:`~.Config.scenario`,
   :attr:`~.Config.scenario_version`,
   :attr:`~.Config.base_year`,
   :attr:`~.Config.convergence_year`,
   :attr:`~.Config.use_vintages`,
   :attr:`~.Config.fom_rate`, and
   :attr:`~.Config.format`.

:func:`.create_cost_projections` in turn calls the other functions in the module in the correct order, and returns a Python :class:`dict` with the following keys mapped to :class:`pandas.DataFrame`.

- "inv_cost": the investment costs of the technologies in each region.
- "fix_cost": the fixed O&M costs of the technologies in each region.

To use the tool, you **must** first obtain a copy of the SSP input data.
See the documentation at :mod:`message_ix_models.project.ssp.data`.

Next, create a :class:`.Config` object and pass it as an argument to :func:`.create_cost_projections`::

   from message_ix_models.tools.costs import Config, create_cost_projections

   # Use default settings
   cfg = Config()

   # Compute cost projections
   costs = create_cost_projections(cfg)

   # Show the resulting data
   costs["inv_cost"]
   costs["fix_cost"]

These data can be further manipulated; for instance, added to a scenario using :func:`.add_par_data`.
See the file :file:`message_ix_models/tools/costs/demo.py` for multiple examples using various non-default settings to control the methods and data used by :func:`.create_cost_projections`.


.. note:: The data produced are for all valid combinations of :math:`(y^V, y^A)`—including those that are beyond the `technical_lifetime` of the |t| to which they apply.
   This may produce large data frames, depending on the number of technologies, regions, and scenarios.
   At the moment, :mod:`.tools.costs` does not filter out these combinations.
   If this is problematic, the user may consider filtering the data for valid combinations of :math:`(y^V, y^A)`.

Code reference
==============

The top-level function and configuration class:

.. autosummary::

   Config
   create_cost_projections

The other submodules implement the supporting methods, calculations, and data handling, in roughly the following order:

1. :mod:`~.costs.regional_differentiation` calculates the regional differentiation of costs for technologies.
2. :mod:`~.costs.decay` projects the costs of technologies in a reference region with only a cost reduction rate applied.
3. :mod:`~.costs.gdp` adjusts the regional differentiation of costs for technologies based on the GDP per capita of the region.
4. :mod:`~.costs.projections` combines all the above steps and returns the projected costs for each technology in each region.

.. automodule:: message_ix_models.tools.costs
   :members:

.. currentmodule:: message_ix_models.tools.costs.decay

Cost reduction of technologies over time (:mod:`~.costs.decay`)
---------------------------------------------------------------

.. automodule:: message_ix_models.tools.costs.decay
   :members:

   .. autosummary::

      get_cost_reduction_data
      get_technology_reduction_scenarios_data
      project_ref_region_inv_costs_using_reduction_rates

.. currentmodule:: message_ix_models.tools.costs.gdp

GDP-adjusted costs and regional differentiation (:mod:`~.costs.gdp`)
--------------------------------------------------------------------

.. automodule:: message_ix_models.tools.costs.gdp
   :members:

   .. autosummary::

      default_ref_region
      process_raw_ssp_data
      adjust_cost_ratios_with_gdp

.. currentmodule:: message_ix_models.tools.costs.projections

Projection of costs given input parameters (:mod:`~.costs.projections`)
-----------------------------------------------------------------------

.. automodule:: message_ix_models.tools.costs.projections
   :members:

   .. autosummary::

      create_projections_constant
      create_projections_gdp
      create_projections_converge
      create_message_outputs
      create_iamc_outputs

.. currentmodule:: message_ix_models.tools.costs.regional_differentiation

Regional differentiation of costs (:mod:`~.costs.regional_differentiation`)
---------------------------------------------------------------------------

.. automodule:: message_ix_models.tools.costs.regional_differentiation
   :members:

   .. autosummary::

      get_weo_data
      get_intratec_data
      get_raw_technology_mapping
      subset_module_map
      adjust_technology_mapping
      get_weo_regional_differentiation
      get_intratec_regional_differentiation
      apply_regional_differentiation
