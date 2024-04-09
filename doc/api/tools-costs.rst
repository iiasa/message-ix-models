.. currentmodule:: message_ix_models.tools.costs
.. _tools-costs:

Technoeconomic investment and fixed O&M costs projection (:mod:`.tools.costs`)
******************************************************************************

:mod:`.tools.costs` is a tool that projects the investment costs and fixed operational and maintenance costs of technologies in MESSAGEix until the year 2100.
The tool is able to project costs for different regions, technologies, and scenarios. The costs are projected based on historical (mostly a base year) data and assumptions about future cost reductions.

Future costs in reference region
================================

The costs in the reference region are projected based on the following assumption: given a cost reduction rate, the cost of the technology in the reference region experiences an exponential decay over time.

Future costs in non-reference regions
=====================================

The costs for each technology in all non-reference regions can be calculated in one of three ways:

1. Constant cost reduction rate (called `constant`): the regional cost ratio that is calculated in the base year is kept constant and used to project regionally-differentiated costs across all years.
2. Convergence to reference region costs by a certain year (called `convergence`): all other regions' costs exponentially decay until they become they same as the reference region's cost by a specified year.
3. GDP-adjusted cost reduction rate (called `gdp`): this method assumes that regional costs converge not based on a specified year but based on GDP per capita. In this case, all non-reference regions' costs are adjusted based on the ratio of the GDP per capita of the region to the GDP per capita of the reference region.

Modules
=======

Within the context of the tool, modules are defined as subsets of technologies.
Currently two modules are available:
- `energy`: mosty power technologies, as well as a few other supply-side technologies
- `materials`: technologies relevant to the materials and industry sectors

Consider the `energy` module as sort of the base module, as it contains the most technologies.

To add a new module, the following steps are required:

- Add the relevant data to the `data` directory, under the `costs` subdirectory. Create another folder with the name of the new module. The following files are needed:

   - `first_year_[module].csv`: a file with a list of technologies and the corresponding first year that the respective technology can start being deployed/modeled. The file should have the following columns:

     - `message_technology`: the technology name
     - `first_year_original`: the first year the technology can start being deployed

   - `tech_map_[module].csv`: a file with the mapping of technologies to a base year cost source. The file should have the following columns:

     - `message_technology`: the technology name
     - `reg_diff_source` and `reg_diff_technology`: the source data for the regional differentiation of costs and the corresponding technology to map to. If `reg_diff_source` is `energy`, then `reg_diff_technology` should be a technology that is present in the `energy` module. If `reg_diff_source` is `weo`, then `reg_diff_technology` should be a technology that is present in the WEO data (refer to the `tech_map_energy.csv` file for the kinds of WEO technologies available, as all energy technologies are mapped to a WEO technology). You can also add another source of regional differentation (in the case of `materials`, a newly created source called `intratec` is used). However, this method is a little more involved as it would involved changing the code to read in this new source data.
     - `base_year_reference_region_cost`: the base year cost for the technology in the reference region
     - `fix_ratio`: the ratio of fixed O&M costs to investment costs for the technology

- Add the new module to the config file in `tools.costs.config` under the `modules` key.

Please note that the following assumptions are made in technology costs mapping:

* If a technology is mapped to a technology in the `energy` module, then the cost reduction across scenarios is the same as the cost reduction of the mapped technology.
* If a `materials` (or any other non-`energy`) technology is has `reg_diff_source` as `energy` and the `base_year_reference_region_cost` is not empty, then the `base_year_reference_region_cost` that is in `tech_map_materials.csv` is used as the base year cost for the technology in the reference region. If the `base_year_reference_region_cost` is empty, then the cost reduction across scenarios is the same as the cost reduction of the mapped technology.
* If using the `materials` module, if a technology that is specified in `tech_map_materials.csv` already exists in `tech_map_energy.csv`, then the reference region cost is taken from `tech_map_materials.csv`.
* If a technology in a module is not mapped to any source of regional differentation, then no cost reduction over the years is applied to the technology.
* If a technology has a non-empty `base_year_reference_region_cost` but is not mapped to any source of regional differentation, then assume no regional differentiation and use the reference region base year cost as the base year cost for all regions.

Data sources
============

The tool uses the following data sources for the regional differentiation of costs:

* WEO: the World Energy Outlook data from the International Energy Agency (IEA)
* Intratec: the Intratec data, which is a database of production costs for chemicals and other materials

The tool also uses SSP data (called upon by the :mod:`exo_data` module) to adjust the costs of technologies based on GDP per capita.

How to use the tool
===================

:func:`.create_cost_projections` is the top-level entry point.
This function in turns calls the other functions in the module in the correct order, according to settings stored on a :class:`.costs.Config` object.

The inputs for :func:`.create_cost_projections` are:

* Module: the module to use for the cost projections (either `energy` or `materials`). Default is `energy`.
* Method: the method to use for projecting costs in non-reference regions (either `constant`, `convergence`, or `gdp`). Default is `gdp`.
* Node: the regional level (node) to use for the cost projections (either `R11` or `R12`). Default is `R12`.
* Reference region: the reference region to use for the cost projections (by default, NAM is used)
* Scenario: the scenario to use for the cost projections (such as `SSP1`, `SSP2`, `SSP3`, `SSP4`, `SSP5`, or `LED`). By default, `all` is used, which means that the costs are projected for all scenarios.
* Scenario version: the version of the SSP data to use (either `updated` or `original`). Default is `updated`.
* Base year: the base year to use for the cost projections. Default is 2021.
* Convergence year: the year by which the costs in all regions should converge to the reference region costs (if using the `convergence` method). By default, the year 2050 is used.
* FOM rate: the rate at which the fixed O&M rate of a technology increases over time. Default is 0.025.
* Format: the format of the output data (either `message` or `iamc`). Default is `message`.

To use the tool with the default settings, simply create a :class:`.costs.Config` object and call the :func:`.create_cost_projections` function with the :class:`.costs.Config` object as the input:
The output of :func:`.create_cost_projections` is a dictionary with the following keys:

* `inv_cost`: the investment costs of the technologies in each region
* `fix_cost`: the fixed O&M costs of the technologies in each region

An example is::

   from message_ix_models.tools.costs.config import Config
   from message_ix_models.tools.costs.projections import create_cost_projections

   cfg = Config()
   costs = create_cost_projections(cfg)

   costs["inv_cost"]
   costs["fix_cost"]

More examples of how to use the function are given in the `tools/costs/demo.py` file, which also shows how to use settings that are not the default in the `config.py` file.

Code reference
==============

.. autosummary::

   Config
   create_cost_projections

The other submodules implement the supporting methods, calculations, and data handling:

1. :mod:`.tools.costs.regional_differentiation` calculates the regional differentiation of costs for technologies.
2. :mod:`.tools.costs.decay` projects the costs of technologies in a reference region with only a cost reduction rate applied.
3. :mod:`.tools.costs.gdp` adjusts the regional differentiation of costs for technologies based on the GDP per capita of the region.
4. :mod:`.tools.costs.projections` combines all the above steps and returns the projected costs for each technology in each region.

.. automodule:: message_ix_models.tools.costs
   :members:

.. currentmodule:: message_ix_models.tools.costs.decay

Cost reduction of technologies over time (:mod:`.tools.costs.decay`)
--------------------------------------------------------------------

.. automodule:: message_ix_models.tools.costs.decay
   :members:

   .. autosummary::

      get_cost_reduction_data
      get_technology_reduction_scenarios_data
      project_ref_region_inv_costs_using_reduction_rates

.. currentmodule:: message_ix_models.tools.costs.gdp

GDP-adjusted costs and regional differentiation (:mod:`.tools.costs.gdp`)
-------------------------------------------------------------------------

.. automodule:: message_ix_models.tools.costs.gdp
   :members:

   .. autosummary::

      default_ref_region
      process_raw_ssp_data
      adjust_cost_ratios_with_gdp

.. currentmodule:: message_ix_models.tools.costs.projections

Projection of costs given input parameters (:mod:`.tools.costs.projections`)
----------------------------------------------------------------------------

.. automodule:: message_ix_models.tools.costs.projections
   :members:

   .. autosummary::

      create_projections_constant
      create_projections_gdp
      create_projections_converge
      create_message_outputs
      create_iamc_outputs

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
