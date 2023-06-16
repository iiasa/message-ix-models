MESSAGEix-Nexus (:mod:`.model.water`)
*************************************

:mod:`message_ix_models.model.water` adds water usage and demand related representation to the MESSAGEix-GLOBIOM global model.
The resulting model is referred to as **“MESSAGEix-Nexus”**.
This work extends the water sector linkage described by Parkinson et al. (2019) :cite:`parkinson-2019`.

.. contents::
   :local:

CLI usage
=========

Use the :doc:`CLI </cli>` command ``mix-data water`` to invoke the commands defined in :mod:`.water.cli`. Example:
``mix-models --url=ixmp://ixmp_dev/ENGAGE_SSP2_v4.1.7/baseline_clone_test water cooling``
model and scenario specifications can be either set manually in ``cli.py`` or specified in the ``--url`` option

.. code::

   Usage: mix-models water [OPTIONS] COMMAND [ARGS]...

   Options:
   --regions [ISR|R11|R12|R14|R32|RCP|ZMB]
                                    Code list to use for 'node' dimension.
   --help                          Show this message and exit.

   Commands:
   cooling  Build and solve model with new cooling technologies.
   nexus    Add basin structure connected to the energy sector and water...
   report   function to run the water report_full from cli to the scenario...

Country vs Global implementation
--------------------------------

The :mod:`message_ix_models.model.water` is designed to being able to add water components to either a global R11 (or R12) model or any country model designed with `the MESSAGEix single country <https://github.com/iiasa/message_single_country>`_ model prototype.
For any of the region configuration a shapefile is needed to run the pre-processing part, while, once the data is prepared, only a .csv file similar to those in `message_ix_models.data.water.delineation` is needed.

To work with a country model please ensure that:

1. country model and scenario are specified either in ``--url`` or in the ``cli.py`` script
2. the option ``--regions`` is used with the ISO3 code of the country (e.g. for Israel ``--regions=ISR``)
3. Following the Israel example add a 'country'.yaml file in `message_ix_models.data.node` for the specific country
4. Following the Israel example add the country ISO3 code in the 'regions' options in `message_ix_models.utils.click`

Annual vs sub-annual implementation
-----------------------------------

if a sub-annual timestep is defined (e.g. seasons or months), the water module will automatically generate the water system with seasonality, using the `time` set components that is not `year` (assuming that the data has been prepared accordingly).

In the case you want to add seasonality in the water sector to a model with only annual timesteps, the best way is to pre-define the required sets (e.g. `time` and `map_time`).
Then, running the water nexus module will automatically build the sub-annual water module.

In the case there are already multiple sub-annual time-steps levels already defined and not all are relevant to the water module, the components of the set `time` that are of interest for the water module should be manually added as a cli option (e.g. `--time=year` or `--time=[1,2]`)

Code reference
==============

.. currentmodule:: message_ix_models.model.water

.. automodule:: message_ix_models.model.water
   :members:

Build and run
-------------

.. automodule:: message_ix_models.model.water.build
   :members:

Data preparation
----------------

.. automodule:: message_ix_models.model.water.data
   :members:

.. automodule:: message_ix_models.model.water.data.water_for_ppl
   :members:

.. automodule:: message_ix_models.model.water.data.demands
   :members:

.. automodule:: message_ix_models.model.water.data.infrastructure
   :members:

.. automodule:: message_ix_models.model.water.data.water_supply
   :members:

.. automodule:: message_ix_models.model.water.data.irrigation
   :members:

Utilities and CLI
-----------------

.. automodule:: message_ix_models.model.water.utils
   :members:
   :exclude-members: read_config

.. automodule:: message_ix_models.model.water.cli
   :members:

Reporting
---------

.. warning::

   The current reporting features only work for the global model.

.. automodule:: message_ix_models.model.water.reporting
   :members:

Data, metadata, and config files
================================

See also :doc:`/water/files`.

- :file:`data/water/`: contains input data used for building the Nexus module

  - :file:`delineation/`: contains geospatial files for basin mapping and MESSAGE regions. These spatial files are created through intersecting HydroSHEDS basin and the MESSAGE region shapefile. The scripts and processing data at 'P:\ene.model\NEST\delineation'
  - :file:`ppl_cooling_tech/`: contains cooling technology shares, costs and water intensities for different regional definitions
  - :file:`water_demands/`: contains water sectoral demands, connection rates for basins
  - :file:`water_dist/`: contains water infrastructure (distribution, treatment mapping) and historical and projected capacities  of desalination technologies
  - :file:`technology.yaml`: metadata for the 'technology' dimension.
  - :file:`set.yaml`: metadata for other sets.

Pre-processing
==============
- :file:`data/water/`: contains scripts used in pre_processing source data for the water sector implementation


  - :file:`calculate_ppl_cooling_technology_shares.r`: contains script for processing cooling technology shares at global level for different regional specifications.
  - :file:`groundwater_harmonize.r`: contains workflow to calculate historical capacity of renewable groundwater, table depth and energy consumption
  - :file:`generate_water_constraints.r`: contains function to calculate municipal, manufacturing, rural water demands, water access and sanitation rates
  - :file:`desalination.r`: contains script for assessing the historical and possible future desalination capacity of a region or country
  - :file:`hydro_agg_raster.py`: contains workflow for processing the hydrological data in NC4 and adjust the unit conversions, daily to monthly aggregation.
  - :file:`hydro_agg_spatial.R`: contains workflow for spatially aggregating monthly hydrological data onto basin using appropriate raster masking onto shapefiles
  - :file:`hydro_agg_basin.py`: contains workflow for aggregating monthly data to 5 yearly averages using appropriate statistical methods (quantiles, averages etc.).
    It also calculates e flows based on Variable MF method.

Deprecated R Code
=================

- :file:`data/water/deprecated`: contains `R` scripts from the older water sector implementation

  - :file:`Figures.R`: R script for producing figures
  - :file:`cooling_tech_av.R`: contains similar code as in the above-mentioned scripts, but this was originated from another workstream.
  - :file:`add_water_infrastructure.R`: contains spatially-explicit analysis of gridded demands and socioeconomic indicators to develop pathways for sectoral water withdrawals, return flows and infrastructure penetration rates in each MESSAGE region. The pathways feature branching points reflecting a specific water sector development narrative (e.g., convergence towards achieving specific SDG targets).

Reference
=========

.. toctree::
   :maxdepth: 2

   /water/files

.. footbibliography::
