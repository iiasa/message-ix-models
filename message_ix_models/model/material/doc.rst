Materials accounting
********************

.. warning::

   :mod:`.material` is **under development**.

   For details, see the
   `materials <https://github.com/iiasa/message_data/labels/materials>`_ label and
   `current tracking issue (#248) <https://github.com/iiasa/message_data/issues/248>`_.

This module adds (life-cycle) accounting of materials associated with technologies and demands in MESSAGEix-GLOBIOM.

The implementation currently supports four key energy/emission-intensive material industries: Steel, Aluminum, Cement, and Petrochemical.
The petrochemical sector will soon expand to cover plastic production processes, and ammonia and nitrogen-based fertilizer process will be added too.

.. contents::
   :local:

Code reference
==============

.. currentmodule:: message_data.model.material

.. automodule:: message_data.model.material
  :members:

.. automodule:: message_data.model.material.data
  :members:

.. automodule:: message_data.model.material.util
  :members:

Data preparation
################

These modules do the necessary parametrization for each sector, which can be turned on and off individually under `DATA_FUNCTIONS` in `__init__.py`.
For example, the buildings module (`data_buildings.py`) is only used when the buildings model outputs are given explicitly without linking the CHILLED/STURM model through a soft link.

.. automodule:: message_data.model.material.data_aluminum
  :members:

.. automodule:: message_data.model.material.data_steel
  :members:

.. automodule:: message_data.model.material.data_cement
  :members:

.. automodule:: message_data.model.material.data_petro
  :members:

.. automodule:: message_data.model.material.data_power_sector
  :members:

.. automodule:: message_data.model.material.data_buildings
  :members:

.. automodule:: message_data.model.material.data_generic
  :members:


CLI usage
=========

Use ``mix-models materials build`` to add the material implementation on top of existing standard global (R12) scenarios, also giving the base scenario and indicating the relevant data location, e.g.::

    $ mix-models \
      --url="ixmp://ixmp_dev/MESSAGEix-GLOBIOM_R12_CHN/baseline_new_macro#8" \
      --local-data "./data" material build

Currently, a set of given base scenario names will be translated to own scenario names.
The output scenario will be ``ixmp://ixmp_dev-ixmp/MESSAGEix-Materials/{name}``, where {name} is a shortened version of the input scenario name.
Using an additional tag `--tag` can be used to add a suffix to the new scenario name.
This command line only builds the scenario but does not run it. To run the scenario, use ``mix-models materials solve``, giving the scenario name, e.g.::

    $ mix-models material solve --scenario_name NoPolicy_R12




Data, metadata, and configuration
=================================

Binary/raw data files
---------------------

The code relies on the following input files, stored in :file:`data/material/`:

:file:`CEMENT.BvR2010.xlsx`
   Historical cement demand data

:file:`STEEL_database_2012.xlsx`
  Historical steel demand data

:file:`demand_aluminum.xlsx`
  Historical aluminum demand data

:file:`demand_aluminum.xlsx`
  Historical aluminum demand data

:file:`aluminum_techno_economic.xlsx`
  Techno-economic parametrization data for aluminum sector

:file:`Global_steel_cement_MESSAGE.xlsx`
  Techno-economic parametrization data for steel and cement sector combined (R12)

:file:`China_steel_cement_MESSAGE.xlsx`
  Techno-economic parametrization data for steel and cement sector combined (China standalone)

:file:`generic_furnace_boiler_techno_economic.xlsx`
  Techno-economic parametrization data for generic furnace technologies

:file:`LED_LED_report_IAMC*.csv`
  Output from buildings model on the sector's energy/material/floor space demand. It was used for ALPS2020 report, when the linkage to the buildings model is not yet set up.

:file:`MESSAGE_region_mapping_R14.xlsx`
  MESSAGE region mapping used for fertilizer trade mapping

:file:`iamc_db ENGAGE baseline GDP PPP.xlsx`
  SSP GDP projection used for material demand projections

:file:`Ammonia feedstock share.Global.xlsx`
   Feedstock shares (gas/oil/coal) for NH3 production for MESSAGE R11 regions.

:file:`CD-Links SSP2 N-fertilizer demand.Global.xlsx`
   N-fertilizer demand time series from SSP2 scenarios (NPi2020_1000, NPi2020_400, NoPolicy) for MESSAGE R11 regions.

:file:`N fertil trade - FAOSTAT_data_9-25-2019.csv`
   Raw data from FAO used to generate the two :file:`trade.FAO.*.csv` files below.
   Exported from `FAOSTAT <www.fao.org/faostat/en/>`_.

:file:`n-fertilizer_techno-economic.xlsx`
   Techno-economic parameters from literature for various NH3 production technologies (used as direct inputs for MESSAGE parameters).

:file:`trade.FAO.R11.csv`
   Historical N-fertilizer trade records among R11 regions, extracted from FAO database.

:file:`trade.FAO.R14.csv`
   Historical N-fertilizer trade records among R14 regions, extracted from FAO database.

:file:`NTNU_LCA_coefficients.xlsx`
   Material intensity (and other) and other coefficients for power plants based on lifecycle assessment (LCA) data from the THEMIS database, compiled in the `ADVANCE project` <http://www.fp7-advance.eu/?q=content/environmental-impacts-module>`_.

:file:`MESSAGE_global_model_technologies.xlsx`
   Technology list of global MESSAGEix-GLOBIOM model with mapping to LCA technology dataset.

:file:`LCA_region_mapping.xlsx`
   Region mapping of global 11-regional MESSAGEix-GLOBIOM model to regions of THEMIS LCA dataset.

:file:`LCA_commodity_mapping.xlsx`
   Commodity mapping (for materials) of global 11-regional MESSAGEix-GLOBIOM model to commodities of THEMIS LCA dataset.

:file:`material/set.yaml`
----------------------------

.. literalinclude:: ../../../data/material/set.yaml
   :language: yaml

R code and dependencies
-----------------------

:file:`ADVANCE_lca_coefficients_embedded.R`
The code processing the material intensity coefficients of power plants is written in R and integrated into the Python workflow via the Python package `rpy2`.
R code is called from the Python data module `data_power_sector.py`.
Depending on the local R installation(s), the environment variables `R_HOME` and `R_USER` may need to be set for the installation to work (see `stackoverflow <https://stackoverflow.com/questions/12698877/how-to-setup-environment-variable-r-user-to-use-rpy2-in-python>`_).
Additional dependencies include the R packages `dplyr`, `tidyr`, `readxl` and `imputeTS` that need to be installed in the R environment.
