MESSAGEix-Materials
********************

.. warning::

   :mod:`.material` is **under development**.

   For details, see the
   `materials <https://github.com/iiasa/message_data/labels/materials>`_ label and
   `current tracking issue (#248) <https://github.com/iiasa/message_data/issues/248>`_.

Description
===========

This module adds (life-cycle) accounting of materials associated with technologies and demands in MESSAGEix-GLOBIOM.

The implementation currently supports four key energy/emission-intensive material industries: Steel, Aluminum, Cement, and Petrochemical.
The petrochemical sector will soon expand to cover production processes of plastics, ammonia and nitrogen-based fertilizers.

The technologies to represent for the primary production processes of the materials are chosen based on their emission mitigation potential and the degree
of commercialization.

Processing secondary materials
------------------------------

After the primary production stages of the materials, finishing and manufacturing processes are carried out which results in a complete product.
For metals, during the manufacturing process new scrap is formed as residue. This type of scrap requires less preparation before recycling and has a higher quality
as it is the direct product of the manufacturing unlike the old scrap which is formed at the end of the life cycle of a product.
The percentage of the new scrap is an exogenous fixed ratio in the model.

The products that are produced are used in different end-use sectors as stocks and therefore they are not immediately available for recycling until the end of their lifetime.
In the model, each year only certain quantity of products are available for recycling and this ratio is exogenously determined based on historical values.
The end-of-life products coming from buildings and power sector can be endogenously determined in case the relevant links are turned on.

Modeling recycling decisions
----------------------------

In the model, there is a minimum recycling rate specified for different materials and it is based on the historical recycling rates. This parameter can also be used to represent
regulations in different regions. In the end recycling rate is a model decision which can be higher than the minimum rate depending on the economic attractiveness.

The end-of-life products that are collected as old scrap are classified in three different grade/quality. This reflects the degree of difficulty of recycling process in terms of
labor and energy. Different initial designs and final use conditions determine the ease of recycling which is also reflected in costs.
The availability of different scrap is set with 1-2-1 ratio as default for high, medium and low scrap quality.

Three different scrap preparation technologies have different variable costs and energy inputs to process the old scraps with different grades. At the end of the preparation process
these scraps are returned as new scrap all with the same quality. The quality differences in the end product are neglected. All of the old scrap that is collected is used in the model
assuming scrap availability and collection are the main bottlenecks of the recycling process. All the scraps are sent to a secondary melter where they are turned into final materials.
During this process there are also recycling losses.

.. contents::
   :local:

Code reference
==============

.. automodule:: message_data.model.material
  :members:

.. automodule:: message_data.model.material.data
  :members:

.. automodule:: message_data.model.material.util
  :members:

Data preparation
----------------

These modules are not necessary for the parametrization for each sector and they can be turned on and off individually under `DATA_FUNCTIONS` in `__init__.py`.
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

Build and Solve the model from CLI
==================================

Use ``mix-models materials build`` to add the material implementation on top of existing standard global (R12) scenarios, also giving the base scenario and indicating the relevant data location, e.g.::

    $ mix-models \
      --url="ixmp://ixmp_dev/MESSAGEix-GLOBIOM_R12_CHN/baseline_new_macro#8" \
      --local-data "./data" material build

It can be helpful to note that this command will not work for all R12 scenarios because of dependencies on certain levels or commodities described in :file:`set.yaml`.
Currently, a set of pre-defined base scenario names will be translated to own scenario names. But when an unknown base scenario name is given, we reuse it for the output scenario name.
The output scenario will be ``ixmp://ixmp_dev-ixmp/MESSAGEix-Materials/{name}``, where {name} is the output scenario name.

An additional tag `--tag` can be used to add an additional suffix to the new scenario name.
The mode option `--mode` has two different inputs 'by_url' (by default) or 'by_copy'. The first one uses the provided url to add the materials implementation on top of the scenario from the url.
The latter is used to create a 2 degree mitigation scenario with materials by copying relevant carbon prices to the scenario that is specified by `--scenario_name`.

    $ mix-models material build --tag test --mode by_copy --scenario_name NoPolicy_R12

This command line only builds the scenario but does not run it. To run the scenario, use ``mix-models materials solve``, giving the scenario name, e.g.::

    $ mix-models material solve --scenario_name NoPolicy_R12

The solve command has the `--add_calibration` option to add MACRO calibration to a baseline scenario. `--add_macro` option solves the scenario with MACRO.

Reporting
=========

The reporting of the scenarios that include materials representation mainly involves
two steps. In the first step the material specific variables are generated.
At the end of this step all the variables are uploaded as ixmp
timeseries objects to the scenario. The reporting file is generated under
message_data\\reporting\\materials with the name “New_Reporting_MESSAGEix-Materials_scenario_name.xls”.
In the second step, rest of the default reporting variables are obtained by running
the general reporting code. This step combines all the variables that were uploaded
as timeseries to the scenario together with the generic IAMC variables. It also
correctly reports the aggregate variables such as Final Energy and Emissions.
The reporting is executed by the following command:

$ mix-models material report --model_name MESSAGEix-Materials --scenario_name xxxx

If the model is ran with other end-use modules such as buildings/appliances or
transport, the new reporting variables from these should be uploaded to the scenario
as timeseries object before running the above command.

It is possible to add reporting variables from the Buildings model results by using
the following command:
$ mix-models material add_buildings_ts --model_name xxxx --scenario_name xxxx
The reporting output files in csv form should be located under
message_data\\data\\material\\buildings.

There should be no other existing timeseries (other than the ones from the end-use modules)
in the scenario when running the reporting command to obtain correct results.

To remove any existing timeseries in the scenario the following command can be used:
$ mix-models material report --model_name MESSAGEix-Materials --scenario_name xxxx --remove_ts True

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
=======================

:file:`ADVANCE_lca_coefficients_embedded.R`
The code processing the material intensity coefficients of power plants is written in R and integrated into the Python workflow via the Python package `rpy2`.
R code is called from the Python data module `data_power_sector.py`.
Depending on the local R installation(s), the environment variables `R_HOME` and `R_USER` may need to be set for the installation to work (see `stackoverflow <https://stackoverflow.com/questions/12698877/how-to-setup-environment-variable-r-user-to-use-rpy2-in-python>`_).
Additional dependencies include the R packages `dplyr`, `tidyr`, `readxl` and `imputeTS` that need to be installed in the R environment.
