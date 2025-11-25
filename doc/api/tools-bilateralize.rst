.. currentmodule:: mesage_ix_models.tools.bilateralize
Bilateralize (:mod:`.tools.bilateralize`)
#########################################

.. contents::
  :local:
  
Overview
========
This documentation will outline how to bilateralize trade flows in MESSAGEix.

By default, trade flows are based on a “global pool” approach, wherein all 
exporters export energy/commodities to a pool from which importers import based 
on regionally resolved demands.

By “bilateralizing” trade, we specify exporter and importer flows and can therefore 
represent trade flows as networks.

The bilateralization tool, **bilateralize**, is generalized for any traded commodity, 
whether that is a fuel (e.g., LNG), or a material (e.g., steel). 
It also explicitly represents bilateral trade “flows”, or how a fuel/commodity is 
transported from exporter to importer. 
These flow technologies are user defined and flexible; the most common are pipelines 
(e.g., gas pipelines), maritime shipping (e.g., LNG tanker), and transmission lines.

The ``bilateralize`` tool is a Python script that can be used to bilateralize trade flows 
in MESSAGEix. It is located in the ``message-ix-models/tools/bilateralize`` directory.

The tool follows the following steps, which are also available in ``tool/bilateralize/workflow.py``:

Step 1 | Edit (``tools/bilateralize/prepare_edit.py``)
=============
The first step is to generate empty (or default valued) parameters that are required for 
bilateralization, specified by commodity. This step requires updates to a configuration file 
(``config.yaml``) that should be housed in a project directory 
(e.g., ``message-ix-models/projects/newpathways-trade/config.yaml``). A template configuration 
file is provided at ``message-ix-models/data/bilateralize/configs/base_config.yaml``. 

Once the configuration is updated, the user can run 
``message_ix_models.tools.bilateralize.prepare_edit.generate_edit_files(log, project_name, config_name, message_regions)`` 
to produce empty (or default valued) parameters as CSV files. 
These CSV files will populate in ``message-ix-models/data/[your_trade_commodity]/edit_files``. 

The tools may stop if the user specifies in their config that they want to specify a trade network 
(i.e., specify which regions can trade with regions). In this case, a file called ``specify_trade_network.csv`` 
will appear in ``message-ix-models/data/bilateralize/[your_trade_commodity]/speciy_network_[your_trade_commodity].csv``.

Additional functions used here include:
  - ``message_ix_models.tools.bilateralize.calculate_distance()``: 
  Calculates the great-circle distance between regions (TODO: update this to use explicit maritime routes)
  - ``message_ix_models.tools.bilateralize.historical_calibration.build_historical_price()``: 
  Builds historical price dataframes
  - ``message_ix_models.tools.bilateralize.mariteam_calibration.calibrate_mariteam()``: 
  Calibrates maritime shipping (flow technologies) using MariTEAM output.
  - ``message_ix_models.tools.bilateralize.pull_gem.import_gem()``: 
  Imports pre-downloaded raw data from the Global Energy Monitor which is used to calibrate 
  the flow technology piped oil and gas

**This step is not necessary for the following commodities 
(they are already defined in ``scenario_parameters.pkl`` in 
``message-ix-models/data/bilateralize/configs/``):**
  - Biomass (``biomass_shipped``)
  - Coal (``coal_shipped``)
  - Crude Oil (``crude_shipped`` and ``crude_piped``): Note that the global pool version of 
  MESSAGEix names this ``oil_exp`` and ``oil_imp`` 
  and combines shipped and piped trade.
  - Ethanol (``eth_shipped``)
  - Fuel Oil (``foil_shipped`` and ``foil_piped``): Note that this uses the same oil pipeline 
  infrastructure as crude oil and light oil.
  - Light Oil (``loil_shipped`` and ``loil_piped``): See note above on pipelines
  - Liquid H2 (``lh2_shipped``)
  - LNG (``LNG_shipped``)
  - Methanol (``meth_shipped``)
  - Piped gas (``gas_piped``)

Step 2 | Bare (``tools/bilateralize/bare_to_scenario``)
==============
The second step is to review, edit, and transfer the files 
in ``message-ix-models/data/bilateralize/[your_trade_commodity]/edit_files/``.

The user should then review all files in ``message-ix-models/data/bilateralize/[your_trade_commodity]/edit_files/`` 
and transfer these files to ``message-ix-models/data/bilateralize/[your_trade_commodity]/bare_files/``. 

By default, the following parameters are transfered automatically with default values from 
``message-ix-models/data/bilateralize/[your_trade_commodity]/edit_files/`` to 
``message-ix-models/data/bilateralize/[your_trade_commodity]/bare_files/``, to ensure that a scenario 
can be built (even if that scenario doesn't make a ton of sense).

- For **trade technologies** (i.e., whether or not/how much a commodity is traded):
   - ``capacity_factor``
   - ``input``
   - ``output``
   - ``technical_lifetime``
- For **flow technologies** (i.e., how a commodity is transported when traded, 
such as via pipelines or maritime shipping)
   - ``capacity_factor``
   - ``input``
   - ``output``
   - ``relation_activity_flow`` (this links the flow technology to the trade technology)
   - ``technical_lifetime``

The user can call ``message_ix_models.tools.bilateralize.bilateralize.build_parameter_sheets(log)`` to 
pull parameters from ``message-ix-models/data/bilateralize/[your_trade_commodity]/bare_files/`` 
into a dictionary of parameter dataframes that will be used to build a scenario. 
Note that this function pulls from ``bare_files`` and not ``edit_files``, so the user should ensure that 
the right files are transferred in the previous step.

Additional functions here include:
  - ``message_ix_models.tools.bilateralize.historical_calibration.build_hist_new_capacity_flow(message_regions)``: 
  Builds new capacity dataframes for historical activity of flow technologies (e.g., pipelines)
  - ``message_ix_models.tools.bilateralize.historical_calibration.build_hist_new_capacity_trade(message_regions)``: 
  Builds new capacity dataframes for historical activity of trade technologies (e.g., piped gas)
  - ``message_ix_models.tools.bilateralize.historical_calibration.build_historical_activity(message_regions)``: 
  Builds historical activity dataframes

Step 3 | Build (``tools/bilateralize/load_and_solve.py``)
===============
This step builds a scenario. 

The user should use the function 
``message_ix_models.tools.bilateralize.load_and_solve.load_and_solve()``. 
This will pull the base model/scenario, clone it, remove specified trade technologies, 
add them back as bilateralized versions, and export to a GDX file (if specifed- the default is to not export) 
and/or solve the scenario (default is to solve). Note that exporting to GDX means that it is not stored in the ixmp database.
This will also optionally solve the scenario.

Reporting
=========
This code is in progress.

To add a new commodity
======================
To add a new commodity:

- Create a new configuration file for the commodity in ``data/bilateralize/configs/`` 
(see examples from existing configurations or use ``template.yaml``)

- Add the commodity to the ``base_config.yaml`` under ``covered_trade_technologies``

- Run the workflow from ``prepare_edit``, then ``bare_to_scenario``, then ``load_and_solve``

Data
====
All raw data required for a MESSAGEix-GLOBIOM update are currently stored in ``P:/ene_model/MESSAGE_trade``.
