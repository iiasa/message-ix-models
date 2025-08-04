NEWPATHWAYS-TRADE
#################

- Project lead: :gh-user:`volker-krey`
- Lead modeler: :gh-user:`shepard`


Introduction
************
NEWPATHWAYS-TRADE covers Task 3.5 (Resilience of the Energy System Transformation Towards the Paris Goal) in Work Product 3 (Sector Transitions Including Land and Energy) in the 2024-2027 NEWPATHWAYS project. This task includes the bilateralization of trade networks in MESSAGEix. 

This documentation will outline how to bilateralize trade flows in MESSAGEix. By default, trade flows are based on a “global pool” approach, wherein all exporters export energy/commodities to a pool from which importers import based on regionally resolved demands.

By “bilateralizing” trade, we specify exporter and importer flows and can therefore represent trade flows as networks.

The bilateralization tool, **bilateralize**, is generalized for any traded commodity, whether that is a fuel (e.g., LNG), or a material (e.g., steel). It also explicitly represents bilateral trade “flows”, or how a fuel/commodity is transported from exporter to importer. These flow technologies are user defined and flexible; the most common are pipelines (e.g., gas pipelines), maritime shipping (e.g., LNG tanker), and transmission lines.

Tool
****
The ``bilateralize`` tool is a Python script that can be used to bilateralize trade flows in MESSAGEix. It is located in the ``message-ix-models/tools/bilateralize`` directory.

The tool follows the following steps, which are also available in ``tool/bilateralize/workflow.py``:

1. Edit (``tools/bilateralize/prepare_edit.py``)
=======
The first step is to generate empty (or default valued) parameters that are required for bilateralization, specified by commodity. This step requires updates to a configuration file (``config.yaml``) that should be housed in a project directory (e.g., ``message-ix-models/projects/newpathways-trade/config.yaml``). A template configuration file is provided at ``message-ix-models/data/bilateralize/configs/base_config.yaml``. Once the configuration is updated, the user can run ``message_ix_models.tools.bilateralize.bilateralize.generate_bare_sheets(log, message_regions)`` to produce empty (or default valued) parameters as CSV files. These CSV files will populate in ``message-ix-models/data/[your_trade_commodity]/edit_files``. 

The tools may stop if the user specifies in their config that they want to specify a trade network (i.e., specify which regions can trade with regions). In this case, a file called ``specify_trade_network.csv`` will appear in ``message-ix-models/data/bilateralize/[your_trade_commodity]/speciy_network_[your_trade_commodity].csv``.

Additional functions here include:
  - ``message_ix_models.tools.bilateralize.bilateralize.import_gem(input_file, input_sheet, trade_technology, flow_technology, project_name, config_name)``: This function pulls in pre-downloaded raw data from the Global Energy Monitor in oil and gas infrastructure and can be used to calibrate the flow technology for moving oil and gas via pipeline.
  - ``message_ix_models.tools.bilateralize.bilateralize.calibrate_mariteam(covered_tec, message_regions)``: This function pulls in MariTEAM output to calibrate maritime shipping (flow technologies).

**This step is not necessary for the following commodities (they are already defined):**
  - Biomass (``biomass_shipped``)
  - Coal (``coal_shipped``
  - Crude Oil (``crude_shipped`` and ``crude_piped``): Note that the global pool version of MESSAGEix names this ``oil_exp`` and ``oil_imp`` and combines shipped and piped trade.
  - Ethanol (``eth_shipped``)
  - Fuel Oil (``foil_shipped`` and ``foil_piped``): Note that this uses the same oil pipeline infrastructure as crude oil and light oil.
  - Light Oil (``loil_shipped`` and ``loil_piped``): See note above on pipelines
  - Liquid H2 (``lh2_shipped``)
  - LNG (``LNG_shipped``)
  - Methanol (``meth_shipped``)
  - Piped gas (``gas_piped``)

2. Bare (``tools/bilateralize/bare_to_scenario``)
=======
The second step is to review, edit, and transfer the files in ``message-ix-models/data/bilateralize/[your_trade_commodity]/edit_files/``.

The user should then review all files in ``message-ix-models/data/bilateralize/[your_trade_commodity]/edit_files/`` and transfer these files to ``message-ix-models/data/bilateralize/[your_trade_commodity]/bare_files/``. 

By default, the following parameters are transfered automatically with default values from ``message-ix-models/data/bilateralize/[your_trade_commodity]/edit_files/`` to ``message-ix-models/data/bilateralize/[your_trade_commodity]/bare_files/``, to ensure that a scenario can be built (even if that scenario doesn't make a ton of sense).

- For **trade technologies** (i.e., whether or not/how much a commodity is traded):
   - ``capacity_factor``
   - ``input``
   - ``output``
   - ``technical_lifetime``
- For **flow technologies** (i.e., how a commodity is transported when traded, such as via pipelines or maritime shipping)
   - ``capacity_factor``
   - ``input``
   - ``output``
   - ``relation_activity_flow`` (this links the flow technology to the trade technology)
   - ``technical_lifetime``

The user can call ``message_ix_models.tools.bilateralize.bilateralize.build_parameter_sheets(log)`` to pull parameters from ``message-ix-models/data/bilateralize/[your_trade_commodity]/bare_files/`` into a dictionary of parameter dataframes that will be used to build a scenario. Note that this function pulls from ``bare_files`` and not ``edit_files``, so the user should ensure that the right files are transferred in the previous step.

Additional functions here include:
  - ``message_ix_models.tools.bilateralize.bilateralize.build_historical_activity(message_regions)``: This function pulls raw IEA World Energy Balances/Natural Gas Flow data to build historical activity in the regionality specified.

3. Build (``tools/bilateralize/load_and_solve.py``)
========
This step builds a scenario. 

The user should use the function ``message_ix_models.tools.bilateralize.bilateralize.clone_and_update(trade_dict, log, to_gdx, solve)``. This will pull the base model/scenario, clone it, remove specified trade technologies, add them back as bilateralized versions, and export to a GDX file (if specifed- the default is to not export) and/or solve the scenario (default is to solve). Note that exporting to GDX means that it is not stored in the ixmp database.

4. Solve
========
Solve can be completed using the ``message_ix_models.tools.bilateralize.bilateralize.clone_and_update(trade_dict, log, to_gdx, solve)`` function above. By default scenario will be run. 

5. Report
=========
This code is in progress.

To add a new commodity
***********************
To add a new commodity:

- Create a new configuration file for the commodity in ``data/bilateralize/configs/`` (see examples from existing configurations or use ``template.yaml``)

- Add the commodity to the ``base_config.yaml`` under ``covered_trade_technologies``

- Run the workflow from ``prepare_edit``, then ``bare_to_scenario``, then ``load_and_solve``

Scenario Identifier
*******************
- Model: ``NP-SSP2`` (We are basing this framework on SSP2 by default)
- Scenario (default bilateralization): ``default_bilat``


Data
****
All raw data required are currently stored in ``P:/ene_model/MESSAGE_trade``.
