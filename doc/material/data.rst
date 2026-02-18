Input data for MESSAGEix-Materials
**********************************

This page describes the structure and format of inputs required for building MESSAGEix-Materials.

.. contents::
   :local:

Binary/raw data files
---------------------

The code relies on the following input files, stored in :file:`data/material/`:

**Cement**

:file:`CEMENT.BvR2010.xlsx`
   Historical cement demand data

:file:`Global_cement_MESSAGE.xlsx`
  Techno-economic parametrization data for cement sector combined (R12)

:file:`demand_cement.yaml`
  Base year demand data

**Aluminum**

:file:`demand_aluminum.xlsx`
  Historical aluminum demand data

:file:`aluminum_trade_data.csv`
  Data retrieved from IAI MFA model and used for trade calibration

:file:`aluminum_techno_economic.xlsx`
  Techno-economic parametrization data for aluminum sector

:file:`demand_aluminum.yaml`
  Base year aluminum demand data

**Iron and Steel**

:file:`STEEL_database_2012.xlsx`
  Historical steel demand data

:file:`Global_steel_MESSAGE.xlsx`
  Techno-economic parametrization data for steel sector combined (R12)

:file:`worldsteel_steel_trade.xlsx`
  Historical data from `worldsteel Association <https://worldsteel.org/data)>`_

:file:`demand_steel.yaml`
  Base year steel demand data

**Chemicals**

:file:`nh3_fertilizer_demand.xlsx`
  Nitrogen fertilizer demand

:file:`fert_techno_economic.xlsx`
  Techno-economic parameters for NH3 production technologies

:file:`cost_conv_nh3.xlsx`
  Regional cost convergence settings for NH3 production technologies over time

:file:`methanol demand.xlsx`
  Methanol demand

:file:`methanol_sensitivity_pars.xlsx`
  Methanol sensitivity parameters

:file:`methanol_techno_economic.xlsx`
  Techno-economic parameters for methanol production technologies

:file:`petrochemicals_techno_economic.xls`
  Techno-economic parameters for refinery and high-value chemicals production technologies

:file:`steam_cracking_hist_act.csv`
  Steam cracker historical activities in R12 regions

:file:`steam_cracking_hist_new_cap.csv`
  Steam cracker historical capacities in R12 regions

:file:`ammonia/demand_NH3.yaml`
   Ammonia demand in each R12 region in year 2020

:file:`petrochemicals/demand_HVC.yaml`
   HVC demand in each R12 region in year 2020

:file:`methanol/demand_methanol.yaml`
   Methanol demand in each R12 region in year 2020

:file:`/methanol/results_material_SHAPE_comm.csv`
   Commercial sector data from MESSAGEix-Buidings SHAPE scenario used to get wood demands from buildings to estimate resin demands

:file:`/methanol/results_material_SHAPE_resid.csv`
   Residential sector data from MESSAGEix-Buidings SHAPE scenario used to get wood demands from buildings to estimate resin demands

:file:`/methanol/results_material_SSP2_comm.csv`
   Commercial sector data from MESSAGEix-Buidings SSP2 scenario used to get wood demands from buildings to estimate resin demands

:file:`/methanol/results_material_SSP2_resid.csv`
   Residential sector data from MESSAGEix-Buidings SSP2 scenario used to get wood demands from buildings to estimate resin demands

**Power sector**

:file:`NTNU_LCA_coefficients.xlsx`
   Material intensity (and other) coefficients for power plants based on lifecycle assessment (LCA) data from the THEMIS database, compiled in the `ADVANCE project <http://www.fp7-advance.eu/?q=content/environmental-impacts-module>`_.

:file:`MESSAGE_global_model_technologies.xlsx`
   Technology list of global MESSAGEix-GLOBIOM model with mapping to LCA technology dataset.

:file:`LCA_region_mapping.xlsx`
   Region mapping of global 11-regional MESSAGEix-GLOBIOM model to regions of THEMIS LCA dataset.

:file:`LCA_commodity_mapping.xlsx`
   Commodity mapping (for materials) of global 11-regional MESSAGEix-GLOBIOM model to commodities of THEMIS LCA dataset.


**Buildings**

:file:`buildings/LED_LED_report_IAMC_sensitivity_R12.csv`
   Data from MESSAGEix-Buidings LED scenarios used to get steel/cement/aluminum demands from buildings

:file:`buildings/report_IRP_SSP2_BL_comm_R12.csv`
   Commerical sector data from MESSAGEix-Buidings used to get steel/cement/aluminum demands from buildings

:file:`buildings/report_IRP_SSP2_BL_resid_R12.csv`
   Residential sector data from MESSAGEix-Buidings used to get steel/cement/aluminum demands from buildings

**Other**

:file:`generic_furnace_boiler_techno_economic.xlsx`
  Techno-economic parametrization data for generic furnace technologies

:file:`iamc_db ENGAGE baseline GDP PPP.xlsx`
  SSP GDP projection used for material demand projections

:file:`MESSAGEix-Materials_final_energy_industry.xlsx`
  Final energy values to calibrate base year values for industries

:file:`residual_industry_2019.xlsx`
  Final energy values to calculate the residual industry demand

:file:`SSP_UE_dyn_input.xlsx`
   Calibration file for industry end-use energy demands

:file:`SSP_UE_dyn_input_all.xlsx`
   Calibration file for end-use energy demands

:file:`iea_mappings/all_technologies.csv`
   Mapping of MESSAGEix-GLOBIOM technologies to IEA EWEB flows and products

:file:`iea_mappings/chemicals.csv`
   Mapping of MESSAGEix-GLOBIOM industry technologies to IEA EWEB products of chemical sector flows

:file:`iea_mappings/industry.csv`
   Mapping of MESSAGEix-GLOBIOM industry technologies to IEA EWEB products of industry sector flows not covered by MESSAGEix-Materials

:file:`other/mer_to_ppp_default.csv`
   Default conversion factors for GDP MER to PPP used in MESSAGEix-GLOBIOM SSP2 scenarios.
   Used to create demand projections for steel/cement/aluminum/chemicals if GDP_PPP data is not in scenario

Data, metadata, and configuration
---------------------------------

See :doc:`/material/files`.


