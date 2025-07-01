MESSAGEix-Materials (:mod:`.model.material`)
********************************************

Description
===========

This module adds material stock and flow accounting in MESSAGEix-GLOBIOM. The implementation currently includes four key energy/emission-intensive material industries: Iron&Steel, Aluminum, Cement, and Chemicals.

.. contents::
   :local:

Code reference
==============

.. automodule:: message_ix_models.model.material
  :members:

.. automodule:: message_ix_models.data.material
  :members:

.. note::
   See also :pull:`130`/the archived branch `materials-migrate <https://github.com/iiasa/message-ix-models/tree/migrate-materials>`_ for a distinct version of :mod:`.material`.
   That earlier PR was superseded by :pull:`188`, but contains the 1.0.0 version of MESSAGEix-Materials, which was used for the first submission of :cite:`unlu_2024_materials`. The model structure is almost identical to the default model that was added by :pull:`188`.
   Compared to :pull:`188` this version differs particularly in the following areas:

   - Older base year calibration of "other industries" using outdated IEA EWEB data.
   - Material demands computed in R through ``rpy2``, instead of Python implementation.
   - Less accurate regional allocation/aggregation of base year demands for cement and steel.
   - No use of :mod:`.tools.costs`.

Data preparation
----------------

These scripts are used to prepare and read the data into the model.
They can be turned on and off individually under ``DATA_FUNCTIONS`` in :mod:`__init__.py`.

.. automodule:: message_ix_models.model.material.data_aluminum
  :members:

.. automodule:: message_ix_models.model.material.data_steel
  :members:

.. automodule:: message_ix_models.model.material.data_cement
  :members:

.. automodule:: message_ix_models.model.material.data_petro
  :members:

.. automodule:: message_ix_models.model.material.data_power_sector
  :members:

.. automodule:: message_ix_models.model.material.data_generic
  :members:

.. automodule:: message_ix_models.model.material.data_ammonia_new
  :members:

.. automodule:: message_ix_models.model.material.data_methanol
  :members:

Build and solve the model from CLI
==================================

Note: To include material stocks from power sector message_ix should be the following
version from source:
https://github.com/iiasa/message_ix/tree/material_stock

Use ``mix-models materials-ix {SSP} build`` to add the material implementation on top of existing standard global (R12) scenarios, also giving the base scenario and indicating the relevant data location, e.g.::

    mix-models \
        --url="ixmp://ixmp_dev/MESSAGEix-GLOBIOM 1.1-R12/baseline_DEFAULT#21" \
        --local-data "./data" material-ix SSP2 build --tag test --nodes R12

The output scenario name will be baseline_DEFAULT_test. An additional tag ``--tag`` can be used to add an additional suffix to the new scenario name.
The mode option ``--mode`` has two different inputs 'by_url' (by default) or 'by_copy'.
The first one uses the provided ``--url`` to add the materials implementation on top of the scenario from the url.
This is the default option. The latter is used to create a 2 degree mitigation scenario with materials by copying carbon prices to the scenario that is specified by ``--scenario_name``::

    mix-models --url="ixmp://ixmp_dev/MESSAGEix-Materials/scenario_name" material-ix \
     build --tag test --mode by_copy

This command line only builds the scenario but does not solve it. To solve the scenario, use ``mix-models materials-ix solve``, giving the scenario name, e.g.::

    mix-models --url="ixmp://ixmp_dev/MESSAGEix-Materials/scenario_name" material-ix \
     SSP2 solve --add_calibration False --add_macro False

The solve command has the ``--add_calibration`` option to add MACRO calibration to a baseline scenario with a valid calibration file specified with ``--macro-file``.
The ``--add_macro`` option determines whether the scenario should be solved with MESSAGE or MESSAGE-MACRO.
MESSAGEix-Materials provides one calibration file that is only compatible with scenarios with first model year 2025 and the common model structure of a MESSAGEix-GLOBIOM scenario.
To first calibrate the scenario and then solve that scenario with MACRO both options should be set to :any`True`.

It is also possible to shift the first model year and solve a clone with shifted years with ``--shift_model_year``.
If ``--shift_model_year`` is set together with the macro options the model year will be shifted before the MACRO calibration.

All three options are :any:`False` by default.

Reporting
=========

The reporting generates specific variables related to materials, mainly Production and Final Energy.
The resulting reporting file is generated under :file:`message_ix_models/data/material/reporting_output` with the name “New_Reporting_MESSAGEix-Materials_scenario_name.xlsx”.
More detailed variables related to the whole energy system and emissions are not included in this reporting.

Reporting is executed by the following command::

    mix-models --url="ixmp://ixmp_dev/MESSAGEix-Materials/scenario_name" \
        --local-data "./data" material-ix SSP2 report

To remove any existing timeseries in the scenario the following command can be used::

    mix-models --url="ixmp://ixmp_dev/MESSAGEix-Materials/scenario_name" material-ix \
        SSP2 report --remove_ts True

Data, metadata, and configuration
=================================

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

:file:`material/set.yaml`
-------------------------

.. literalinclude:: ../../message_ix_models/data/material/set.yaml
   :language: yaml

Release notes
=============

This is the list of changes to MESSAGEix-Materials between each release.

.. toctree::
   :maxdepth: 2

   v1.1.0
   v1.2.0
