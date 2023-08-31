LEAP-RE NEST implementation of a national model
***********************************************

Project organization
====================

- https://www.leap-re.eu/re4afagri/
- Duration 2021-05 to 2024-03
- Modeling work is divided into 4 parts across the consortium partners. The ECE Program is involved with the development of the MLED model and NEST, the latter using the MESSAGEix framework.
- `Comprehensive Project documentation <https://re4afagri-platform-docs.readthedocs.io/en/latest/>`_
- `p:LEAP_RE label <https://github.com/iiasa/message_data/issues?q=label:p:LEAP_RE>`_ on GitHub.

.. contents::
   :local:

Task 12.2 Integration of Modelling Infrastructure
-------------------------------------------------

Project Lead:
  Giacomo Falchetta
Lead Modeler:
  Adriano Vinca
Technical Advice:
  Muhammad Awais

- Build a country (Zambia) national model, using the downscaling prototype​
- Set right setup for parameters that will not be edited later, e.g. resources​
- Add sub-annual time steps​
- Energy demand from MLED (will require the basin structure, either add only the nodes, or do after adding the water module). Not all demand will be edited​
- Rural energy supply options (ONSET connection: solar, small generation, minigrid, connection to main grid​
- Water module (Need clean PR to be finalized)​
- Crops, connect to Watercrop

Material and code
=================

This folder contains scripts and data processing tools for developing national models for climate-energy-land-water analysis. Particulat features are the sub-national spatial disaggregation and the sub-annual time resolution.

.. contents::
   :local:
   :backlinks: none

Folders
=======

:file:`data/projects/leap_re_nest`
   TODO
:file:`message_data/projects/leap_re_nest/script/`
   Contains the script to build scenarios, run the model and post-processing.

Usage
=====

Make sure *ixmp* amd *message_ix* packages are cloned and *ixmp* is installed.

**Different required steps rely on external repositories (waiting for PRs)**


1) Country model prototype
--------------------------

Clone the repository `Github branch <https://github.com/iiasa/message_single_country/tree/message_zmb>`_ 
run the script run :file:`interface_standalone.py`.


2) Scenario generation and run
------------------------------

In :file:`/projects/leap_re_nest/script/`:

1. :file:`1_adding_time_steps.py` this file take a scenario from the ENGAGE family, clones it, and calibrates it with MACRO.
   The output will have the scenario name ``baseline_calibrated``.

   - Make sure the model and scenario names are those desired.

2. :file:`2_fix_units.py` this script takes the baseline calibrated scenario adjust units for few reported variables and add historical data from original ENGAGE scenario.

   - Make sure the ENGAGE scenario for historical data is the same as in step 1.

3. :file:`3_scenario_editor_R11_markers.py` this script generates, calibrates and run the post-processing for four different COVID-19 scenarios with a fixed GDP trajectory.

4. [not necessary for the following steps] In case interested in running scenarios that explore GDP uncertainty, run :file:`4. GDP_sensitivity.py`.

The scripts :file:`scenario_editor_functions.py` and :file:`adjust_growth_rates.py` contain functions used in 1-4, do not need to be run separately.
