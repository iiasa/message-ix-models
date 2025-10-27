GDP–Climate Impacts
===================

.. note:: The functions in this module have two external dependencies not mainetined under ``message-ix-models``, namely ``rime`` and ``climate-processor``.
   The rime package can be downloaded and installed from github https://github.com/iiasa/rime.
   Climate processor (https://github.com/iiasa/climate-processor) is an internal IIASA package that features the open source package
   climate-assessment (https://github.com/iiasa/climate-assessment). 

Overview
--------

The ``gdp-ci`` command group provides a command-line interface (CLI) to run
iterative GDP–climate impact workflows. These workflows combine MESSAGEix
scenario results, the MAGICC climate model, and three climate damage functions 
(Burke et al., 2018, Kotz et al., 2024 and Waidelich et al., 2024) emulated with RIME, to
calculate feedback effects of climate damages on GDP.

.. contents::
   :local:

CLI usage
=========

.. automodule:: message_ix_models.project.GDP_climate_shocks.cli
   :no-members:
   :no-undoc-members:
   :no-show-inheritance:


.. code::

  Usage: mix-models gdp-ci [OPTIONS] COMMAND [ARGS]...

  GDP-Climate Impact iteration workflow.

  Options:
  --help  Show this message and exit.

  Commands:
  run_full         Run the full GDP–Climate Impact iteration workflow.
  run_magicc_rime  Launches the run_magicc_rime function from cli.

Two main workflows are available:

- **Full iteration workflow** (:command:`mix-models gdp-ci run_full`)  
  This workflow:

  1. Runs an initial MESSAGEix scenario and MAGICC simulation.
  2. Applies GDP damages using RIME and re-solves MESSAGE–MACRO iteratively
    until GMT change converges below a threshold.
  3. Produces full reporting using the legacy MESSAGE reporting setup.

- **MAGICC–RIME workflow** (:command:`mix-models gdp-ci run_magicc_rime`)  
  This workflow executes MAGICC and RIME calculations in one of three modes:

  - **Default**: From MESSAGEix scenarios (MAGICC run if no output exists).
  - **Single input file**: From a pre-generated MAGICC input file specified in the config.
  - **List input**: From a list of scenarios in a CSV file.

Commands
--------

``run_full``
~~~~~~~~~~~~

.. autofunction:: message_ix_models.project.GDP_climate_shocks.cli.run_full


``run_magicc_rime``
~~~~~~~~~~~~~~~~~~~

.. autofunction:: message_ix_models.project.GDP_climate_shocks.cli.run_magicc_rime


**Other CLI auxiliary functions**

.. autofunction:: message_ix_models.project.GDP_climate_shocks.cli.run_initial_scenario_if_needed
.. autofunction:: message_ix_models.project.GDP_climate_shocks.cli.iterate_with_climate_impacts
.. autofunction:: message_ix_models.project.GDP_climate_shocks.cli.load_and_override_config
.. autofunction:: message_ix_models.project.GDP_climate_shocks.cli.run_from_single_input
.. autofunction:: message_ix_models.project.GDP_climate_shocks.cli.run_from_input_list
.. autofunction:: message_ix_models.project.GDP_climate_shocks.cli.run_from_messageix_scenarios

Other functions
===============

Functions in gdp_table_out_ISO
------------------------------

Functions to run RIME

.. automodule:: message_ix_models.project.GDP_climate_shocks.gdp_table_out_ISO
   :members:
   :undoc-members:

Functions in call_climate_processor
-----------------------------------

Functions to run MAGICC

.. automodule:: message_ix_models.project.GDP_climate_shocks.call_climate_processor
   :members:
   :undoc-members:

Functions in util
-----------------

Utility functions

.. automodule:: message_ix_models.project.GDP_climate_shocks.util
   :members:
   :undoc-members:

Configuration
-------------

All workflows can be configured using:

- A YAML configuration file (``--config`` argument).  
- CLI options that override config values (e.g., ``--model_name``,
  ``--damage_model``, ``--percentiles``).  

The configuration specifies reference scenarios, SSP, regions, percentiles, and
the path to RIME output.

**Examples**

Run a full GDP–climate impact iteration on a reference scenario:

.. code-block:: bash

   mix-models gdp-ci run_full \
       --model_name ENGAGE_SSP2 \
       --model_name-clone ENGAGE_SSP2_clone \
       --scens_ref Baseline \
       --damage_model Burke \
       --percentiles 50 \
       --ssp SSP2 \
       --regions R12

Run MAGICC and RIME from a pre-generated input file:

.. code-block:: bash

   mix-models gdp-ci run_magicc_rime \
       --config config.yaml \
       --model_name ENGAGE_SSP2 \
       --scens_ref Baseline \
       --damage_model Burke \
       --percentiles 50 \
       --input_only single

