GDP Impacts
***********

Overview
========

The main function is the core of a script designed to analyze the economic and climate impacts of different scenarios using the MESSAGEix framework. It integrates climate modeling (via MAGICC), economic impact assessment (via RIME), and iterative convergence to account for feedback between climate and economic systems. This function is part of a larger workflow that evaluates the effects of climate change on GDP and other economic indicators under various scenarios, damage models, and percentiles.

The script is designed to:

- Initialize and solve baseline scenarios without climate impacts.
- Run the MAGICC climate model to estimate global surface temperature changes.
- Apply economic damage models (via RIME) to assess GDP impacts.
- Iteratively adjust scenarios to account for climate-economic feedback until convergence is achieved.
- Generate detailed reports for each scenario.

Key Components
==============

Scenario Initialization
-----------------------

- The script starts by initializing a reference scenario (``sc_ref``) using the MESSAGEix framework.
- It clones the reference scenario to create a new scenario (``sc0``) without climate impacts, which serves as the baseline for further analysis.

Baseline Scenario Solving
-------------------------

- The baseline scenario (``sc0``) is solved using the MESSAGE or MESSAGE-MACRO model.
- Emissions reporting is run to generate necessary outputs for the climate model.

Climate Modeling with MAGICC
----------------------------

- The MAGICC climate model is invoked to estimate global surface temperature changes for the baseline scenario.
- The output is saved in a specified directory for further use.

Economic Impact Assessment with RIME
------------------------------------

- The RIME model is used to assess the economic impacts of climate change on GDP.
- This step is repeated for each damage model and percentile specified by the user.

Iterative Convergence
---------------------

- The script iteratively adjusts the scenario to account for feedback between climate impacts and economic outcomes.
- Convergence is achieved when the difference in global average temperature between iterations falls below a threshold (0.05°C).

Reporting
---------

- Once convergence is achieved, detailed reports are generated for the final scenario using legacy reporting functions.

Input Parameters
================

The main function accepts the following arguments:

- ``scens_ref``: A list of reference scenarios to analyze. These scenarios serve as the starting point for the analysis.
- ``damage_model``: A list of damage models to apply. These models define how climate impacts affect GDP.
- ``percentiles``: A list of percentiles to evaluate. These represent different levels of uncertainty in climate projections.

Workflow
========

Setup
-----

- Logging and memory usage tracking are initialized.
- The MESSAGEix platform is configured, and the reference scenario is loaded.

Baseline Analysis
-----------------

- A baseline scenario (``sc0``) is created and solved without climate impacts.
- Emissions reporting is run, and the MAGICC model is executed to estimate temperature changes.

Economic Impact Analysis
------------------------

- For each damage model and percentile, the RIME model is run to assess GDP impacts.
- The results are used to adjust the scenario iteratively.

Convergence
-----------

- The script iteratively adjusts the scenario to account for feedback between climate and economic systems.
- Convergence is achieved when the temperature difference between iterations is below 0.05°C.

Final Reporting
--------------

- Once convergence is achieved, detailed reports are generated for the final scenario.

Example Usage
=============

To run the script, use the following command:

.. code-block:: bash

   python script_name.py --scens_ref SSP2_Ref --damage_model model1 model2 --percentiles 50 90

This command will:

- Analyze the ``SSP2_Ref`` scenario.
- Apply two damage models (``model1`` and ``model2``).
- Evaluate the 50th and 90th percentiles of climate uncertainty.

Outputs
=======

The script generates the following outputs:

MAGICC Output Files
-------------------

- Located in the ``magicc_output`` directory.
- Contain temperature projections for each scenario.

RIME Output Files
-----------------

- Located in the ``reporting_output`` directory.
- Contain GDP impact assessments for each damage model and percentile.

Final Reports
-------------

- Generated using legacy reporting functions.
- Provide detailed insights into the economic and climate impacts of each scenario.

