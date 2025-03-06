Update Hydro Potential
======================

.. automodule:: message_data.tools.utilities.wrapper_newhydro.IncorporateNewHydro
   :members:

.. automodule:: message_data.tools.utilities.newhydro_change.UpdateNewHydro
   :members:

.. _policy:

Source
------

The scripts are used to updated the future potential of hydropower in MESSAGE based on Gernaat et al. 2021, which considers different climate scenarios (RCP 2.6 and 6.0)
Climate change impacts on renewable energy supply. Nat. Clim. Chang. 11, 119–125 (2021). https://doi.org/10.1038/s41558-020-00949-9

The R scripts :file:`data/hydro/agreggate_MSG.R` and :file:`data/hydro/load_data.R` are used to convert the raw data (that is not shared in this repository) to the right 
format needed for the MESSAGE model in the "R11" or "R12" configuration. The scripts aggregate the capital cost data and load factor data to an arbitrary number # or "bins" 
(by default eight) which then correspond to # hydropower technologies in MESSAGE. The generated data is located in `data\hydro\output_MESSAGE_aggregation`.

Use
---

To replace the old hydropower implementation (with `hydro_lc` and `hydro_hc`), use the function `IncorporateNewHydro`.

.. code-block::
   
      IncorporateNewHydro(scenario, code="ensemble_2p6", reg="R11",startyear=2020)

To change the hydropower climate reference (e.g. from RCP2.6 to 6.0) from a scenario that already has the "new" implementation, use the function `UpdateNewHydro`.

.. code-block::
   
      UpdateNewHydro(scenario, code="ensemble_2p6", reg="R11",startyear=2020)

.. note:: the argument "code" includes a component that recalls the climate model used (or ensemble) another part recalling the scenario. The most common choices are: "ensemble_hist_historical", "ensemble_2p6" and "ensemble_6p0"