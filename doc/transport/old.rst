MESSAGE V-Transport
*******************

This page describes building blocks for transferring the detailed transport module in MESSAGE V to the MESSAGEix framework.

It was originally written by David McCollum in December 2018; fragments of that text are sometimes set off with ‘DLM:’/“in quotes,” to distinguish from rewritten/expanded description.

The files are located in :file:`P:\ene.model\TaxSub_Transport_Merged\\` on the IIASA ‘Projects’ shared drive, except where otherwise noted.
Some of the files are reproduced in the :mod:`message_data` repository within :file:`/data/transport/` (if addressed by any code, including older, disused code) or in :file:`/reference/transport/` (if not).

.. contents::
   :local:
   :depth: 2
   :backlinks: none

Structure
=========

- Extra level, ``consumer``, has commodities like ``Dummy_RUEAA``. These have:

  - multiple ‘producers’ like ``ELC_100_RUEAA`` and other LDV technologies for the same consumer group.
  - a single ‘consumer’, ``cons_convert``.

- At the ``final`` level:

  - ``Dummy_Vkm`` is produced by ``cons_convert`` and consumed by ``Occupancy_ptrp``.
  - ``Dummy_Tkm`` is produced by ``FR_.*`` and consumed by ``Load_factor_truck``.
  - ``Dummy_Hkm`` is produced by ``.*_moto`` and consumed by ``Occupancy_moto``.
  - ``DummyGas_ref`` is produced by ``gas_ref_ptrp``.
  - ``DummyH2_stor`` is produced by ``h2stor_ptrp``.
  - ``DummyHybrid`` is produced by ``hybrid_ptrp``.
  - ``DummyOil_ref`` is produced by ``oil_ref_ptrp``.
  - ``Dummy_fc`` is produced by ``fuel_cell_ptrp``.
  - ``Dummy_util`` is produced by ``disutility``.

- At the ``useful`` level:

  - ``trp_2wh`` is produced by ``Occupancy_moto``.
  - ``trp_avi`` is produced by ``con.*_ar`` (4).
  - ``trp_fre`` is produced by ``Load_factor_truck``.
  - ``trp_pas`` is produced by ``Occupancy_ptrp``.
  - ``trp_rai`` is produced by ``dMspeed_rai``, ``Hspeed_rai``, and ``Mspeed_rai``.
  - ``trp_urb`` is produced by ``.*_bus`` (11).
  - ``transport`` also exists, produced by (perhaps legacy technologies): ``back_trp``, ``back_trp``, ``back_trp``, ``eth_ic_trp``, ``eth_fc_trp``, ``h2_fc_trp``, ``coal_trp``, ``elec_trp``, ``foil_trp``, ``gas_trp``, ``loil_trp``, ``meth_ic_trp``, ``meth_fc_trp``, ``Trans_1``, ``Trans_2``, ``Trans_3``, ``Trans_4``, ``Trans_5``.

MESSAGE_Transport_port_to_ix\\sqlite\\
======================================

- sqlite file for the baseline scenario run used for :cite:`mccollum-2018`, the MIP paper on vehicle choice that came out of the ADVANCE project.
- In the baseline scenario, there is no carbon tax and zero/infinitesimal disutility costs assigned to LDV technologies.

glb\\
=====

- The file ``run_TaxSub_Transport_v2_sqlite`` was used as a run script (in Unix via a Putty window) to create the sqlite file above.
- Note that when running this script, one needs to manually update the name of the output sqlite file in the script ``rmxg_soft_dboutput`` (around line 58; e.g., in this case to "ADV3TRAr2_BaseX2_sqlite.sqlite").

- A note on **‘soft constraints’**: The soft constraints (i.e., flexible market growth constraints depending on price) do not exist in the sqlite file above.
  In the old framework, they entered the model through a different channel when one executed a new MESSAGE run.
  Soft constraints are handled different nowadays in MESSAGEix; however, the new ix framework doesn’t know anything about the detailed transport technologies.
  For a list of these, please see the file ``easemps3_geam.free`` (scroll to just over halfway down in the file and then see the comments).
  Technologies are listed here by their 4-digit identifiers, but these can be mapped to real technology names by using information in the sqlite file and region-specific ``.dic`` files.

.. note::
   PNK: these appear to be applied to nearly all transport technologies. They are growth constraints on activity, with levelized cost penalty of 50$ for growth


nam\\
=====

- In each of the regional subfolders (e.g., ``'nam'`` here), you can find the dictionary (``.dic``) and chain (``.chn``) files, which in combination with the sqlite file can help to understand the model structure (i.e., the Reference Energy System, RES).
  See ``nam_geam.chn`` and ``nam_geam.dic``.
- Also potentially useful are the input files (sometimes referred to as ‘MatrixAsLDB’) for each model run.
  The one closest to the sqlite generated above (should be identical actually) would be ``nam_geam_ADV3TRAr2_BaseX2_0.inp``.
  The advantage of these files is that they are a bit human-friendlier to read than the sqlite file (at least Volker, Oliver and I can easily make sense of them).
- …and then similar for all the other MESSAGE regions (e.g., afr, cpa, etc.)…

DB_SQL_Java\\Model_merger\\
===========================

- 2 Excel files found in this folder.
  Both would be useful for creating sets of commodities, relations (i.e., equations), and technologies (both real and dummy technologies) that should be filtered for in the sqlite file and then their parameterizations translated into new MESSAGEix speak.
- ``Taxsub_transport_merge_template_DM.xlsx`` lists out the commodities, relations, and technologies that characterize the detailed transport module of MESSAGE.
  Hopefully nothing has been forgotten.
- ``cmp_bl_trpmrg1_LowOP1_ADVWP3ts55mxg1_incl_relations_DM2.xlsx`` then repeats that information (hopefully a one-to-one matching) but is a bit more specific in its categorizations (e.g., which type of relation: c, p, s, 1, 2—less critical in the new MESSAGEix framework).
  This file simultaneously contains the unique commodities, relations, and technologies for the ‘Taxes-Subsidies’ model version, in case there would be interest in porting these over later as well.

cin\\
=====

- The ``.cin`` files were the post-processing code in the old MESSAGE framework.
  Documentation was previously available on our internal MESSAGE wiki, but that website may no longer be active.
  Perhaps Volker or Peter can provide some access or have PDFs of the pages.
  It would be good to see this documentation so that you know how equation-building worked with the ``.cin`` files.

- Important ``.cin`` files to be aware of and to potentially port over to the post-processing scripts of MESSAGEix would include the following:

  - ``transport_detail_expanded.cin``
  - ``transport_detail_expanded_PART2.cin``
  - ``transport_dis_costs_endog_LDV_sales.cin``
  - ``transport_dis_costs_endog_LDV_stock.cin``
  - ``db_input.cin`` (detailed transport technologies show up in only a few tables)

- ``message-macro6_taxsub.cin``:  some equations in this file are important for producing MESSAGE output that then gets linked to the R script that contains the pkm projection and mode-choice algorithms.

macro_runs6\\
=============

- This folder contains pretty much all of the code that is needed to run MACRO as well as the R script that contains the pkm projection and mode-choice algorithms.
  In the new MESSAGEix framework, the linking of MESSAGE and MACRO is more endogenous through GAMS.
- An important file that kicked off MACRO and the transport R script was ``\glb\rmacro6_soft``

  - In this script, there is a command to execute ``Disutility_Cost_Calculator_v1.R`` located in ``\MA3T\Endogenized_Disutility_Costs\R_project\Scripts``.
    However, I’m not sure how necessary this is going forward.
    The code is the product of an experimental project I did with ALPS-RITE funding, wherein I ran MESSAGE iteratively and consumer preferences, i.e. the disutility costs, changed by vehicle type and consumer group as a function of changing AFV sales and stock levels.

- The output files in the following folder are produced by the ``message-macro6_taxsub.cin`` file mentioned above.
  They pass information from MESSAGE to MACRO and to the transport R script.  ``\macro_runs6\message-exchange``.

- ``MM_link6.R`` and ``macro6_geam.R``: critical R scripts containing the pkm projection and mode-choice algorithms.

- In ``MM_link6.R``, see the section after the halfway point that is commented with ‘Scenario parameters of freedom’.
  This is where the pkm (Schaefer/Victor) and mode-choice (Kyle/Kim) stuff starts, I think.
  Most of the stuff that comes before this in the script is more generic MACRO stuff, which will be treated differently now in MESSAGEix.
  It’s actually not a whole lot of code (<200 lines if I’m not mistaken), so could be pulled out into a separate script pretty easily.
  Of course, what Camila is working on is intended (eventually) to replace this older code.
- ``transport_modeplots.R``: Useful script for visualizing results of pkm projection and mode-choice algorithms.


MA3T
====

DLM: The file ``LDV_costs_efficiencies_US-TIMES_MA3T.xlsx`` contains the original LDV-related parameter assumptions before they get sucked into MESSAGE update files and then eventually the sqlite file.

.. contents::
   :local:
   :backlinks: none

The file has 190 sheets.

Sheet “MESSAGE_instructions”
----------------------------

Instructions for how to generate MESSAGE .upd files that include costs and efficiencies for all LDV technologies and consumer groups.

1. In the "MESSAGE_regional_assumptions" sheet, specify the following for each region: diesel shares, vehicle size class splits, cost multipliers, annual driving distances, vehicle lifetimes, suburbanization rates, etc.

  - Note that as of 2014-4-10, these values are in many cases simply guestimates, and no deep research has been done to peg the values at anything precise.
  - Note that the annual driving distances (and maybe vehicle lifetimes) need to be the same as entered into the MA3T model and MESSAGE adb/upd files.

2. Use the "MESSAGE_LDV_all_regions" sheet to compare how the costs and efficiencies of vehicle technologies compare across regions.

  - Make sure there are no spikes, dips, or anything else that is odd (e.g., one region much higher/lower than others).

3. Copy values from the "MESSAGE_upd_xxx" sheet (where 'xxx' is the region) and paste them into the respective .upd files for each region.

  - The update files are called "transport_techsX_NAM.upd" and "transport_techsX_non_NAM.upd" (where X is either blank or a number >1).

  .. caution:: As of 2015-05-04, the set-up for calculating the consumer group %-splits for the "transport_techs4_nonNAM.upd" and "transport_techs4_NAM.upd" files has been moved to another XLS file (e.g., "consumer_group_splits_2015-04-28_MESSAGE.xlsx").
     Therefore, do NOT use the information on the "MESSAGE_upd4_xxx" sheets in this file.
     Also, note that the naming convention of these update files has slightly changed:  "nonNAM" instead of "non_NAM" in the suffix.

  .. caution:: As of 2016-01-26, the vehicle INV/FOM costs and efficiencies have been made consistent with the MA3T (2015 version of model).
     This has currently only been done for the NAM region (see the purple sheets).

Sheet “MA3T_Techno_data”
------------------------

This sheet contains quantities from MA3T. The dimensions are:

- Period: annual from 2005 to 2050 inclusive.
- Vehicle type: 300 different categories.

The quantities:

- Vehicle Manufacturer Cost [USD 2005 / vehicle] (type, period)
- Fuel Economy, UDDS, CD, Fuel [gallons gasoline equivalent / mile] (type, period)
- Fuel Economy, HWFET, CD, Fuel (type, period)
- Fuel Economy, UDDS, CS, Fuel (type, period)
- Fuel Economy, HWFET, CS, Fuel (type, period)
- Electricity Consumption, UDDS, CD [watt-hour / mile] (type, period)
- Electricity Consumption, HWFET, CD (type, period)
- Range, blended CD, UDDS [mile] (type, period)
- Range, blended CD, HWFET (type, period)
- Year on the Market (type)
- Technology Grouping for Output (type)
- Fuel Economy Adjustment Factor (type, drive cycle, fuel)
- Annual Maintenant [sp] Cost [USD 2005] (type, period) — units are written as “$”.
- Vehicle Price Mark-up Factor [1] (type, period)
- Vehicle Fuel Consumption Rate in Step 0 [gallons gasoline equivalent / mile] (type, period)
- Vehicle Electricity Consumption Rate in Step 0 [watt-hour / mile]

Sheet “MESSAGE_regional_assumptions”
------------------------------------

The tables in this sheet have been preserved as the following files:

- “Vehicle class splits” → ldv_class.csv.
- “Regional cost multipliers” → config.yaml keys ``factor / cost / ldv 2010``, ``ldv cost catch-up year``.
- “Annual driving distances by consumer type” → config.yaml keys ``ldv activity``, ``factor / activity / ldv``.
- “Vehicle lifetimes by consumer type” → :file:`lifetime-ldv.csv`.
- “Suburbanization rates” → suburb_area_share.csv.

  .. admonition:: PNK

     The use of ‘area’ here was probably a mistake: it seems to mean the fraction of *population* and thus their driving activity.

- “Which population projections should be used?”

  Refers to sheet “Urbanization_data_GEA_{Mix,Supply,Eff}”.
  These contain population [million people] by: period (2005, 2010, …, 2100), region (R11), {urban, suburban, total}, scenario (``geama_450_btr_full``, ``geaha_450_atr_full``, and ``geala_450_atr_nonuc`` respectively).

Sheet “MESSAGE_Process”
-----------------------

This sheet contains data from US-TIMES.

- DLM: “Because the original US-TIMES efficiency values were in terms of HHV, I modified Kalai's original conversion factor to ensure that the efficiencies are in terms of LHV (which is what MESSAGE uses). This inflates the values slightly; inflate because the GW-yr value is in the denominator.”

  - The values in thie sheet are converted by the unit conversion factors in the header from sheet “ProcessCharac”, which has similar structure.
  - Cells in that sheet are in turn references to “TRNLDV_Reduced ver,” which has the comment “EPANMD_10_TRNLDV_v1.0”

- Column J contains the quantity ``CEFF-I`` “Commodity input efficiency” [billion vehicle kilometre / gigawatt hour year].
  Columns to the right contain values for other periods.
- Column Z contains the quantity ``NCAP_COST`` “Investment Cost” [million ‘$’ / million vehicle].
  Columns to the right contain values for other periods.

  - For “Existing” vehicles, DLM comments “Because the original US-TIMES data gave no costs here, I assume they are equal to present-day/future conventional vehicles (whether gasoline or diesel).”

- Other comments appearing in this sheet:

  - DLM: “There are no {mini-compact, pickup} diesels in the US-TIMES dataset, so I roughly estimate what the efficiency of that vehicle type would be by using as a proxy the relative efficiencies of the mini-compact and compact gasoline vehicles.”
  - Others by someone named “Samaneh.”

Sheet “MESSAGE_LDV_nam”
-----------------------

This sheet is the prototype for model input calculations.

Parameter appearing in this sheet:

- “Scaling factor to reduce the cost of NGA vehicles” → preserved in config.yaml key ``factor / cost / lgv nga``; see comment there.

Calculations:

- Efficiency [billion vehicle kilometre / gigawatt-hour-year]::

    = 1 / (
      (
        (1 - $E$20) * (
          ($E$22 * (1 / $MESSAGE_Process.J$153))
          + ($E$23 * (1 / $MESSAGE_Process.J$155))
          + ($E$24 * (1 / $MESSAGE_Process.J$159))
          + ($E$25 * (1 / $MESSAGE_Process.J$165))
          + ($E$26 * (1 / $MESSAGE_Process.J$167))
          + ($E$27 * (1 / $MESSAGE_Process.J$161))
          + ($E$28 * (1 / $MESSAGE_Process.J$163))
        )
      ) + (
        ($E$20) * (
          ($E$22 * (1 / $MESSAGE_Process.J$11))  # [no label] / Mini compact Diesel URBAN
          +($E$23 * (1 / $MESSAGE_Process.J$13))  # TLCDSLURBAN / Compact Diesel URBAN
          +($E$24 * (1 / $MESSAGE_Process.J$17))  # TLFDSLURBAN / Full Diesel URBAN
          +($E$25 * (1 / $MESSAGE_Process.J$23))  # TLSSDSLURBAN / Small SUV Diesel URBAN
          +($E$26 * (1 / $MESSAGE_Process.J$25))  # TLLSDSLURBAN / Large SUV Diesel URBAN
          +($E$27 * (1 / $MESSAGE_Process.J$19))  # TLMVDSLURBAN / Minivan Diesel URBAN
          +($E$28 * (1 / $MESSAGE_Process.J$21))  # TLPDSLURBAN / Pickup Diesel URBAN
        )
      )
    )

    E20 is the diesel/gasoline share; the other entries from column E are the vehicle class shares.
    → This is a weighted average efficiency.

- Investment cost [million $ / million vehicle]: weighted average over column Z


P:\\ene.model\\MESSAGE_transport_Kalai_V2_copy\\
================================================

- ``GEAM_TRP_techinput.xls``: this file contains the original non-LDV-related parameter assumptions before they get sucked into MESSAGE update files and then eventually the sqlite file.

MA3T\\ADVANCE_WP3_MIP\\
=======================

- See the files: ``disut_cost_comp_summarized_2016-04-08_MESSAGE.xlsx`` and ``consumer_group_splits_2015-06-08_MESSAGE.xlsx``, which are located in the subfolder ``\MA3T\ADVANCE_WP3_MIP\Disutil_cost_and_Consumer_splits``.

- This is where the underlying calculations for the disutility costs by technology, consumer group, and region are done.

MESSAGE_Transport_port_to_ix\\Emails_and_documentation
======================================================

- Saved a few old email conversation chains, which sort of serve as documentation for how the merging of model versions (transport + taxes/subsidies) was done previously.
  I'm not sure how useful these will be at the current stage, but they were a bit helpful for me when trying to refresh my memory of what came from where; therefore, I figured it's worth parking these aside in case someone else needs them.

- There is no outstanding technical documentation for how the detailed transport model works at a fundamental level.
  The best we have is the more conceptual description, which can be found in the supplementary information of :cite:`mccollum-2017`.
