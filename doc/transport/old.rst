MESSAGE V file reference
::::::::::::::::::::::::

(Primary author:  David McCollum in December 2018)

Building blocks for transferring the detailed transport module in old MESSAGE to the newer MESSAGEix framework.

The files are located in ``P:\ene.model\TaxSub_Transport_Merged\`` on the IIASA projects drive, except where otherwise noted.


``MESSAGE_Transport_port_to_ix\sqlite\``
----------------------------------------

- sqlite file for the baseline scenario run used for the Nature Energy MIP paper on vehicle choice [2]_ that came out of the ADVANCE project.
- In the baseline scenario, there is no carbon tax and zero/infinitesimal disutility costs assigned to LDV technologies.

``glb\``
--------

- The file ``run_TaxSub_Transport_v2_sqlite`` was used as a run script (in Unix via a Putty window) to create the sqlite file above.
- Note that when running this script, one needs to manually update the name of the output sqlite file in the script ``rmxg_soft_dboutput`` (around line 58; e.g., in this case to "ADV3TRAr2_BaseX2_sqlite.sqlite").

- A note on **‘soft constraints’**: The soft constraints (i.e., flexible market growth constraints depending on price) do not exist in the sqlite file above.
  In the old framework, they entered the model through a different channel when one executed a new MESSAGE run.
  Soft constraints are handled different nowadays in MESSAGEix; however, the new ix framework doesn’t know anything about the detailed transport technologies.
  For a list of these, please see the file ``easemps3_geam.free`` (scroll to just over halfway down in the file and then see the comments).
  Technologies are listed here by their 4-digit identifiers, but these can be mapped to real technology names by using information in the sqlite file and region-specific ``.dic`` files.

.. note::
   PNK: these appear to be applied to nearly all transport technologies. They are growth constraints on activity, with levelized cost penalty of 50$ for growth


``nam\``
--------

- In each of the regional subfolders (e.g., ``'nam'`` here), you can find the dictionary (``.dic``) and chain (``.chn``) files, which in combination with the sqlite file can help to understand the model structure (i.e., the Reference Energy System, RES).
  See ``nam_geam.chn`` and ``nam_geam.dic``.
- Also potentially useful are the input files (sometimes referred to as ‘MatrixAsLDB’) for each model run.
  The one closest to the sqlite generated above (should be identical actually) would be ``nam_geam_ADV3TRAr2_BaseX2_0.inp``.
  The advantage of these files is that they are a bit human-friendlier to read than the sqlite file (at least Volker, Oliver and I can easily make sense of them).
- …and then similar for all the other MESSAGE regions (e.g., afr, cpa, etc.)…

``DB_SQL_Java\Model_merger\``
-----------------------------

- 2 Excel files found in this folder.
  Both would be useful for creating sets of commodities, relations (i.e., equations), and technologies (both real and dummy technologies) that should be filtered for in the sqlite file and then their parameterizations translated into new MESSAGEix speak.
- ``Taxsub_transport_merge_template_DM.xlsx`` lists out the commodities, relations, and technologies that characterize the detailed transport module of MESSAGE.
  Hopefully nothing has been forgotten.
- ``cmp_bl_trpmrg1_LowOP1_ADVWP3ts55mxg1_incl_relations_DM2.xlsx`` then repeats that information (hopefully a one-to-one matching) but is a bit more specific in its categorizations (e.g., which type of relation: c, p, s, 1, 2—less critical in the new MESSAGEix framework).
  This file simultaneously contains the unique commodities, relations, and technologies for the ‘Taxes-Subsidies’ model version, in case there would be interest in porting these over later as well.

``cin\``
--------

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

``macro_runs6\``
----------------

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


``LDV_costs_efficiencies_US-TIMES_MA3T.xlsx``
---------------------------------------------

This file contains the original LDV-related parameter assumptions before they get sucked into MESSAGE update files and then eventually the sqlite file.

Text from the ``MESSAGE_instructions`` sheet:

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

``P:\ene.model\MESSAGE_transport_Kalai_V2_copy\``
-------------------------------------------------

- ``GEAM_TRP_techinput.xls``: this file contains the original non-LDV-related parameter assumptions before they get sucked into MESSAGE update files and then eventually the sqlite file.

``MA3T\ADVANCE_WP3_MIP\``
-------------------------

- See the files: ``disut_cost_comp_summarized_2016-04-08_MESSAGE.xlsx`` and ``consumer_group_splits_2015-06-08_MESSAGE.xlsx``, which are located in the subfolder ``\MA3T\ADVANCE_WP3_MIP\Disutil_cost_and_Consumer_splits``.

- This is where the underlying calculations for the disutility costs by technology, consumer group, and region are done.

``MESSAGE_Transport_port_to_ix\Emails_and_documentation``
---------------------------------------------------------

- Saved a few old email conversation chains, which sort of serve as documentation for how the merging of model versions (transport + taxes/subsidies) was done previously.
  I'm not sure how useful these will be at the current stage, but they were a bit helpful for me when trying to refresh my memory of what came from where; therefore, I figured it's worth parking these aside in case someone else needs them.

- There is no outstanding technical documentation for how the detailed transport model works at a fundamental level.
  The best we have is the more conceptual description, which can be found in the supplementary information of the McCollum et al. (2016) paper in Transportation Research Part D. [1]_


.. [1] https://www.sciencedirect.com/science/article/pii/S1361920915300900
.. [2] https://www.nature.com/articles/s41560-018-0195-z
