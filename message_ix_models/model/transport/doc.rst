MESSAGEix-Transport
===================

:mod:`message_ix.tools.transport` adds a technology-rich representation of transport to the MESSAGEix-GLOBIOM global model.
The resulting model is referred to as **“MESSAGEix-Transport”**. This extends the formulation described by McCollum et al. (2016) [1]_ for the older, MESSAGE V framework that predated MESSAGEix.

The code and data:

- Check the RES to confirm that it contains a specific MESSAGEix representation of transportation, to be replaced—including elements of the ``technology``, ``commodity``, ``node`` (region) and other sets.
- Prepares data for MESSAGEix-Transport, based on:

  - files in ``data/transport`` describing the technologies,
  - raw model files from MESSAGE V, and
  - input spreadsheets containing preliminary calculations.

  …and inserts this data into a target :class:`message_ix.Scenario`.

- Provide an exogenous mode choice model that iterates with MESSAGEix-GLOBIOM
  through the ixmp callback feature, to set demand for specific transport
  technologies.

This document contains notes used for porting the representation to the new model framework.

.. contents::
   :local:

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

- This file contains the original LDV-related parameter assumptions before they get sucked into MESSAGE update files and then eventually the sqlite file.

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


Structure
:::::::::

``data/transport/messagev-tech.yaml`` describes the set of technologies.

.. code:: yaml

   .. include:: ../../../data/transport/messagev-tech.yaml


…in MESSAGE V
-------------

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


…in the MESSAGEix-GLOBIOM RES
-----------------------------

- Demand (``commodity=transport``, ``level=useful``) in GWa.
- Technologies producing this output; all at ``m=M1``, except where noted. This is the same set as in MESSAGE V above, i.e. in MESSAGE V, the aggregate transport representation is inactive but still present.

  - ``coal_trp``
  - ``foil_trp``
  - ``loil_trp``
  - ``gas_trp``
  - ``elec_trp``
  - ``meth_ic_trp``
  - ``eth_ic_trp``
  - ``meth_fc_trp``
  - ``eth_fc_trp``
  - ``h2_fc_trp``
  - ``back_trp`` — at modes M1, M2, M3
  - ``Trans_1``
  - ``Trans_2``
  - ``Trans_3``
  - ``Trans_4``
  - ``Trans_5``
- ``historical_activity`` and ``ref_activity`` indicates which of these technologies were active in the model base year.
  - Some, e.g. ``back_trp``, are not (zero values)
  - Disaggregated technologies must match these totals.


Bibliography
::::::::::::

.. [1] https://www.sciencedirect.com/science/article/pii/S1361920915300900
.. [2] https://www.nature.com/articles/s41560-018-0195-z


Usage
:::::

The shell script ``run`` is provided that executes commands defined in :mod:`message_ix.tools.transport.cli`. Try:

.. code::

   $ ./run --help
   Usage: run [OPTIONS] COMMAND [ARGS]...

   Options:
   --help  Show this message and exit.

   Commands:
   clone    Wipe local DB & clone base scenario.
   debug    Temporary code for debugging.
   migrate  Migrate data from MESSAGE V-Transport.
   solve    Set up and run the transport model.

Each individual command also has its own help text; try e.g. ``./run migrate --help``.


API reference
:::::::::::::

.. automodule:: message_ix.tools.transport
   :members:

Command-line
------------
.. automodule:: message_ix.tools.transport.cli
   :members:

Migration
---------
.. automodule:: message_ix.tools.transport.migrate
   :members:

Utilities
---------
.. automodule:: message_ix.tools.transport.utils
   :members:
