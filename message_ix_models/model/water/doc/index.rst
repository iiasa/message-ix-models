MESSAGEix-Nexus Module
**********************

:mod:`message_data.model.water` adds water usage and demand related representation to the MESSAGEix-GLOBIOM global model.
The resulting model is referred to as **“MESSAGEix-Nexus”**. This work extends the water sector linkage described by Parkinson et al. (2019) :cite:`Parkinson2019`.



.. contents::
   :local:

Code reference
==============

.. currentmodule:: message_data.model.water

.. automodule:: message_data.model.water
   :members:

Build and run
-------------
.. automodule:: message_data.model.water.build
   :members:


Data preparation
----------------

.. automodule:: message_data.model.water.data
   :members:

.. automodule:: message_data.model.water.data.water_for_ppl
   :members:

.. automodule:: message_data.model.water.data.waste_t_d
   :members:

.. automodule:: message_data.model.water.data.demands
   :members:


Utilities and CLI
-----------------

.. automodule:: message_data.model.water.utils
   :members:
   :exclude-members: read_config

.. automodule:: message_data.model.water.cli
   :members:


Data, metadata, and config files
================================

See also: :doc:`water/files`.

- :file:`data/water/`: contains input data used for building the Nexus module

  - :file:`delineation/`: contains geospatial files for basin mapping and MESSAGE regions
  - :file:`ppl_cooling_tech/`: contains cooling technology shares, costs and water intensities for different regional definitions
  - :file:`technology.yaml`: metadata for the 'technology' dimension.
  - :file:`set.yaml`: metadata for other sets.

Deprecated R Code
=================

- :file:`data/water/deprecated`: contains `R` scripts from the older water sector implementaion

  - :file:`add_water_infrastructure.R`: contains spatially-explicit analysis of gridded demands and socioeconomic indicators to develop
      pathways for sectoral water withdrawals, return flows and infrastructure penetration rates
      in each MESSAGE region. The pathways feature branching points reflecting a specific water sector development narrative (e.g., convergence towards achieving specific SDG targets).
  - :file:`generate_water_constraints.R`: contains input data processing and implementation into the MESSAGEix model using the ixmp utilities
      and solving the model for different policy cases to ensure the framework operates as anticipated.
  - :file:`calculate_ppl_cooling_technology_shares.r`: contains script for processing cooling technology shares at global level for different regional specifications.
  - :file:`Figures.R`: R script for producing figures
  - :file:`cooling_tech_av.R`: contains similar code as in the above-mentioned scripts, but this was originated from another workstream


CLI usage
=========

Use the :doc:`CLI <cli>` command ``mix-data water`` to invoke the commands defined in :mod:`.water.cli`. Try:

.. code::

   Usage: mix-data water [OPTIONS] COMMAND [ARGS]...

     MESSAGE-water model.

   Options:
     --help  Show this message and exit.

   Commands:
     build    Prepare the model.
     clone    Clone base scenario to the local database.
     solve    Run the model.

Reference
=========

.. toctree::
   :maxdepth: 2

   water/files
   water/old
   water/cooling