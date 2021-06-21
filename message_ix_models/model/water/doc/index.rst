MESSAGEix-Nexus Module
**********************

:mod:`message_data.model.water` adds water usage and demand related representation to the MESSAGEix-GLOBIOM global model.
The resulting model is referred to as **“MESSAGEix-Nexus”**. This work extends the water sector linkage described by Parkinson et al. (2019) :cite:`parkinson2019`.



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

Pre-processing 
==============
- :file:`data/water/`: contains scripts used in pre-processing source data for the water sector implementaion

  - :file:`add_water_infrastructure.R`: contains spatially-explicit analysis of gridded demands and socioeconomic indicators to develop pathways for sectoral water withdrawals, return flows and infrastructure penetration rates in each MESSAGE region. The pathways feature branching points reflecting a specific water sector development narrative (e.g., convergence towards achieving specific SDG targets).
  - :file:`calculate_ppl_cooling_technology_shares.r`: contains script for processing cooling technology shares at global level for different regional specifications.

Deprecated R Code
=================

- :file:`data/water/deprecated`: contains `R` scripts from the older water sector implementaion

  - :file:`Figures.R`: R script for producing figures
  - :file:`cooling_tech_av.R`: contains similar code as in the above-mentioned scripts, but this was originated from another workstream

CLI usage
=========

Use the :doc:`CLI <cli>` command ``mix-data water`` to invoke the commands defined in :mod:`.water.cli`. Example:
``mix-models --url=ixmp://ixmp_dev/ENGAGE_SSP2_v4.1.7/baseline_clone_test water cooling``
model and scenario specifications can be either set manually in ``cli.py`` or specificed in the ``--url`` option

.. code::

   Usage: mix-models water [OPTIONS] COMMAND [ARGS]...

     MESSAGE-water model.

   Options:
     --regions [ZMB|ISRs|R11|R14|R32|RCP]
     --help  Show this message and exit.

   Commands:
     cooling  Build and solve model with new cooling technologies.

Country vs Global implementation
--------------------------------

The :mod:`message_data.model.water` is designed to being able to add water components to either a global R11 model or any countrty model designed with `the MESSAGEix single country <https://github.com/iiasa/message_single_country>`_ model prototype.
To work with a country model please ensure that:

1. country model and scenario are specified either in ``--url`` or in the ``cli.py`` script
2. the option ``--regions`` is used with the ISO3 code of the country (e.g. for Israel ``--regions=ISR``)
3. Following the Israel example add a 'country'.yaml file in `message_ix_models.data.node` for the specific country
4. Following the Israel example add the country ISO3 code in the 'regions' options in `message_ix_models.usil.click`

Reference
=========

.. toctree::
   :maxdepth: 2

   water/files
