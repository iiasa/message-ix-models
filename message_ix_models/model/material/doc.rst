Materials accounting
********************

.. warning::

   :mod:`.material` is **under development**.

   For details, see the
   `materials <https://github.com/iiasa/message_data/labels/materials>`_ label and
   `current tracking issue (#248) <https://github.com/iiasa/message_data/issues/248>`_.

This module adds (life-cycle) accounting of materials associated with technologies and demands in MESSAGEix-GLOBIOM.

The initial implementation supports nitrogen-based fertilizers.

.. contents::
   :local:

Code reference
==============

.. currentmodule:: message_data.model.material

.. automodule:: message_data.model.material
   :members:

.. automodule:: message_data.model.material.data
   :members:


CLI usage
=========

Use ``mix-data materials run``, giving the base scenario, e.g.::

    $ mix-data \
      --url ixmp://ene-ixmp/CD_Links_SSP2/baseline
      materials solve
    $ mix-data \
      --url ixmp://ene-ixmp/CD_Links_SSP2/NPi2020-con-prim-dir-ncr
      materials solve
    $ mix-data \
      --url ixmp://ene-ixmp/CD_Links_SSP2/NPi2020_1000-con-prim-dir-ncr
      materials solve
    $ mix-data \
      --url ixmp://ene-ixmp/CD_Links_SSP2/NPi2020_400-con-prim-dir-ncr
      materials solve

The output scenario will be ``ixmp://ene-ixmp/JM_GLB_NITRO/{name}``, where {name} is a shortened version of the input scenario name.


Data, metadata, and configuration
=================================

Binary/raw data files
---------------------

The code relies on the following input files, stored in :file:`data/material/`:

:file:`Ammonia feedstock share.Global.xlsx`
   Feedstock shares (gas/oil/coal) for NH3 production for MESSAGE R11 regions.

:file:`CD-Links SSP2 N-fertilizer demand.Global.xlsx`
   N-fertilizer demand time series from SSP2 scenarios (NPi2020_1000, NPi2020_400, NoPolicy) for MESSAGE R11 regions.

:file:`N fertil trade - FAOSTAT_data_9-25-2019.csv`
   Raw data from FAO used to generate the two :file:`trade.FAO.*.csv` files below.
   Exported from `FAOSTAT <www.fao.org/faostat/en/>`_.

:file:`n-fertilizer_techno-economic.xlsx`
   Techno-economic parameters from literature for various NH3 production technologies (used as direct inputs for MESSAGE parameters).

:file:`trade.FAO.R11.csv`
   Historical N-fertilizer trade records among R11 regions, extracted from FAO database.

:file:`trade.FAO.R14.csv`
   Historical N-fertilizer trade records among R14 regions, extracted from FAO database.

:file:`NTNU_LCA_coefficients.xlsx`
   Material intensity (and other) and other coefficients for power plants based on lifecycle assessment (LCA) data from the THEMIS database, compiled in the `ADVANCE project` <http://www.fp7-advance.eu/?q=content/environmental-impacts-module>`_.

:file:`MESSAGE_global_model_technologies.xlsx`
   Technology list of global MESSAGEix-GLOBIOM model with mapping to LCA technology dataset.

:file:`LCA_region_mapping.xlsx`
   Region mapping of global 11-regional MESSAGEix-GLOBIOM model to regions of THEMIS LCA dataset.

:file:`LCA_commodity_mapping.xlsx`
   Commodity mapping (for materials) of global 11-regional MESSAGEix-GLOBIOM model to commodities of THEMIS LCA dataset.

:file:`material/config.yaml`
----------------------------

.. literalinclude:: ../../../data/material/config.yaml
   :language: yaml

R code and dependencies
-----------------------

:file:`ADVANCE_lca_coefficients_embedded.R`
The code processing the material intensity coefficients of power plants is written in R and integrated into the Python workflow via the Python package `rpy2`.
R code is called from the Python data module `data_power_sector.py`.
Depending on the local R installation(s), the environment variables `R_HOME` and `R_USER` may need to be set for the installation to work (see `stackoverflow <https://stackoverflow.com/questions/12698877/how-to-setup-environment-variable-r-user-to-use-rpy2-in-python>`_).
Additional dependencies include the R packages `dplyr`, `tidyr`, `readxl` and `imputeTS` that need to be installed in the R environment.
