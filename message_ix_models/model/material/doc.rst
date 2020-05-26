Materials accounting
********************

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


Data, metadata, and configuration
=================================

Binary/raw data files
---------------------

The code relies on the following input files, stored in :file:`data/material/`:

:file:`Ammonia feedstock share.Global.xlsx`
   TODO describe the contents, original source, and layout of this file.

:file:`CD-Links SSP2 N-fertilizer demand.Global.xlsx`
   TODO describe the contents, original source, and layout of this file.

:file:`N fertil trade - FAOSTAT_data_9-25-2019.csv`
   TODO describe the contents, original source, and layout of this file.

:file:`n-fertilizer_techno-economic.xlsx`
   TODO describe the contents, original source, and layout of this file.

:file:`trade.FAO.R11.csv`
   TODO describe the contents, original source, and layout of this file.

:file:`trade.FAO.R14.csv`
   TODO describe the contents, original source, and layout of this file.


:file:`material/config.yaml`
----------------------------

.. literalinclude:: ../../../data/material/config.yaml
   :language: yaml
