Data requirements
*****************

This page describes the structure and format of data required for building MESSAGEix-Transport.

.. contents::
   :local:
   :backlinks: none

In files under :file:`/data/transport/`
=======================================

In most cases, these files are read from a subdirectory like :file:`/data/transport/{id}/`, using the `id` of the :doc:`node code list <message-ix-models:pkg-data/node>`, e.g. "R12".
Files **must** be placed in these subdirectories if the corresponding quantity has a node (:math:`n`) dimension, because any such data will not be suitable for use in models with different spatial representation.

Where the file is missing, the version in :file:`/data/transport/` is used as a default.
Files for quantities without a :math:`n` dimension **may** be placed here.

:file:`demand-scale.csv`
------------------------

:file:`ldv-class.csv`
---------------------

:file:`ldv-cost-efficiency.xlsx`
--------------------------------

Data on costs and efficiencies of LDV technologies.

This is a highly-structured spreadsheet that peforms some input calculations.
The function :func:`get_USTIMES_MA3T` reads data from multiple sheets in this file.
To understand the sheet names and cell layout expected, see the code for that function.

As the name implies, the data for :doc:`MESSAGE (V)-Transport <old>` was derived from the US-TIMES and MA³T models.

:file:`mer-to-ppp.csv`
----------------------

:file:`population-suburb-share.csv`
-----------------------------------

:file:`ma3t/population.csv`
---------------------------

:file:`ma3t/attitude.csv`
-------------------------

:file:`ma3t/driver.csv`
-----------------------

Other data sources
==================

GDP and population
------------------

These are read using :func:`.gdp_pop` which (see its documentation) allows a choice between sources such as the GEA, SSP, SHAPE, and other databases. [1]_

.. [1] formerly, a file :file:`gdp.csv` was used.
   This is no longer supported; instead, use databases via :func:`.gdp_pop` or introduce quantities with the same dimensions and units into the :class:`.Computer` used for model building/reporting.
