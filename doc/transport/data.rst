Data requirements
*****************

This page describes the structure and format of required for building MESSAGEix-Transport.

.. contents::
   :local:
   :backlinks: none

:file:`demand-scale.csv`
========================

:file:`gdp.csv`
===============

:file:`ldv-class.csv`
=====================

:file:`ldv-cost-efficiency.xlsx`
================================

Data on costs and efficiencies of LDV technologies.

This is a highly-structured spreadsheet that peforms some input calculations.
The function :func:`get_USTIMES_MA3T` reads data from multiple sheets in this file.
To understand the sheet names and cell layout expected, see the code for that function.

As the name implies, the data for :doc:`MESSAGE (V)-Transport <transport/old>`_ was derived from the US-TIMES and MA³T models.

:file:`mer-to-ppp.csv`
======================

:file:`population-suburb-share.csv`
===================================

:file:`ma3t/population.csv`
===========================

:file:`ma3t/attitude.csv`
=========================

:file:`ma3t/driver.csv`
=======================
