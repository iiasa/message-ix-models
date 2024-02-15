Configuration and input data
****************************

This page describes the structure and format of inputs required for building MESSAGEix-Transport.

.. contents::
   :local:
   :backlinks: none

Both input data and configuration are stored in files under :file:`/data/transport/` in the :mod:`message_data` repository.
(When migrated to :mod:`message_ix_models`, these files will live in :file:`message_ix_models/data/transport`.)

In most cases, these files are read from a subdirectory like :file:`/data/transport/{nodes}/`, where `nodes` denotes the :mod:`message_ix_models` :doc:`node code list <message_ix_models:pkg-data/node>`—for instance, "R12"—for which MESSAGEix-Transport will be built.
This value is retrieved from the :attr:`Context.regions <.model.Config.regions>` setting.

- If the file data or configuration settings have a node (:math:`n`) dimension, the file **must** be placed in such a subdirectory.
  Data for one node list is not usable for base models using a different node list.
- For other data, a node list–specific file **may** be used.
  If none exists, the file of the same name in :file:`/data/transport/` is used as a default.
  For example, :file:`/data/transport/R12/set.yaml` is used if it exists; if not, then :file:`/data/transport/set.yaml` is used.

.. _transport-config:

Configuration
=============

General (:file:`config.yaml`, required)
---------------------------------------

The contents of this configuration file exactly map to the attributes of the class :class:`.transport.Config`.
The class stores all the settings understood by the code for building, solving, and reporting MESSAGEix-Transport, including their default values.
(There is no common default like :file:`/data/transport/config.yaml`.)
It also has methods for reading the configuration from file; see the detailed description of :meth:`.Config.from_context`.

The following is the configuration file for a base model with R12 nodes:

→ View :source:`data/transport/R12/config.yaml` on GitHub

Technology code list (:file:`technology.yaml`)
----------------------------------------------

This file gives the code list for the MESSAGE ``technology`` concept/set/dimension.
Some annotations (``iea-eweb-flow``, ``input``, ``report``) and the :attr:`.child` hierarchy give information about technologies' grouping according to transport modes.

→ View :source:`data/transport/technology.yaml` on GitHub

Code lists for other MESSAGE sets (:file:`set.yaml`)
----------------------------------------------------

This file gives code lists for other MESSAGE concepts/sets/dimensions.

→ View :source:`data/transport/set.yaml` on GitHub

.. _transport-data-files:

Input data files
================

:data:`.transport.files.FILES` gives a list of all data files.
Through :func:`.transport.build.main` (ultimately, :func:`.transport.build.add_exogenous_data`), each of these files is connected to a :class:`genno.Computer` used for model-building, and its contents appear at the key given in the list below.

.. admonition:: Example

   Contents of the file :file:`freight-mode-share-ref.csv` are available at the key ``freight mode share:n-t:ref``; this key indicates the dimensionality of this quantity is :math:`(n, t)`.

Not all files are currently or always used in model-building computations.
Some submodules of :mod:`~message_data.model.transport` use additional data files via other mechanisms.
Most of the files have a header comment including a precise description of the quantity, source of the data, and units of measurement; in some cases extended information is below (where a header comment would be too long).

- :file:`demand-scale.csv` → ``demand scale:n-y:exo``
- :file:`disutility.csv` → ``disutility:n-cg-t-y:per vehicle``
- :file:`energy-other.csv` → ``energy:c-n:transport other``
- :file:`freight-activity.csv` → ``freight activity:n:ref``
- :file:`freight-mode-share-ref.csv` → ``freight mode share:n-t:ref``
- :file:`fuel-emi-intensity.csv` → ``fuel emi intensity:c-e:exo``
- :file:`ikarus/availability.csv` → ``ikarus availability:source-t-c-y:exo``
- :file:`ikarus/fix_cost.csv` → ``ikarus fix_cost:source-t-c-y:exo``
- :file:`ikarus/input.csv` → ``ikarus input:source-t-c-y:exo``
- :file:`ikarus/inv_cost.csv` → ``ikarus inv_cost:source-t-c-y:exo``
- :file:`ikarus/technical_lifetime.csv` → ``ikarus technical_lifetime:source-t-c-y:exo``
- :file:`ikarus/var_cost.csv` → ``ikarus var_cost:source-t-c-y:exo``
- :file:`input-base.csv` → ``input:t-c-h:base``
- :file:`ldv-activity.csv` → ``ldv activity:n:exo``
- :file:`ldv-class.csv` → ``ldv class:n-vehicle_class:exo``
- :file:`ldv-new-capacity.csv` → ``cap_new:nl-t-yv:ldv+exo``
- :file:`load-factor-ldv.csv` → ``load factor ldv:n:exo``
- :file:`load-factor-nonldv.csv` → ``load factor nonldv:t:exo``
- :file:`ma3t/attitude.csv` → ``ma3t attitude:attitude:exo``
- :file:`ma3t/driver.csv` → ``ma3t driver:census_division-area_type-driver_type:exo``
- :file:`ma3t/population.csv` → ``ma3t population:census_division-area_type:exo``
- :file:`mer-to-ppp.csv` → ``mer to ppp:n-y:exo``
- :file:`pdt-cap-ref.csv` → ``pdt:n:capita+ref``
- :file:`population-suburb-share.csv` → ``population suburb share:n-y:exo``

:file:`ldv-cost-efficiency.xlsx`
--------------------------------

Data on costs and efficiencies of LDV technologies.

This is a highly-structured spreadsheet that peforms some input calculations.
The function :func:`get_USTIMES_MA3T` reads data from multiple sheets in this file.
To understand the sheet names and cell layout expected, see the code for that function.

As the name implies, the data for :doc:`MESSAGE (V)-Transport <old>` was derived from the US-TIMES and MA³T models.

Other data sources
==================

:mod:`~message_data.model.transport` makes use of the :mod:`message_ix_models.tools.exo_data` mechanism to retrieve data from common (not transport-specific) sources.
:class:`.DataSourceConfig`, :attr:`.transport.Config.ssp`, and other settings determine which sources and quantities are used.

These include:

- GDP and population from the :mod:`.tools.ssp` databases or other sources including the ADVANCE project, the Global Energy Assessment project, the SHAPE project, etc.

  .. note:: Formerly, file :file:`gdp.csv` was used.

   This is no longer supported; instead, use databases via :func:`.exo_data.prepare_computer` or introduce quantities with the same dimensions and units into the :class:`.Computer` used for model building/reporting.

- Energy from the IEA Extended World Energy Balances.
- :class:`.IEA_Future_of_Trucks`.
- :class:`.MERtoPPP`.
