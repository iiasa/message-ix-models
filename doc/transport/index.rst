MESSAGEix-Transport
===================

:mod:`message_data.model.transport` adds a technology-rich representation of transport to the MESSAGEix-GLOBIOM global model.
The resulting model is referred to as **“MESSAGEix-Transport”**. This extends the formulation described by McCollum et al. (2016) :cite:`McCollum2017` for the older, MESSAGE V framework that predated MESSAGEix.

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


Structure
:::::::::

``data/transport/technology.yaml`` describes the set of technologies.

.. literalinclude:: /../../data/transport/technology.yaml
   :language: yaml


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
.. currentmodule:: message_data.model.transport

.. automodule:: message_data.model.transport
   :members:

Command-line
------------
.. automodule:: message_data.model.transport._cli
   :members:

Importing non-LDV technologies
------------------------------
.. automodule:: message_data.model.transport.data.ikarus
   :members:

Migration
---------
.. automodule:: message_data.model.transport.migrate
   :members:

Utilities
---------
.. automodule:: message_data.model.transport.utils
   :members:


Files
:::::


- ``data/transport/``: data files from ``P:\\ene.model\\TaxSub_Transport_Merged`` and other metadata used for defining transport technologies.
- ``data/transport/technology.yaml``: metadata for technologies from the old *MESSAGE V-Transport* model.
- ``message_data/tools/messagev/``: tools for extracting data from *MESSAGE V*.
- ``message_data/model/transport/``: code for running *MESSAGEix-Transport*.
- ``message_data/model/transport/data/``: scripts for parsing data files in ``data/transport/``.
- ``tests/model/test_transport.py``: tests for *MESSAGEix-Transport*.
- ``reference/transport``: files from the ca. 2016 *MESSAGE V-Transport* model. The
  directory structure matches ``P:\\ene.model\\TaxSub_Transport_Merged``.
  See **paths.txt**.

On a separate page:

.. toctree::
   :maxdepth: 1

   transport/old
