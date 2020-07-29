MESSAGEix-Transport
*******************

:mod:`message_data.model.transport` adds a technology-rich representation of transport to the MESSAGEix-GLOBIOM global model.
The resulting model is referred to as **“MESSAGEix-Transport”**. This extends the formulation described by McCollum et al. (2016) :cite:`McCollum2017` for the older, MESSAGE V framework that predated MESSAGEix.

The code and data:

- Check the RES to confirm that it contains a specific MESSAGEix representation of transportation, to be replaced—including elements of the ``technology``, ``commodity``, ``node`` (region) and other sets.
- Prepares data for MESSAGEix-Transport, based on:

  - files in :file:`data/transport` describing the technologies,
  - input spreadsheets containing preliminary calculations.

  …and inserts this data into a target :class:`.Scenario`.

- Provide an exogenous mode choice model that iterates with MESSAGEix-GLOBIOM
  through the ixmp callback feature, to set demand for specific transport
  technologies.

.. contents::
   :local:

Usage
=====

Preliminaries. Create a platform named e.g. ``mt``::

    $ ixmp platform add mt jdbc hsqldb path/to/db

Create the bare RES, or identify another base scenario::

    $ export BASE="ixmp://mt/Bare RES/baseline"
    $ mix-data --url=$BASE res create-bare

.. note:: Other usable base scenarios include ``ixmp://ene-ixmp/CD_LINKS_SSP2_v2/baseline``.

Build the model::

    # export URL=ixmp://mt/MESSAGEix-Transport/baseline
    $ mix-data --url=$BASE transport build --dest=$URL

Solve the model::

    $ message-ix --url=$URL solve

Report the results, using :mod:`model.transport.report` to add additional reporting calculations::

    $ mix-data --url=$URL report -m model.transport "transport plots"

Code reference
==============

.. currentmodule:: message_data.model.transport

.. automodule:: message_data.model.transport
   :members:

Build and run
-------------
.. automodule:: message_data.model.transport.build
   :members:

.. automodule:: message_data.model.transport.callback
   :members:

Data preparation
----------------
.. automodule:: message_data.model.transport.data
   :members:

.. automodule:: message_data.model.transport.data.groups
   :members:

.. automodule:: message_data.model.transport.data.ikarus
   :members:

.. automodule:: message_data.model.transport.data.ldv
   :members:

.. automodule:: message_data.model.transport.data.non_ldv
   :members:

Utilities and CLI
-----------------
.. automodule:: message_data.model.transport.report
   :members:

.. automodule:: message_data.model.transport.utils
   :members:
   :exclude-members: read_config

.. automodule:: message_data.model.transport.cli
   :members:


Data, metadata, and config files
================================

See also: :doc:`transport/files`.

- :file:`data/transport/`: data files from :file:`P:\ene.model\TaxSub_Transport_Merged` and other metadata used for defining transport technologies.

  - :file:`config.yaml`: general configuration for :func:`.transport.build.main`.
  - :file:`technology.yaml`: metadata for the 'technology' dimension.
  - :file:`set.yaml`: metadata for other sets.

- ``reference/transport``: files from the ca. 2016 *MESSAGE V-Transport* model. The
  directory structure matches :file:`P:\ene.model\TaxSub_Transport_Merged\`.
  See :doc:`data`.


CLI usage
=========

Use the :doc:`CLI </usage>` command ``mix-data transport`` to invoke the commands defined in :mod:`.transport._cli`. Try:

.. code::

   Usage: mix-data transport [OPTIONS] COMMAND [ARGS]...

     MESSAGE-Transport model.

   Options:
     --help  Show this message and exit.

   Commands:
     build    Prepare the model.
     clone    Clone base scenario to the local database.
     migrate  Migrate data from MESSAGE(V)-Transport.
     solve    Run the model.

Each individual command also has its own help text; try e.g. ``mix-data transport build --help``.


Base structure
==============

The MESSAGEix-GLOBIOM RES (e.g. :mod:`.model.create` or :mod:`.model.bare`) contains an aggregated transport representation, as follows:

- Demand (``commodity=transport``, ``level=useful``) in GWa.
- Technologies producing this output; all at ``m=M1``, except where noted.
  This is the same set as in :doc:`MESSAGE V <transport/old>`, i.e. in MESSAGE V, the aggregate transport representation is inactive but still present.

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


Reference
=========

.. toctree::
   :maxdepth: 2

   transport/files
   transport/old
