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

On this page:

.. contents::
   :local:

On other pages:

- :doc:`transport/report`
- :doc:`transport/disutility`

Usage
=====

**Preliminaries.**
Check the list of :doc:`pre-requisite knowledge <message_ix:prereqs>` for working with :mod:`.message_data`.

.. note:: One pre-requisite is basic familiarity with using a shell/command line.

   Specifically: ``export BASE="…"``, seen below, is a built-in command of the Bash shell (Linux or macOS) to set an environment variable.
   ``$BASE`` refers to this variable.
   In the Windows Command Prompt, use ``set BASE="…"`` to set and ``%BASE%`` to reference.
   Variables with values containing spaces must be quoted when referencing, as in the example commands below.

   To avoid using environment variables altogether, insert the URL directly in the command, for instance::

       $ mix-models --url="ixmp://mt/Bare RES/baseline" res create-bare

**Choose a platform.**
This example uses a platform named ``mt``.
If not already configured on your system, create the configuration for the platform to be used::

    $ ixmp platform add mt jdbc hsqldb /path/to/db

.. note:: See the :ref:`ixmp documentation <ixmp:configuration>` for how to use the ``ixmp`` command to add or edit configuration for specific platforms and databases.

**Identify the base scenario.**
One option is to create the ‘bare’ RES; the following is equivalent to calling :func:`.bare.create_res`::

    $ export BASE="ixmp://mt/Bare RES/baseline"
    $ mix-models --url="$BASE" res create-bare

For other possibilities, see :ref:`transport-base-scenarios`.

**Build the model.**
The following is equivalent to cloning ``BASE`` to ``URL``, and then calling :func:`.transport.build.main` on the scenario stored at ``URL``::

    $ export URL=ixmp://mt/MESSAGEix-Transport/baseline
    $ mix-models --url="$BASE" transport build --dest="$URL"

**Solve the model.**
The following is equivalent to calling :meth:`message_ix.Scenario.solve`::

    $ message-ix --url="$URL" solve

**Report the results.**
The ``-m model.transport`` option indicates that additional reporting calculations from :mod:`model.transport.report` should be added to the base reporting configuration for MESSAGEix-GLOBIOM::

    $ mix-models --url="$URL" report -m model.transport "transport plots"


.. _transport-base-scenarios:

Base scenarios
==============

The following existing scenarios are targets for the MESSAGEix-Transport code to operate on:

``ixmp://ene-ixmp/CD_Links_SSP2_v2/baseline``
   regions=R11, years=A.

``ixmp://ixmp-dev/ENGAGE_SSP2_v4.1.7/baseline#3``
   regions=R11, years=B.

``ixmp://ixmp-dev/ENGAGE_SSP2_v4.1.7_ar5_gwp100/EN_NPi2020_1000_emif_new#5``
   regions=R11, years=B.
   This scenario has a “hybrid” or “dual” implementation of emissions accounting: it includes *both*:

   - the ‘old’ structure, in which emissions are accounted using :mod:`message_ix` ``relation_activity`` and related parameter, but ``emission_factor`` is unused/empty, **and**
   - a ‘new’ structure in which the ``emission_factor`` parameter is actually used.

``ixmp://ixmp-dev/MESSAGEix-GLOBIOM_R12_CHN/baseline#17``
   regions=R12, years=B. Based on ENGAGE, without MACRO calibration.

``ixmp://ixmp-dev/MESSAGEix-GLOBIOM_R12_CHN/baseline_macro#3``
   regions=R12, years=B. Includes MACRO calibration


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

Use the :doc:`CLI <cli>` command ``mix-models transport`` to invoke the commands defined in :mod:`.transport.cli`. Try:

.. code::

   Usage: mix-models transport [OPTIONS] COMMAND [ARGS]...

     MESSAGE-Transport variant.

   Options:
     --help  Show this message and exit.

   Commands:
     build    Prepare the model.
     debug    Temporary code for development.
     migrate  Migrate data from MESSAGE(V)-Transport.
     solve    Run the model.

Each individual command also has its own help text; try e.g. ``mix-models transport build --help``.


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

   transport/report
   transport/files
   transport/disutility
   transport/old
