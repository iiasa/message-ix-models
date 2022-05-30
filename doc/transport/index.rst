MESSAGEix-Transport
*******************

.. warning::

   MESSAGEix-Transport is **under development**.

   For details, see the
   `transport <https://github.com/iiasa/message_data/labels/transport>`_ label,
   `project board <https://github.com/iiasa/message_data/projects/1>`_, and
   `current tracking issue (#180) <https://github.com/iiasa/message_data/issues/180>`_.

:mod:`message_data.model.transport` adds a technology-rich representation of transport to the MESSAGEix-GLOBIOM global model.
The resulting model is referred to as **“MESSAGEix-Transport”**. This extends the formulation described by McCollum et al. (2016) :cite:`McCollum2017` for the older, MESSAGE V framework that predated MESSAGEix.

On this page:

.. contents::
   :local:

On other pages:

- :doc:`transport/files`
- :doc:`transport/data`
- :doc:`transport/disutility`
- :doc:`transport/report`
- :doc:`transport/old`

Summary
=======

The code and data:

- Check the RES to confirm that it contains a specific MESSAGEix representation of transportation, to be replaced—including elements of the ``technology``, ``commodity``, ``node`` (region) and other sets.
- Prepares data for MESSAGEix-Transport, based on:

  - files in :file:`data/transport` describing the technologies,
  - input spreadsheets containing preliminary calculations.

  …and inserts this data into a target :class:`.Scenario`.

- Provide an exogenous mode choice model that iterates with MESSAGEix-GLOBIOM
  through the ixmp callback feature, to set demand for specific transport
  technologies.

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

Utilities
---------

The :command:`gen-demand` CLI command is provided to allow generating demand data without building a full scenario::

    $ mix-models transport gen-demand "SHAPE innovation" ../output_dir/

Output is written to a CSV file in the indicated directory.
See :func:`gdp_pop`, the ``--help`` text, and the command-line output.


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

``ixmp://ixmp-dev/MESSAGEix-Materials/NoPolicy_GLOBIOM_R12_s#1``
  regions=R12, years=B. Includes :doc:`material` detail.

``ixmp://ixmp-dev/MESSAGEix-Materials/NoPolicy_2305#?`` (default version as of 2022-05-25)
  regions=R12, years=B. Includes :doc:`material` detail.


Structure of base scenarios
---------------------------

The MESSAGEix-GLOBIOM RES (e.g. :mod:`.model.create` or :mod:`.model.bare`) contains an aggregated transport representation, as follows:

- Demand (``commodity=transport``, ``level=useful``) is denoted in **energy units**, i.e. GWa.
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


Data, metadata, and config files
================================

See also: :doc:`transport/files` and :doc:`transport/data`.

:func:`~.transport.read_config` reads files from :file:`data/transport/` **or** a subdirectory.
This allows to separate input data files according to the node list used by the base model.
See the function docs for details.

Other data files include:

- :file:`data/transport/` contains data files originally from :file:`P:\ene.model\TaxSub_Transport_Merged` (a private IIASA shared drive) and other metadata used for defining transport technologies.
- :file:`reference/transport/` contains files from :doc:`transport/old` that are for reference, and not used by MESSAGEix-Transport.
  The directory structure matches :file:`P:\ene.model\TaxSub_Transport_Merged\\`.


Reference
=========

.. toctree::
   :maxdepth: 2

   transport/files
   transport/data
   transport/disutility
   transport/report
   transport/old


Code reference
==============

.. currentmodule:: message_data.model

.. autosummary::
   :toctree: _autosummary
   :template: autosummary-module.rst
   :recursive:

   transport
