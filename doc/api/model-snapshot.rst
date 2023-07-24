.. currentmodule:: message_ix_models.model.snapshot

Load model snapshots (:mod:`.model.snapshot`)
*********************************************

This code allows to fetch *snapshots* containing completely parametrized MESSAGEix-GLOBIOM model instances, and load these into :class:`Scenarios <message_ix.Scenario>`.

Usage
=====

From the command line, download data for a single snapshot::

    $ mix-models snapshot fetch 0

…where :program:`0` is the ID of a snapshot; see :data:`.SNAPSHOTS`.

In code, use :func:`.snapshot.load`:

.. code-block:: python

    from message_ix import Scenario
    from message_ix_models.model import snapshot

    scenario = Scenario(...)

    snapshot.load(scenario, 0)

.. note:: For snapshot 0, contrary to the `description of the Zenodo item <https://10.5281/zenodo.5793870>`__, the file cannot be loaded using :meth:`.Scenario.read_excel`.
   This limitation will be fixed in subsequent snapshots.

Code reference
==============

.. automodule:: message_ix_models.model.snapshot
   :members:
