Reproduce the RES (:mod:`.model.bare`)
**************************************

In contrast to :mod:`.model.create`, this module creates the RES 'from scratch'.
:func:`.create_res` begins by creating a new, totally empty :class:`~message_ix.Scenario` and adding data to it (instead of cloning and modifying an existing scenario).

.. note:: Currently, the Scenario returned by :func:`.create_res`…

   - is not complete, nor the official/preferred version of MESSAGEix-GLOBIOM, and as such **must not** be used for actual research,
   - however, it **should** be used for creating unit tests of other code that is designed to operate on MESSAGEix-GLOBIOM scenarios; code that works against the bare RES should also work against MESSAGEix-GLOBIOM scenarios.

:func:`.bare.get_spec` can also be used directly, to get a *description* of the RES based on certain settings/options, but without any need to connect to a database, load an existing Scenario, or call :func:`.bare.create_res`.
This can be useful in code that processes data into a form compatible with MESSAGEix-GLOBIOM.

Configuration
=============

The code obeys the settings on the :class:`.model.Config` instance stored at ``context.model``.


Code reference
==============

.. currentmodule:: message_ix_models.model.bare

.. automodule:: message_ix_models.model.bare
   :members:
   :exclude-members: get_spec

.. autofunction:: get_spec

   Since the RES is the base for all variants of MESSAGEix-GLOBIOM, the 'require' and 'remove' portions of the spec are empty.

   For the 'add' section, :func:`message_ix_models.model.structure.get_codes` is used to retrieve data from the YAML files in :mod:`message_ix_models`.

   Settings are retrieved from `context`, as above.


.. currentmodule:: message_ix_models.model.data

.. automodule:: message_ix_models.model.data
   :members:


.. Roadmap
.. =======

.. todo:: With `ixmp#212 <https://github.com/iiasa/ixmp/pull/212>`_ merged,
   some :mod:`.model.bare` code could be moved to a new class and method like
   :meth:`.MESSAGE_GLOBIOM.initialize`.
