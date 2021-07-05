Low-level utilities (:mod:`~message_ix_models.util`)
****************************************************


.. currentmodule:: message_ix_models.util

Submodules:

.. autosummary::

   click
   context
   importlib
   _logging
   scenarioinfo

Commonly used:

.. autosummary::

   adapt_R11_R14
   as_codes
   broadcast
   copy_column
   ffill
   identify_nodes
   load_package_data
   load_private_data
   make_io
   make_matched_dfs
   make_source_tech
   merge_data
   package_data_path
   private_data_path
   same_node
   ~context.Context
   ~scenarioinfo.ScenarioInfo

.. automodule:: message_ix_models.util
   :members:


:mod:`.util.click`
==================

.. currentmodule:: message_ix_models.util.click

.. automodule:: message_ix_models.util.click
   :members:

:mod:`.util.context`
====================

.. currentmodule:: message_ix_models.util.context

.. automodule:: message_ix_models.util.context
   :members:
   :exclude-members: Context

.. autoclass:: Context
   :members:
   :exclude-members: clone_to_dest

   Context is a subclass of :class:`dict`, so common methods like :meth:`~dict.copy` and :meth:`~dict.setdefault` may be used to handle settings.
   To be forgiving, it also provides attribute access; ``context.foo`` is equivalent to ``context["foo"]``.

   Context provides additional methods to do common tasks that depend on configurable settings:

   .. autosummary::
      clone_to_dest
      close_db
      delete
      get_cache_path
      get_local_path
      get_platform
      get_scenario
      handle_cli_args
      only
      use_defaults

   The following Context methods and attribute are **deprecated**:

   .. autosummary::
      get_config_file
      get_path
      load_config
      units

   .. automethod:: clone_to_dest

      To use this method, either decorate a command with :func:`common_params`:

      .. code-block:: python

         from message_data.tools.cli import common_params

         @click.command()
         @common_params("dest")
         @click.pass_obj
         def foo(context, dest):
             scenario, mp = context.clone_to_dest()

      or, store the settings ``dest_scenario`` and ``dest_platform`` on `context`:

      .. code-block:: python

         c = Context.get_instance()

         c.dest_scenario = dict(model="foo model", scenario="foo scenario")
         scenario_mp = context.clone_to_dest()

      The resulting scenario has the indicated model- and scenario names.

      If ``--url`` (or ``--platform``, ``--model``, ``--scenario``, and optionally ``--version``) are given, the identified scenario is used as a 'base' scenario, and is cloned.
      If ``--url``/``--platform`` and ``--dest`` refer to different Platform instances, then this is a two-platform clone.

      If no base scenario can be loaded, :func:`.bare.create_res` is called to generate a base scenario.


:mod:`.util.importlib`
======================

.. currentmodule:: message_ix_models.util.importlib

.. automodule:: message_ix_models.util.importlib
   :members:

:mod:`.util._logging`
=====================

.. currentmodule:: message_ix_models.util._logging

.. automodule:: message_ix_models.util._logging
   :members:

:mod:`.util.scenarioinfo`
=========================

.. currentmodule:: message_ix_models.util.scenarioinfo

.. automodule:: message_ix_models.util.scenarioinfo
   :members:
