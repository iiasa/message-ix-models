Low-level utilities (:mod:`~message_ix_models.util`)
****************************************************


.. currentmodule:: message_ix_models.util

Submodules:

.. autosummary::

   click
   context
   importlib
   _logging
   node
   pooch
   scenarioinfo
   sdmx

Commonly used:

.. autosummary::

   ~config.Config
   ~config.ConfigHelper
   ~context.Context
   ~scenarioinfo.ScenarioInfo
   ~scenarioinfo.Spec
   .adapt_R11_R12
   .adapt_R11_R14
   .as_codes
   broadcast
   cached
   check_support
   convert_units
   copy_column
   ffill
   identify_nodes
   load_package_data
   load_private_data
   local_data_path
   make_io
   make_matched_dfs
   make_source_tech
   maybe_query
   merge_data
   ~node.nodes_ex_world
   package_data_path
   private_data_path
   same_node
   same_time
   series_of_pint_quantity

.. automodule:: message_ix_models.util
   :members:
   :exclude-members: as_codes, eval_anno

.. autodata:: message_ix_models.util.cache.SKIP_CACHE


:mod:`.util.click`
==================

.. currentmodule:: message_ix_models.util.click

.. automodule:: message_ix_models.util.click
   :members:

:mod:`.util.config`
===================

.. currentmodule:: message_ix_models.util.config

.. automodule:: message_ix_models.util.config
   :members:
   :exclude-members: Config

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

   A Context instance always has the following members:

   - ``core``: an instance of :class:`message_ix_models.Config`.
   - ``model``: an instance of :class:`message_ix_models.model.Config`.

   Attributes of these classes may be accessed by shorthand, e.g. ``context.regions`` is shorthand for ``context.model.regions``.

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

   .. The following Context methods and attribute are **deprecated**:

      .. autosummary::
         (currently none)

   .. automethod:: clone_to_dest

      To use this method, either decorate a command with :func:`.common_params`:

      .. code-block:: python

         from message_data.tools.cli import common_params

         @click.command()
         @common_params("dest")
         @click.pass_obj
         def foo(context, dest):
             scenario, mp = context.clone_to_dest()

      or, store the settings :attr:`.Config.dest_scenario` and optionally :attr:`.Config.dest_platform` on `context`:

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

:mod:`.util.node`
=================

.. currentmodule:: message_ix_models.util.node

.. automodule:: message_ix_models.util.node
   :members:
   :exclude-members: identify_nodes

:mod:`.util.pooch`
==================

.. currentmodule:: message_ix_models.util.pooch

.. automodule:: message_ix_models.util.pooch
   :members:


:mod:`.util.scenarioinfo`
=========================

.. currentmodule:: message_ix_models.util.scenarioinfo

.. automodule:: message_ix_models.util.scenarioinfo
   :members:

:mod:`.util.sdmx`
=================

.. currentmodule:: message_ix_models.util.sdmx

.. automodule:: message_ix_models.util.sdmx
   :members:
