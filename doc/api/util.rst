Low-level utilities (:mod:`~message_ix_models.util`)
****************************************************


.. currentmodule:: message_ix_models.util

Submodules:

.. autosummary::

   click
   context
   genno
   importlib
   _logging
   node
   pooch
   pycountry
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
   datetime_now_with_tz
   ffill
   identify_nodes
   iter_keys
   load_package_data
   load_private_data
   local_data_path
   make_io
   make_matched_dfs
   make_source_tech
   maybe_query
   merge_data
   minimum_version
   ~node.nodes_ex_world
   package_data_path
   private_data_path
   same_node
   same_time
   show_versions

.. automodule:: message_ix_models.util
   :members:
   :exclude-members: as_codes, eval_anno

.. autodata:: message_ix_models.util.cache.SKIP_CACHE

:mod:`.util.click`
==================

.. currentmodule:: message_ix_models.util.click

.. automodule:: message_ix_models.util.click
   :members:

   :data:`PARAMS` contains, among others:

   - :program:`--urls-from-file=…` Path to a file containing scenario URLs, one per line.
     These are parsed and stored on :attr:`.Config.scenarios`.

:mod:`.util.config`
===================

.. currentmodule:: message_ix_models.util.config

.. automodule:: message_ix_models.util.config
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

   Context is :class:`dict`-like, so :class:`dict` methods including :meth:`~dict.copy` and :meth:`~dict.setdefault` may be used to handle settings.
   It also provides attribute access: :py:`context.foo` is equivalent to :py:`context["foo"]`.

   A Context instance always has the following members:

   1. :attr:`core`: an instance of :class:`message_ix_models.Config`.
   2. :attr:`model`: an instance of :class:`message_ix_models.model.Config`.
   3. :attr:`report`: an instance of :class:`message_ix_models.report.Config`.

   Attributes of (1) and (2) **may** be accessed by shorthand/aliases.
   For instance, :py:`context.regions` is an alias for :py:`context.model.regions`.
   However, for clarity and to support type checking, explicit reference to the configuration class and its attributes **should** be used.

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

:mod:`.util.genno`
==================

.. currentmodule:: message_ix_models.util.genno

.. automodule:: message_ix_models.util.genno
   :members:

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

:mod:`.util.pycountry`
======================

.. currentmodule:: message_ix_models.util.pycountry

.. automodule:: message_ix_models.util.pycountry
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

:mod:`.util.slurm`
==================

.. currentmodule:: message_ix_models.util.slurm

.. automodule:: message_ix_models.util.slurm
   :members:

:mod:`.types`
=============

.. currentmodule:: message_ix_models.types

.. automodule:: message_ix_models.types
   :members:
