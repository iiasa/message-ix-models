Low-level utilities
*******************

.. currentmodule:: message_ix_models.util

.. automodule:: message_ix_models.util
   :members:

   .. autosummary::

      as_codes
      load_package_data
      load_private_data
      package_data_path
      private_data_path
      ~context.Context

.. currentmodule:: message_ix_models.util.context

.. automodule:: message_ix_models.util.context
   :members:
   :exclude-members: Context

.. autoclass:: Context
   :members:

   Context is a subclass of :class:`dict`, so common methods like :meth:`~dict.copy` and :meth:`~dict.setdefault` may be used to handle settings.
   To be forgiving, it also provides attribute access; ``context.foo`` is equivalent to ``context["foo"]``.

   Context provides additional methods to do common tasks that depend on configurable settings:

   .. autosummary::
      get_cache_path
      get_local_path
      get_platform
      get_scenario
      handle_cli_args

   The following Context methods and attribute are **deprecated**:

   .. autosummary::
      get_config_file
      get_path
      load_config
      units

.. currentmodule:: message_ix_models.util.importlib

.. automodule:: message_ix_models.util.importlib
   :members:
