Low-level utilities (:mod:`.util`)
**********************************


.. currentmodule:: message_ix_models.util

Submodules:

.. autosummary::

   click
   context
   importlib
   logging

Commonly used:

.. autosummary::

   as_codes
   load_package_data
   load_private_data
   package_data_path
   private_data_path
   ~context.Context

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

:mod:`.util.importlib`
======================

.. currentmodule:: message_ix_models.util.importlib

.. automodule:: message_ix_models.util.importlib
   :members:

:mod:`.util.logging`
====================

.. currentmodule:: message_ix_models.util.logging

.. automodule:: message_ix_models.util.logging
   :members:
