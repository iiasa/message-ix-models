Work with paths to files and data
*********************************

This HOWTO contains some code examples that may help with following the requirements on :doc:`data </data>`.

.. contents::
   :local:

.. _howto-static-to-local:

Connect static data to local data
=================================

Use the :program:`-rs` options to the :program:`cp` command:

.. code-block:: shell

   git clone git@github.com:iiasa/message-static-data.git
   cd /path/to/message-local-data
   cp -rsv /path/to/message-static-data ./

This recursively creates subdirectories in the :ref:`local data <local-data>` directory that mirror those existing in :ref:`message-static-data <static-data>`,
and creates a symlink to every file in every directory.
Code that looks within the local data directory will then be able to locate these files.

If needed, delete the directories in message-local-data and repeat the :program:`cp` call to recreate.

Identify the cache path used on the current system
==================================================

.. code-block:: python

   from message_ix_models.util.config import Config

   cfg = Config()

   print(cfg.cache_path)

Identify the cache path without a Context
=========================================

Internal code that cannot access a :class:`~.util.config.Config` or :class:`.Context` instance
**should** instead use :func:`platformdirs.user_cache_path` directly:

.. code-block:: python

   from platformdirs import user_cache_path

   # Always use "message-ix-models" as the `appname` parameter
   ucp = user_cache_path("message-ix-models")

   # Construct the sub-directory for the current module
   dir_ = ucp.joinpath("my-project", "subdir")
   dir_.mkdir(parents=True, exist_ok=True)

   # Construct a file path within this directory
   p = dir_.joinpath("data-file-name.csv")
