Migrating from :mod:`message_data`
**********************************

:mod:`message_ix_models` coexists with the private repository/package currently named :mod:`message_data`.
The latter is the location for code related to new research that has not yet been completed and published, data that must remain closed-source permanently, etc.

Over time:

- All other code will be migrated from :mod:`message_data` to :mod:`message_ix_models`.
- Code and data for individual projects will be moved from :mod:`message_data` to :mod:`message_ix_models` at a suitable point during the process of publication.
  (This point may vary from project to project.)
- :mod:`message_data` may be renamed.

This page gives some practices and tips for using the two packages together.

Always import via :mod:`message_ix_models`
   The package installs :class:`.MessageDataFinder` into Python's import system (`importlib <https://docs.python.org/3/library/importlib.html>`_), which changes its default behaviour as follows: if

   1. A module :samp:`message_ix_models.model.{model_name}` or :samp:`message_ix_models.project.{project_name}` is imported, and
   2. This module does not actually exist in :mod:`message_ix_models`,
   3. Then the code will instead file the respective modules :samp:`message_data.model.{model_name}` or :samp:`message_data.project.{project_name}`.

   Even when using code that currently or temporarily lives in :mod:`message_data`, access it like this:

   .. code-block:: python

      # Code in message_data/model/mymodelvariant.py
      from message_ix_models.model import mymodelvariant

      mymodelvariant.build(...)

   This code is *future-proof*: it will not need adjustment if/when “mymodelvariant” is eventually moved from :mod:`message_data` to :mod:`message_ix_models`.

Use the :program:`mix-models` command-line interface (CLI)
   All CLI commands and subcommands defined in :mod:`message_data` are also made available through the :mod:`message_ix_models` CLI, the executable :program:`mix-models`.

   Use this program in documentation examples and in scripts.
   In a similar manner to the point above, these documents and scripts will remain correct if/when code is moved.

Don't import from :mod:`message_data` in :mod:`message_ix_models`
   The open-source code should not depend on any private code.
   If this appears necessary, the code in :mod:`message_data` can probably be moved to :mod:`message_ix_models`.

Use :mod:`message_ix_models.tools` and :mod:`~message_ix_models.util` in :mod:`message_data`
   The former have stricter quality standards and are more transparent, which is better for reproducibility.

   At some points, similar code may appear in both packages as it is being migrated.
   In such cases, always import and use the code in :mod:`message_ix_models`, making any adjustments that are necessary.
