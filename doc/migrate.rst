Migrating from :mod:`message_data`
**********************************

:mod:`message_ix_models` coexists with the private repository/package currently named :mod:`message_data`.
The latter is the location for code related to new research that has not yet been completed and published, data that must remain closed-source permanently, etc.

Over time:

- All other code will be migrated from :mod:`message_data` to :mod:`message_ix_models`.
- Code and data for individual projects will be moved from :mod:`message_data` to :mod:`message_ix_models` at a suitable point during the process of publication.
  (This point may vary from project to project.)
- :mod:`message_data` may be renamed.

Using both packages together
============================

This section gives some practices and tips for using the two packages together.

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
   The open-source code **should** not depend on any private code.
   If this appears necessary, the code in :mod:`message_data` can probably be moved to :mod:`message_ix_models`.

Use :mod:`message_ix_models.tools` and :mod:`~message_ix_models.util` in :mod:`message_data`
   The former have stricter quality standards and are more transparent, which is better for reproducibility.

   At some points, similar code may appear in both packages as it is being migrated.
   In such cases, always import and use the code in :mod:`message_ix_models`, making any adjustments that are necessary.

.. _migrate-filter-repo:

Migrating individual modules
============================

This section describes a general process for migrating (sub)modules of :mod:`.message_data` to :mod:`.message_ix_models`.
Using this process preserves the commit and development history of code and data.
This is useful for future development, and can contain important methodological and research insights that would be lost with a simple copy.

The process:

- Uses the scripts in the :file:`util/` directory of :mod:`.message_ix_models`
- Has been tested on Ubuntu Linux.
  It uses bash shell scripts that may not run in the same way on other systems.
- May need modification depending on the target code.

Requirements:

- Install :program:`git lfs`.
- Install :program:`git-filter-repo`.
  This is a Python package, so e.g.::

    $ pip install git-filter-repo

Read through all the steps first before starting.

1. Create a temporary directory::

   $ mkdir tmp
   $ cd tmp

   This directory will hold *new clones* of both repositories.
   We use new clones to avoid interacting with local settings, uncommitted (ignored) files, or history from other work, and so we can safely delete an incomplete migration and start again.

2. Copy all files from the :file:`util/` in :mod:`.message_ix_models`.

   This can be done from an existing clone, or from GitHub.

3. Edit :file:`settings.sh`.

   Use the comments in the file as a guide to the necessary changes.

4. Prepare :mod:`message_data`. (:file:`1-prep.sh`).
   Run::

     $ ./1-prep.sh

   This script:

   - Clones :mod:`message_data`.
   - Checks out ``$SRC_BRANCH``, the :mod:`message_data` branch from which the migrated code and data will be taken.
   - Fetches all available Git LFS objects associated with any commit in :mod:`message_data`.
     These are needed as the history is replayed in the next step.

     This may download up to 6 GB of data from GitHub, so it can be a slow step.

5. Perform the initial transfer (:file:`2-migrate.sh`).
   Run::

      $ ./2-migrate.sh

   Among other things, this script:

   - Clones :mod:`message_ix_models`.
   - Copies the Git LFS objects from the :mod:`.message_data` clone in step (4), so that they are available for use in the target location.
   - Uses :program:`git-filter-repo` (`docs <https://htmlpreview.github.io/?https://github.com/newren/git-filter-repo/blob/docs/html/git-filter-repo.html>`_) to:

     - Move code.
       For example, all commits pertaining to a file like :file:`message_data/model/foo/bar.py` are preserved, except they now appear to describe changes to :file:`message_ix_models/model/foo/bar.py`.
     - Move data.
       Data is moved from the unpackaged, private, top-level :file:`data/` directory in :mod:`message_data`, to the packageable :file:`message_ix_models/data/` directory.
       There are further considerations; see :doc:`data/` and below.
     - Discard everything else relating to :mod:`message_data`, especially other code and data that are *not* going to be migrated, according to your settings in step (3).
     - Partly clean up commit messages that do not match the code style, e.g. by ensuring they start with a capital letter.

   - Writes two files, :file:`rebase-todo.txt` and :file:`duplicates.txt`, to be used in the next step.

6. Prepare for :program:`git rebase`.

   Make a copy of the file :file:`rebase-todo.txt`, and open the copy.
   This file contains a list of commands for the rebase.
   You can edit this list before using it in step (7); if needed, restore the list by making a fresh copy of the original.

   To help with this, :file:`duplicates.txt` contains a list of identical commit messages that appear more than once in the history.
   These commits *may*—not necessarily—be indication of a *non-linear history*.
   This can occur when branches with similar commit names but different contents are merged together; despite our best efforts, this sometimes happens on :mod:`message_data`.

   Some changes you can make to your copy of :file:`rebase-todo.txt`:

   - Remove lines for duplicated commits, per :file:`duplicates.txt`.
     This avoids commanding :program:`git` to apply the same changes more than once, which can lead to conflicts.
     You could:

     - Keep only the *first* of two or more occurrences of duplicate commits.
     - Keep only the *last* of two or more occurrences of duplicate commits.
     - Use any other strategy to minimize conflicts.

   - Remove lines for merge commits.
     These are ignored by :program:`git rebase` and :program:`git filter-repo`, but you may need to manually skip them if you do not remove them at this step.
   - Add blank lines and comments to help yourself read the history.

7. Perform the rebase.
   Run::

     $ git rebase --interactive --empty=drop main

   - In the editor that opens, delete *everything*.
   - Paste in the contents of :file:`rebase-todo.txt` from step (6).
   - Save the file and exit.

   The interactive rebase begins.

   - Resolve any conflicts that arise in the usual way.
     After resolving, perhaps run::

       $ git add --update && git rebase --continue

   - If you see a message like the following::

       error: commit 47db89c0128e6edf19ebb9ffbcea1d5da4d25176 is a merge but no -m option was given.
       hint: Could not execute the todo command
       hint:
       hint:     pick 47db89c0128e6edf19ebb9ffbcea1d5da4d25176 Merge pull request #123 from iiasa/foo/bar
       hint:
       hint: It has been rescheduled; To edit the command before continuing, please
       hint: edit the todo list first:
       hint:
       hint:     git rebase --edit-todo
       hint:     git rebase --continue

     …follow the instructions:

     1. Give :program:`git rebase --edit-todo`.
     2. Delete the line/command related to the merge commit.
     3. Save and exit.
     4. Give :program:`git rebase --continue`.

   - If many conflicts occur, you may run::

       $ git rebase --abort

     Then, return to step (6) to adjust the list of commands, considering the history and apparent conflicts.

8. Push to ``iiasa/message-ix-models``::

     $ git push --set-upstream=origin migrate-example

   …and open a pull request.

   This can be initially a “draft” state, until you complete step (9).
   The pull request is partly to help you diagnose whether the above steps produced a reasonable result.
   The branch can also be inspected by others, e.g. to compare it to the source in :mod:`.message_data`.

9.  Clean up.

    This can be done directly on the branch from (8).
    However, a better option to create a secondary branch from the HEAD of (8), named like ``migrate-example-tidy``, and make clean-up commits to this branch.
    Create a second pull request to merge this manual clean-up branch into the branch from (8).
    (This way, if the semi-automated process needs to be repeated, it can be re-pushed to ``migrate-example``, and then the manual clean-up branch can be rebased on the newly updated ``migrate-example`` branch, with little disturbance.)

    Push further changes to the clean-up branch to:

    - Modify imports and references.

      For example, when migrating :mod:`message_data.model.foo`, statements like:

      .. code-block:: python

         from message_data.model.foo.bar import baz

      …must be modified to:

      .. code-block:: python

         from message_ix_models.model.foo.bar import baz

      Similar changes must be made to intersphinx references in the documentation.

    - Adjust data handling.

      For example, usage of :func:`.private_data_path` to locate data files must be modified to :func:`.package_data_path` if the data files were moved during the migration.
      Tests can help to ensure that these changes are effective.

    - Address CI checks.
      For example:

      - Add tests, or exclude files from test coverage.
      - Lint files, or exclude files from linting.

    It is important to avoid *scope creep*: do not try to include large modifications, improvements, or refactoring of code in this step.
    This will greatly increase the complexity of the task and make it harder to complete.
    Instead, do these things either *after* or *before* migrating the code.

10. Invite review of your PR.

11. Merge the clean-up branch from (9) into (8), and then (8) into the target branch (currently ``dev``).

At any time:

- Run :program:`./reset.sh` from your temporary directory to delete the clone of :mod:`message_ix_models` and all other changes from step (4).
  Then you can restart from step (4).

After migrating
---------------

Some follow-up actions that **may** or **should** take place after the migration is complete:

- Discuss with the :mod:`message_ix_models` maintainers about releasing a new version of the package, so that the code is available in a released version.
- Open (an) additional issue(s) or PR(s) to record or immediately address missing items—for example, documentation, tests, or small enhancements for reusability—that were identified during the migration.
- Open a PR to *remove* the migrated code from :mod:`message_data`.
  This is important because future development should target the code in its new home in :mod:`message_ix_models`; other projects, workflows, and colleagues should be discouraged to depend on the old code in :mod:`message_data`, where it may not receive updates.

  The simplest way to do this is to delete the code entirely and adjust any other code that imports it to import from the new location in :mod:`message_ix_models`.
  For temporary compatibility, it is also possible to use :func:`message_data.tools.migrated`.

References
----------

:program:`git` and :program:`git filter-repo` are both flexible programs with plenty of power and flexibility.
The above is one suggested way of using them to achieve a clean, history-preserving migration, but there are alternate options.

- :program:`git filter-repo`
  `README <https://github.com/newren/git-filter-repo>`_,
  `user manual <https://htmlpreview.github.io/?https://github.com/newren/git-filter-repo/blob/docs/html/git-filter-repo.html>`_, and
  `discussions <https://github.com/newren/git-filter-repo/discussions>`_
- :program:`git rebase`
  `documentation <https://git-scm.com/docs/git-rebase>`_, and
  `in Chapter 3.6 of the Git Book <https://git-scm.com/book/en/v2/Git-Branching-Rebasing>`_.
- The description of :pull:`86` describes an alternate process.
- PRs that used this process include:

  - :pull:`88` + :pull:`91`, plus `this comment <https://github.com/iiasa/message-ix-models/pull/89#issuecomment-1443393345>`_ showing the manual edits to :file:`rebase-todo.txt`.
  - :pull:`107` + :pull:`110`.
