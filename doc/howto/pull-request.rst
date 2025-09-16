Contribute via pull requests
****************************

This guide explains HOWTO contribute to :mod:`message_ix_model`
through GitHub pull requests (PRs).

First, **read the MESSAGEix documentation** on how to
:ref:`message-ix:contrib-pr`.
Most of the instructions written there also apply here.
This page only adds some more specific guidelines for :mod:`message_ix_models`.

.. contents::
   :local:
   :backlinks: none

Create a draft PR
=================

Create a branch, push the branch, and open a draft pull request *as soon as possible*.
The branch does not need to be complete or tidy in order to do this.
By doing so:

- Colleagues can be aware of your work.
  This means they can:

  - Let you know if they would like to use the tools you are building.
  - Give feedback or point you to information that can make your work easier.
  - Avoid duplicating efforts by writing code that does the same thing as yours.
  - Avoid making conflicting changes to the same file(s).

- The PR provides a natural home for discussion about sub-tasks and the scope of work.
- Maintainers can be aware of upcoming review work.

.. _template:

Fill out the description template
=================================

Again, read the
:ref:`MESSAGEix contributing page <message-ix:contrib-pr>`
that describes PR templates and how to use them.
This section describes each item in the :mod:`message_ix_models` template.

Pull request title
   Use a short, declarative statement similar to a commit message.

   For example: “Change [thing X] to [fix solve bug|enable feature Y]”

General description
   The reader **should** be able to read the first sentence
   and answer the question: “Is this relevant to me? Should I read on?”

   The next sentence(s) or paragraph **should** allow a reviewer
   to understand the scope/contents of the PR within a few minutes.

   After this, it is appropriate to add more detailed
   and possibly extensive
   information about specific changes in the PR.
   Markdown formatting (lists, flat or nested; headings)
   can be used to separate the description into sections,
   or to separate old information
   (for instance, if the implementation approach changes in the course of a PR).

   You can use @-mentions to make people aware of the work being done in the PR,
   without yet or necessarily requesting their review.

“How to review” section
   Here you **must** describe specific things that reviewer(s) should do
   in order to ensure that the PR achieves its goal.
   The reviewers **should** be able to carry out these tasks,
   and then give an “Approve” or “Request changes” based on what they see.

   Some example instructions:

   - “Read the diff.”
   - “Note that the CI checks all pass.”
   - “Run [specific code snippet or command] and check the output.”
   - “Look at [specific page] in the ReadTheDocs preview build of the documentation.”
   - “Ensure that [specific changes/additions] are self-documenting,”
     —that is, that another developer (someone like the reviewer)
     will be able to understand what the code does in the future.

   If no review is required for some or all of the changes,
   write “No review:” and describe why.

   If you request review from **2 or more people**
   (for example, code owners for multiple different files you modify),
   be clear whether each reviewer should look at specific aspects
   of the changes in the PR, or all reviewers should review in the same way.

   If possible, provide an estimate of the time required to review.
   This can help indicate whether a reviewer should be fast or thorough.

“PR checklist” section
  As written in the MESSAGEix docs:
  **do not** remove items from this section.
  Use strikethrough to make a clear and positive indication
  that part or all of an item is not relevant for the PR.

  The specific items are:

  - “Continuous integration checks all ✅”

    This item is always **required**.

  - “Add or expand tests; coverage checks both ✅”

    This is **required** in order to pass the ``codecov/patch``
    and ``codecov/project`` checks.
    For changes solely to documentation, CI configuration, etc.,
    the first part can be struck out, and the coverage checks should pass.

  - “Add, expand, or update documentation.”

    This is **required** if the PR results in changes
    to user-facing behaviour,
    for instance new features or fixes to existing behaviour.

  - “Update doc/whatsnew”

    Same conditions as the previous item.
    Described below.

Complete your contribution
==========================

Depending on the nature of the contribution in your PR,
some or all of the following sections will be relevant.

Add tests
---------

`Test-driven development <https://en.wikipedia.org/wiki/Test-driven_development>`_
is the idea of creating tests *before* or *with* the code that is to be tested.
It can substantially simplify and speed up development.

**Create a test module** under :file:`message_ix_models/tests/`
in a directory or folder that mirror the existing or planned structure
of the code you will modify in the PR.
Then **add or modify** tests that run the target code.
These tests express the following intentions:

- “I should be able to call this function/method in *this* way,
  with *these* arguments.”
- “It should run without error.”
- “It should return *this* value/data structure, with *these* properties.”

This creates a very clear target for then implementing the function(s)
or related code.
Use features built in to your editor or other utility programs
to **automatically run the tests** as you implement.

Add Python submodules
---------------------

Add one or more Python submodules in the tree,
following the :ref:`organization scheme <code-org>`.

- **Prefer a flat hierarchy**:

  - :file:`projects/{name}/{submodule}.py`
    instead of :file:`projects/{name}/{submodule}/__init__.py`
    as the sole file in a directory;
  - :file:`tools/thing.py`
    instead of :file:`tools/multiple/directories_with_long_names/thing.py`.
- **Avoid duplication** in fully-qualified names.
  For example,
  avoid a combination of module and function names like :py:`message_ix_models.tools.pizza_baker.pizza.bake_pizza()`;
  instead aim for :py:`message_ix_models.tools.pizza.bake()`.
- **Re-use and properly locate utility code**.

  When developing code for a module like :py:`message_ix_models.project.foo`,
  :py:`message_ix_model.model.foo`,
  or :py:`message_ix_model.tools.too`,
  any utility classes, functions, and variables can be placed in a :py:`.util` submodule,
  for instance at :py:`message_ix_models.project.foo.util.do_something()`.

  - First check :py:`message_ix_models.util` for similar utility code.
    See “DRY”, below
  - Also search the code base for similar utility code in other submodules
    of :py:`.model`, :py:`.project`, or :py:`.tools`.
    These can be moved to a higher level in the hierarchy
    and then used in your new code and their original submodule.

- **Don't repeat yourself (DRY).**

  Find, learn to use, and build on or improve existing code.
  Avoid reimplementing; if possible, extend or generalize by adding arguments.

  For example, :class:`.Context` and the top-level command-line (:meth:`message_data.cli.main`) handle the ``--url`` argument to identify a target platform, model, scenario, and version for code to operate on.
  Use these instead of adding similar parameters to subcommands.
  Refer to already existing code for argument or option naming conventions.

Choose locations for data
-------------------------

See :ref:`data-goes-where`.

Make tools available via the command line
-----------------------------------------

Instead of a file like :file:`…/project/foo/script.py`
that is invoked like :program:`python script.py`…

**Add a submodule** named :py:`.cli` alongside your code,
for instance :py:`message_ix_models.project.foo.cli`.
Then create a function named :py:`cli()` decorated with :func:`.click.command`:

.. code-block:: python

    import click

    @click.command("foo")
    @click.pass_obj
    def cli(context):
        # Run "foo" code from the command line
        # ...

See current code for examples,
and the `click documentation <https://click.palletsprojects.com>`_
for how to add options and arguments to the command.
Provide unambiguous help text for every parameter.

In :file:`message_ix_models/cli.py`,
modify the variable :py:`submodules`,
adding the module containing new commands.

If new top-level commands or options are added,
update the example :program:`mix-models --help` output
in :file:`doc/cli.rst`.

Add documentation
-----------------

See :ref:`repro-doc`.
This **should** be done *as code is written*.
If left until a project is complete,
it is much more difficult to make time to complete it.

If you have already opened a draft PR,
the integration with ReadTheDocs will automatically run,
building a preview of the modified docs.
Inspect the preview and the log output
to ensure your additions render correctly.

Update :file:`doc/whatsnew.rst`
-------------------------------
**Edit the file**, adding a single line
at the *top* of the “Next release” section similar to:

.. code-block:: rst

   - PR title or single-sentence description from above (:pull:`999`).
     Further details and explanation.

…where '999' is the GitHub pull request number:

**Commit** with a message like “Add #999 to doc/whatsnew”

Communicate about your PR
=========================

There are many points in the lifecycle of a PR
where you can choose to communicate with co-developers,
potential users, reviewers, maintainers, and others.
Some of these communications happen *automatically*:
for example,

- when a PR is switched out of draft status,
  the targeted reviewers get a notification.
- if a person is @-mentioned in a comment or PR description,
  they get a notification.

Other communications can happen online or in person.
These can help people be aware of your contribution
and the inputs they can give you to help complete it.
For example, if you tell a colleague:
“I have started PR #1234, and I think I will probably ask you
to review it in 2 weeks,”
then they can both loosely monitor the PR progress
and make ready to do the review.
