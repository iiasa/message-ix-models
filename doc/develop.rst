Development practices
*********************

This page describes development practices for :mod:`message_ix_models` and :mod:`message_data` intended to help reproducibility, interoperability, and reusability.

In the following, the bold-face words **required**, **optional**, etc. have specific meanings as described in `IETF RFC 2119 <https://tools.ietf.org/html/rfc2119>`_.

On other pages:

- :doc:`message-ix:contributing` in the MESSAGEix docs.
  *All* of these apply to contributions to :mod:`message_ix_models`
  and :mod:`message_data`,
  in particular the :ref:`message-ix:code-style`.
- :doc:`howto/index` including detailed guides for some development tasks.
- :doc:`data` that explains types of data and how they are handled.

On this page:

.. contents::
   :local:
   :backlinks: none

.. _check-support:

Advertise and check compatibility
=================================

There are multiple choices of the base structure for a model in the MESSAGEix-GLOBIOM family, e.g. different :doc:`pkg-data/node` and :doc:`pkg-data/year`.

Code that will only work with certain structures…

- **must** be documented, and include in its documentation any such limitation, e.g. “:func:`example_func` only produces data for R11 and year list B.”
- **should** use :func:`.check_support` in individual pieces of code to pre-emptively check and raise an exception.
  This prevents inadvertent use of the code where its data will be invalid:

.. code-block:: python

    def myfunc(context, *args):
        """A function that only works on R11 and years ‘B’."""

        check_support(
            context,
            dict(regions=["R11"], years=["B"]),
            "Example data produced"
        )

        # … function code to execute if the check passes

Code **may** also check a :class:`.Context` instance and automatically adapt data from certain structures to others, e.g. by interpolating data for certain periods or areas.
To help with validation, code that does this **should** log on the :data:`logging.INFO` level to advertise these steps.

.. _code-owners:

Code owners
===========

The file :file:`.github/CODEOWNERS` (`on GitHub <https://github.com/iiasa/message-ix-models/blob/main/.github/CODEOWNERS>`__) indicates ‘owners’ for some files in the repository.
See `GitHub's documentation of this feature <https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/customizing-your-repository/about-code-owners>`__.
For :mod:`message_ix_models`, we use this to designate people who are capable and responsible to evaluate whether changes in a pull request would have any impact on current or planned research applications of that code, and to suggest whether and how to adjust PRs.

- As of 2025-01-10, we **do not require** pull request approvals from code owners on every PR that modifies files they own.
  Owners only are notified of such PRs.
  The author of a PR **should**:

  - Observe the notified owners, if any.
  - In the "How to review" section of the PR template, address those people individually with what (if anything) they need to look at as part of the PR.
    This **may** entail saying, "@owner-a @owner-b: no need to review because <reasons>".

- Groups of entries **should** include paths to all of the following, where applicable:

  - Documentation, for instance :file:`/doc/{name}` or :file:`/doc/project/{name}.rst`
  - Data, for instance :file:`/message_ix_models/data/{name}`
  - Code, for instance :file:`/message_ix_models/model/{name}` or :file:`/message_ix_models/project/{name}`
  - Tests, for instance :file:`/message_ix_models/tests/model/{name}` or :file:`/message_ix_models/tests/project/{name}`.

- At least 2 people (individually, or via a GitHub team) **should** be designated owners for any file.
  This may include one ‘active’ owner and a ‘backup’, or two or more active owners, etc.

- For any pull request thats add new files to :mod:`message_ix_models`, the author(s) and reviewer(s) **should**:

  - Consider whether the new files have an identifiable owner.
    This may not be the case, for instance for general-purpose utility code.
  - Check whether this understanding aligns with the ownership expressed in :file:`CODEOWNERS`.
  - Add, remove, or adjust entries accordingly.
  - Describe these changes in commit message(s) or their PR description.

- If code owners depart IIASA or are reassigned to other work, they or the :mod:`message_ix_models` maintainers **must** initiate a discussion to identify a new set of owners for their files.

Organization
============

This section describes the organization or layout
of the :mod:`message_ix_models` repository and Python package.
The organization of :mod:`message_data` is roughly similar,
with some differences as noted below.
(See also :doc:`howto/migrate`
for the relationship between this repo and :mod:`message_data`.)

.. _repo-org:

Repository
----------

:file:`message_ix_models/`
   (or :file:`message_data/`)

   This directory contains a Python package,
   thus *code* that creates or manipulates MESSAGE Scenarios,
   or handles data for these tasks.
   See :ref:`code-org` below.

:file:`message_ix_models/data/`
   This directory contains :doc:`data`,
   in particular metadata and input data used by code.
   No code is kept in this directory;
   code **must not** be added.
   Code **should not** write output data (for example, reporting output)
   to this directory.

   Some of these files are packaged with the :mod:`message_ix_models` package
   that is published on PyPI,
   thus are available to any user who installs the package from this source.

   In :mod:`message_data`,
   a directory :file:`data/` at the *top level* is used instead.
   Similarly, code cannot be kept in this directory;
   only code under :file:`message_data/` can be imported
   using :py:`from message_data.submodule.name import x, y, z`.

:file:`doc/`
   The source reStructuredText files for this **documentation**,
   and Sphinx configuration (:file:`doc/conf.py`) to build it.

:file:`reference/`
   (:mod:`message_data` only)

   Static files not used *or* produced by the code,
   but provided for reference as a supplement to the documentation.

.. _code-org:

Code
----

The code is organized into submodules.
The following high-level overview
explains how the organization relates to MESSAGEix-GLOBIOM modeling workflows.
Some general-purpose modules, classes, and functions are not mentioned;
for these, see the table of contents.
See also :ref:`modindex` for an auto-generated list of all modules.

Models (:mod:`message_ix_model.model`)
   **Code that creates models or model variants.**
   MESSAGEix-GLOBIOM is a *family* of models
   in which the “reference energy system”
   (RES; with specific sets and populated parameter values)
   is similar, yet not identical.
   Many models in the family are defined as *derivatives* of other models.

   For example: :mod:`message_ix_models.model.transport` does not create an RES
   in an empty :class:`.Scenario`, from scratch.
   Instead, it operates on a ‘base’ model
   and produces a new model
   —in this case, with additional transport sector technologies.

   In the long run (see :ref:`Roadmap`),
   :mod:`message_ix_models.model` will contain a script
   that recreates a **‘main’** (single, authoritative) MESSAGEix-GLOBIOM RES,
   from scratch.
   Currently, this script does not exist,
   and this ‘main’ RES is itself derived from particular models and scenarios
   stored in the shared IIASA ECE database.
   These were previously from the CD-LINKS project,
   and more recently from the ENGAGE project.
   See :doc:`m-data:reference/model`.

   In the private package, :mod:`message_data.model` also contains:

   - A *general-purpose* :class:`~.model.scenario_runner.ScenarioRunner`
     *class* to manage and run interdependent sets of Scenarios.
   - A runscript for a *standard scenario set*,
     based on the scenario protocol of the :doc:`CD-LINKS <m-data:reference/projects/cd_links>` project;
     see below.

:ref:`index-projects` (:mod:`message_ix_models.project`)
   **Code to create, run, and analyse scenarios
   for specific research projects.**
   Research projects using MESSAGEix-GLOBIOM often involve a “scenario protocol.”
   This is some description of a set of multiple Scenarios with the same
   (or very similar) structure,
   and different parametrizations
   that represent different policies, modeling assumptions, etc.

   Each submodule of :mod:`message_ix_models.project`
   (for example, :mod:`message_ix_models.project.navigate`) corresponds to a single research project,
   and contains tools needed to execute the **project workflow**.
   In some cases these are in the form of :class:`.Workflow` instances,
   in other cases as ‘runscripts’.
   Workflows usually have roughly the following steps:

   1. **Start** with one of the :class:`Scenarios <.Scenario>` created by :mod:`message_ix_models.model`.
   2. **Build** a set of scenarios from this base,
      by applying various code in :mod:`message_ix_models` and :mod:`message_data`,
      with various configuration settings and input data.
   3. **Solve** each scenario generated in step 2.
   4. **Report** the results.

   (Sometimes steps 2 and 3 are ‘chained’,
   with some scenarios being derived from the solution data
   of earlier scenarios.)

:doc:`Reporting and post-processing <api/report/index>` (:mod:`message_ix_models.report`)
   This module builds on :mod:`message_ix.report` and :mod:`ixmp.report`
   to provide general-purpose reporting functionality
   for MESSAGEix-GLOBIOM family models.

   This base reporting corresponds to the ‘main’ RES,
   and is extended by :mod:`message_ix_models.model` submodules
   to cover features of particular model variants;
   or by :mod:`message_ix_models.project` submodules to cover variables
   or output formats needed for particular projects.

   The module was previously at :py:`message_data.reporting`.

:doc:`Tools <api/tools>` (:mod:`message_ix_models.tools`)
   This submodule contains **higher-level** tools
   that perform operations tailored to the structure of MESSAGEix-GLOBIOM and MESSAGE,
   or to particular upstream data sources and their formats.
   These are *used by* code in submodules of :mod:`.model` or :mod:`.project`,
   but generally not vice versa.

:doc:`Utilities <api/util>` (:mod:`message_ix_models.util`)
   This submodule contains a collection of **lower-level**,
   general-purpose programming utilities
   that can be used across the rest of the code base.
   These include convenience wrappers and extensions for basic Python,
   :mod:`pandas`, and other upstream packages.

:ref:`repro-testing` (:mod:`message_ix_models.tests`)
   The test suite is arranged in modules and submodules
   that correspond to the code layout.
   For example:
   a function named :py:`do_thing()` in a module :py:`message_ix_models.project.foo.report.extra`
   will be tested in a module :py:`message_ix_models.tests.project.foo.report.test_extra`,
   and a function named :py:`test_do_thing()`.
   This arrangement makes it easy to locate the tests for any code,
   and vice versa.

:doc:`Test utilities and fixtures <api/testing>` (:mod:`message_ix_models.testing`)
   These are both low-level utilities and high-level tools
   specifically to be used within the test suite.
   They are *not* used anywhere besides :mod:`message_ix_models.tests`.

.. _policy-upstream-versions:

Upstream version policy
=======================

:mod:`message_ix_models` is developed to be compatible with the following versions of its upstream dependencies.

:mod:`ixmp` and :mod:`message_ix`

   The most recent 4 minor versions, or all minor versions released in the past two (2) years—whichever is greater.

   For example, as of 2024-04-08:

   - The most recent release of :mod:`ixmp` and :mod:`message_ix` are versions 3.8.0 of each project.
     These are supported by :mod:`message_ix_models`.
   - The previous 3 minor versions are 3.7.0, 3.6.0, and 3.5.0.
     All were released since 2022-04-08.
     All are supported by :mod:`message_ix_models.`
   - :mod:`ixmp` and :mod:`message_ix` versions 3.4.0 were released 2022-01-24.
     These this is the fifth-most-recent minor version *and* was released more than 2 years before 2024-04-08, so it is not supported.

Python
   All currently-maintained versions of Python.

   The Python website displays a list of these versions (`1 <https://www.python.org/downloads/>`__, `2 <https://devguide.python.org/versions/#versions>`__).

   For example, as of 2024-04-08:

   - Python 3.13 is in "prerelease" or "feature" development status, and is *not* supported by :mod:`message_ix_models`.
   - Python 3.12 through 3.8 are in "bugfix" or "security" maintenance status, and are supported by :mod:`message_ix_models`.
   - Python 3.7 and earlier are in "end-of-life" status, and are not supported by the Python community or by :mod:`message_ix_models`.

- Support for older versions of dependencies **may** be dropped as early as the first :mod:`message_ix_models` version released after changes in upstream versions.

  - Conversely, some parts of :mod:`message_ix_models` **may** continue to be compatible with older upstream versions, but this compatibility is not tested and may break at any time.
  - Users **should** upgrade their dependencies and other code to newer versions; we recommend the latest.
- Some newer code is marked with a :func:`.minimum_version` decorator.

  - This indicates that the marked code relies on features only available in certain upstream versions (of one of the packages mentioned above, or another package), newer than those listed in `pyproject.toml <https://github.com/iiasa/message-ix-models/blob/main/pyproject.toml>`__.
  - These minima **must** be mentioned in the :mod:`message_ix_models` documentation.
  - Users wishing to use this marked code **must** use compatible versions of those packages.

.. _roadmap:

Roadmap
=======

The `message-ix-models Github repository <https://github.com/iiasa/message-ix-models>`_ hosts:

- `Current open issues <https://github.com/iiasa/message-ix-models/issues>`_,
  arranged using many `labels <https://github.com/iiasa/message-ix-models/labels>`_.
- `Project boards <https://github.com/iiasa/message-ix-models/projects>`_ for ongoing efforts.

Documentation for particular submodules,
for instance :mod:`message_ix_models.model.material`,
may contain module- or project-specific roadmaps and change logs.

The same features are available for the private :mod:`message_data` repository:
`issues <https://github.com/iiasa/message_data/issues>`_,
`labels <https://github.com/iiasa/message_data/labels>`_, and
`project boards <https://github.com/iiasa/message_data/projects>`_.

Inline TODOs
------------

This is an automatically generated list of every place
where a :code:`.. todo::` directive is used in the documentation,
including in function docstrings.

.. todolist::
