Reproducibility
***************

On this page:

.. contents::
   :local:
   :backlinks: none

Elsewhere:

- A `high-level introduction <https://paul.kishimoto.name/2021/06/issst/>`_ to how testing supports validity, reproducibility, interoperability, and reusability in :mod:`message_ix_models` and related packages.
- :doc:`api/testing` (:mod:`message_ix_models.testing`).
- :doc:`data` for information about reproducible handling of data, both private and public.

.. _repro-doc:

Documentation
=============

Documentation serves different purposes for completed vs. ongoing work:

- For *completed* work, the documentation **must** allow a reader to understand what was done, replicate or reproduce results, and/or reuse code and/or data.
- For *ongoing* work, the documentation **must** allow colleagues to locate and understand the state of current and planned work.

Projects
--------
Every distinct project for which MESSAGEix-GLOBIOM scenarios and outputs are used **must** be included in the documentation.
The contents **may** be brief or extensive, as appropriate.

See :doc:`HOWTO Document research projects </howto/doc-project>` for a complete guide.

Other code
----------

Docstrings for general-purpose code and functions **should** explain clearly to which data (including scenario(s)) the code is *or* is not applicable.
Code **may** also check explicitly and raise informative Python exceptions if the target data/scenario is not supported.

These allow others to understand when the code:

- can be (re)used without modification,
- can be modified or extended to support new uses, or
- can or should not be used.

.. _repro-testing:

Testing
=======

In addition to atomic/unit tests of individual functions, multiple strategies **may** be used to ensure code works on intended target MESSAGEix-GLOBIOM base scenarios.

- The code in :mod:`.model.bare` generates a **“bare” reference energy system**.
  This is a Scenario that has the same *structure* (ixmp 'sets') as actual instances of the MESSAGEix-GLOBIOM global model, but contains no *data* (ixmp 'parameter' values).
  Code that operates on the global model can be tested on this bare RES; if it works on that scenario, this is one indication (necessary, but not always sufficient) that it should work on fully-populated scenarios.
- :doc:`model/snapshot` can be used as target for tests.

Such tests are faster and lighter than testing on fully-populated scenarios and make it easier to isolate errors in the code that is being tested.

.. _test-suite:

Test suite (:mod:`message_ix_models.tests`)
-------------------------------------------

:mod:`message_ix_models.tests` contains a suite of tests written using `Pytest <https://docs.pytest.org/>`_.

The following is automatically generated documentation of all modules, test classes, functions, and fixtures in the test suite.
Each test **should** have a docstring explaining what it checks.

.. currentmodule:: message_ix_models

.. autosummary::
   :toctree: _autosummary
   :template: autosummary-module.rst
   :recursive:

   tests

Run the test suite
------------------

Some notes for running the test suite.

:func:`.cached` is used to cache the data resulting from slow operations, like parsing large input files.
Data are stored in a location described by the :class:`.Context` setting ``cache_path``.
The test suite interacts with caches in two ways:

- ``--local-cache``: Giving this option causes pytest to use whatever cache directory is configured for normal runs/usage of :mod:`message_ix_models` or :command:`mix-models`.
- By default (without ``--local-cache``), the test suite uses :ref:`pytest's built-in cache fixture <pytest:cache>`.
  This creates and uses a temporary directory, usually :file:`.pytest_cache/d/cache/` within the repository root.
  This location is used *only* by tests, and not by normal runs/usage of the code.

In either case:

- The tests use existing cached data in these locations and skip over code that generates this data.
  If the generating code is changed, the cached data **must** be deleted in order to actually check that the code runs properly.
- Running the test suite with ``--local-cache`` causes the local cache to be populated, and this will affect subsequent runs.
- The continuous integration (below) services don't preserve caches, so code always runs.

.. _ci:

Continuous testing
------------------

The test suite (:mod:`message_ix_models.tests`) is run using GitHub Actions for new commits on the ``main`` branch; new commits on any branch associated with a pull request; and on a daily schedule.
These ensure that the code is functional and produces expected outputs, even as upstream dependencies evolve.
Workflow runs and their outputs can be viewed `here <https://github.com/iiasa/message-ix-models/actions/workflows/pytest.yaml>`__.

Because it is closed-source and requires access to internal IIASA resources, including databases, continuous integration for :mod:`.message_data` is handled by GitHub Actions `self-hosted runners <https://docs.github.com/en/actions/hosting-your-own-runners>`__ running on IIASA systems.

.. _export-test-data:

Prepare data for testing
------------------------

Use the ``export-test-data`` CLI command::

  mix-models --url="ixmp://ixmp-dev/ENGAGE_SSP2_v4.1.7/baseline" export-test-data

See also the documentation for :func:`export_test_data`.
Use the :command:`--exclude`, :command:`--nodes`, and :command:`--techs` options to control the content of the resulting file.

.. _versioning:

Versioning and naming
=====================

The :mod:`message_ix_models` code, as of any commit, can generate many different :mod:`message_ix` scenarios with different structure, parametrization, etc. and solve them in different ways, yielding different results.
In order to uniquely identify scenarios and enable reproduction of their results, users **must** record:

1. A specific commit of the :mod:`message_ix_models` *code* and (if it is used) the :mod:`message_data` code.

   - These are most easily checked using the command :program:`message-ix show-versions`; copy and store the entire result in a text file.
   - Specific `releases of message-ix-models <https://github.com/iiasa/message-ix-models/releases>`_ always correspond exactly to a particular commit; giving a release version is sufficient to identify a commit. [1]_
   - Commits **may** be on a branch other than ``main``; however commits on ``main`` receive the most active maintenance and **should** be preferred.

2. Exact *CLI command(s)* or Python function(s) that is/are run to generate the scenario(s), and
3. Optional *configuration file(s)*.

   These **should** be committed to the repository (1) and **should** be mentioned as command-line arguments (2) as necessary.
   Input data sources, and versions thereof, **must** be specified in the same way.
4. The system on which the command(s) (2) were run.

For example:

  “Using ``message_ix_models 2020.6.21.dev0+g7e59382`` (see also [file] with complete output from :program:`message-ix show-versions`), the command :program:`mix-models --url="ixmp://ene-ixmp/CD_LINKS_SSP2/baseline" transport build` was run on ``hpg914.iiasa.ac.at``.”

This specifies (1), (2), and (4); since no configuration file is mentioned, then for (3) it is implied that the default configuration file(s) as of this version of :mod:`message_ix_models` are used.

Any ‘base’ scenario used as a starting point to build other scenarios **must** be specified via one of (3), (2), or (1)—in that order of preference.

.. [1] Unlike :mod:`ixmp` and :mod:`message_ix`, the packages :mod:`message_ix_models` and :mod:`message_data` *do not* use semantic versioning.
   This is because the notion of “(dis)similarity of different MESSAGEix-GLOBIOM parametrizations” does not map to the notion of “software API compatibility” that is the basis of semantic versioning.

External model names
--------------------

The :mod:`ixmp` data model uniquely identifies scenarios by the triple of (model name, scenario name, version).

In other contexts, “external” model names are used; for instance, in data submitted to model comparison projects using the IAMC data structure—‘version’ is omitted, or not accepted/reassigned by the receiving system.
In these cases, the “external” name:

- May be different from the ‘internally’ name used in IIASA ECE :mod:`ixmp` databases.
- Serves to label and identify MESSAGEix-GLOBIOM model data in contexts where it is compared with other scenarios.
- *Does not*, on its own, suffice to identify the materials and steps to reproduce a scenario.

External model names **must** be recorded as corresponding to specific internal (model name, scenario name, version) identifiers.
This **should** be done by recording scenario URLs.

External model names **should** follow the scheme ``{name} {version}{postfix}``, for example ``MESSAGEix-GLOBIOM 2.0-R17-BT``, wherein the parts are:

name
   “MESSAGEix-GLOBIOM” by default.
   In some cases, certain variants which involve extensive changes to model structure may establish alternate names, for instance :mod:`.model.material`, :mod:`.model.transport`, or :mod:`.model.water`.

version
   is *distinct* from the commit ID, :mod:`message_ix_models` release, etc.
   This serves to identify distinct generations of the model or variant identified by ``name``. [2]_

   This part format follows a loose, reduced form of `semantic versioning <https://semver.org>`_, such that the first part is incremented for “major” changes and the latter for “minor” changes.
   There is no established rule, guideline, or heuristic for what kinds of changes are “minor” or “major”.
   Developers **must**:

   1. Initiate a discussion with colleagues about when to increment either the major or minor part.
   2. Record (below, or on a variant-specific documentation page) changes associated with an incremented version part.

postfix
   This **should** be omitted if the model structure does not differ from the structure given below for the corresponding ``{name} {version}``.

   It consists of one or more parts, each prefixed with a hyphen.
   In order, these **may** include:

   - Node code list, for example ``-R17``, to indicate spatial scope and resolution
   - Year (period) list, for example ``-A``, to indicate temporal scope and resolution.
   - Variants included, for example ``-M`` if using :mod:`.model.material`, or ``-MT`` if using :mod:`.model.material` and :mod:`.model.transport`.
   - Further parts for other structural changes that are not part of a variant with a one-letter code, for example ``-DACCS``.

   Project names or acronyms **should not** be used in the postfix.
   The postfix conveys information about the model structure, not about the purpose to which it is applied.
   Project information **may** be added to the scenario name.

.. [2] This means the sequence of ``version`` parts may be different for each ``name``.
   For instance, ``MESSAGEix-Materials 1.1`` does not necessarily have any correspondence to ``MESSAGEix-GLOBIOM 1.1``, ``MESSAGEix-GLOBIOM 2.0``, etc.


.. _model-names:

Some external model names include:

MESSAGE V.3
   .. todo:: Expand with a list of cases in which this model name has been used.

MESSAGEix-GLOBIOM_GEI 1.0
   .. todo:: Expand with a list of cases in which this model name has been used.

MESSAGEix-GLOBIOM 1.0
   .. todo:: Expand with a list of cases in which this model name has been used.

MESSAGEix-GLOBIOM 1.1
   This version is published as a :doc:`data snapshot <api/model-snapshot>`, and uses:

   - R11 :doc:`node list <pkg-data/node>`.
   - B :doc:`year (period) list <pkg-data/year>`.

   .. todo:: Expand with a list of cases in which this model name has been used.

MESSAGEix-GLOBIOM 1.2
   This version was used in <https://doi.org/10.1038/s41560-021-00904-8>, and is similar to MESSAGEix-GLOBIOM 1.1, but incorporates COVID effects and has annual timesteps from 2020 to 2025.

MESSAGEix-GLOBIOM 2.0
   Used for the 2023–2024 ScenarioMIP/SSP process.
   This configuration uses, by default:

   - R12 node list.
   - B year (period) list.
   - :mod:`.model.material`.

MESSAGEix-GLOBIOM 2.0-M-R12-NGFS
   Used for the NGFS project round in 2024.
