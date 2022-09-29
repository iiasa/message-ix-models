Reproducibility
***************

On this page:

.. contents::
   :local:
   :backlinks: none

Elsewhere:

- A `high-level introduction <https://paul.kishimoto.name/2021/06/issst/>`_, to how testing supports validity, reproducibility, interoperability, and reusability, in :mod:`message_ix_models` and related packages.
- :doc:`api/testing` (:mod:`message_ix_models.testing`), on a separate page.

Strategy
========

The code in :mod:`.model.bare` generates a “bare” reference energy system.
This is a Scenario that has the same *structure* (ixmp 'sets') as actual instances of the MESSAGEix-GLOBIOM global model, but contains no *data* (ixmp 'parameter' values).
Code that operates on the global model can be tested on the bare RES; if it works on that scenario, this is one indication (necessary, but not always sufficient) that it should work on fully-populated scenarios.
Such tests are faster and lighter than testing on fully-populated scenarios, and make it easier to isolate errors in the code that is being tested.

Test suite (:mod:`message_ix_models.tests`)
===========================================

:mod:`message_ix_models.tests` contains a suite of tests written using `Pytest <https://docs.pytest.org/>`_.

The following is automatically generated documentation of all modules, test classes, functions, and fixtures in the test suite.
Each test **should** have a docstring explaining what it checks.

.. currentmodule:: message_ix_models

.. autosummary::
   :toctree: _autosummary
   :template: autosummary-module.rst
   :recursive:

   tests

Running the test suite
======================

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

Continuous testing
==================

The test suite (:mod:`message_ix_models.tests`) is run using GitHub Actions for new commits on the ``main`` branch, or on any branch associated with a pull request.

Because it is closed-source and requires access to internal IIASA resources, including databases, continuous integration for :mod:`message_data` is handled by an internal server running `TeamCity <https://www.jetbrains.com/teamcity/>`_: https://ene-builds.iiasa.ac.at/project/message (requires authorization)


.. _export-test-data:

Prepare data for testing
========================

Use the ``export-test-data`` CLI command::

  mix-models --url="ixmp://ixmp-dev/ENGAGE_SSP2_v4.1.7/baseline" export-test-data

See also the documentation for :func:`export_test_data`.
Use the :command:`--exclude`, :command:`--nodes`, and :command:`--techs` options to control the content of the resulting file.
