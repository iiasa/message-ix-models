Reproducibility
***************

Strategy
========

The code in :mod:`.model.bare` generates a “bare” reference energy system.
This is a Scenario that has the same *structure* (ixmp 'sets') as actual instances of the MESSAGEix-GLOBIOM global model, but contains no *data* (ixmp 'parameter' values).
Code that operates on the global model can be tested on the bare RES; if it works on that scenario, this is one indicator that it should work on fully-populated scenarios.
Such tests are faster and lighter than testing on fully-populated scenarios, and make it easier to isolate errors in the code that is being tested.

Test suite (:mod:`message_ix_models.tests`)
===========================================

:mod:`message_ix_models.tests` contains a suite of tests written using `Pytest <https://docs.pytest.org/>`_.

The following is automatically generated documentation of all modules, test classes, functions, and fixtures in the test suite.
Each test **should** have a docstring explaining what it checks for.

.. currentmodule:: message_ix_models

.. autosummary::
   :toctree: _autosummary
   :template: autosummary-module.rst
   :recursive:

   tests


Continuous testing
==================

The test suite (:mod:`message_ix_models.tests`) is run using GitHub Actions for new commits on the ``main`` branch, or on any branch associated with a pull request.
