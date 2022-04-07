MESSAGEix-Buildings
*******************

MESSAGEix-Buildings refers to a set of models including a specific configuration of MESSAGEix-GLOBIOM.

Code is maintained in the `iiasa/MESSAGE_Buildings <https://github.com/iiasa/MESSAGE_Buildings>`_ repository.

Usage
=====

1. Clone the main MESSAGE_Buildings repo, linked above.
   Note the path.
2. Invoke the model using, for example:

   .. code-block::

       mix-models --platform=ixmp_dev buildings /path/to/mb/repo

.. todo::
   Currently, the CLI command uses hard-coded values for the model and scenario name(s).
   Remove these.
   This will allow invocation by an explicit command like:

   .. code-block::

      mix-models --url="ixmp://ixmp-dev/model name/base scenario" \
                 buildings \
                 --dest="ixmp://ixmp-dev/new model name/target scenario"

Code reference
==============

.. currentmodule:: message_data.model

.. autosummary::
   :toctree: _autosummary
   :template: autosummary-module.rst
   :recursive:

   buildings.cli
