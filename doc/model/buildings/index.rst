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

Configuration
-------------

The code responds to the following keys in ``context["buildings"]``, a :class:`dict` within the :class:`Context`.

.. list-table::
   :widths: 20 10 70
   :header-rows: 1

   * - Key
     - Value(s)
     - Description
   * - **ssp**
     - "SSP"
     - SSP scenario
   * - **clim_scen**
     - "BL", "2C"
     - Climate scenario
   * - **solve_macro**
     - :obj:`False`
     - Solve scenarios using MESSAGE-MACRO (:obj:`True`) or only MESSAGE.
   * - **clone**
     - :obj:`True`
     - Clone the scenario to be used from a base scenario (:obj:`True`) or load and use an existing sceanrio directly.
   * - **use ACCESS**
     - :obj:`True`
     - Run the ACCESS model on every iteration (experimental/untested)
   * - **code_dir**
     - —
     - Path to the MESSAGE_Buildings code and data; passed via the command line (above).

Code reference
==============

.. currentmodule:: message_data.model

.. autosummary::
   :toctree: _autosummary
   :template: autosummary-module.rst
   :recursive:

   buildings.cli
