MESSAGEix-Buildings
*******************

MESSAGEix-Buildings refers to a set of models including a specific configuration of MESSAGEix-GLOBIOM.

Code is maintained in the `iiasa/MESSAGE_Buildings <https://github.com/iiasa/MESSAGE_Buildings>`_ repository.

.. _buildings-usage:

Usage
=====

1. Clone the main MESSAGE_Buildings repo, linked above.
   Note the path.

2. Invoke the model using, for example:

   .. code-block::

      mix-models \
        --url="ixmp://ixmp-dev/model name/base scenario" \
        buildings \
        /path/to/mb/repo \
        build-solve \
        --dest="ixmp://ixmp-dev/new model name/target scenario"

3. Run the reporting:

   .. code-block::

      mix-models \
        --url="ixmp://ixmp-dev/new model name/target scenario" \
        report -m model.buildings "buildings all"


Base and destination scenarios
------------------------------

The following correspond to :file:`MESSAGE-BUILDINGS.py`, given with the comment “BM NPi (after "run_cdlinks_setup" for NPi). This has MACRO but here run MESSAGE only.”

.. code-block::

    mix-models \
      --url="ixmp://ixmp-dev/MESSAGEix-GLOBIOM 1.1-BM-R12-NGFS/NPi2020-con-prim-dir-ncr" \
      buildings \
      /path/to/mb/repo \
      build-solve \
      --dest="ixmp://ixmp-dev/MESSAGEix-GLOBIOM 1.1-BM-R12-NGFS/NPi2020-con-prim-dir-ncr-building"

If using ``clim_scen="2C"``, the following scenario is used as the reference scenario for prices:

.. code-block::

    mix-models \
      --url="ixmp://ixmp-dev/MESSAGEix-GLOBIOM 1.1-BM-R12-NGFS/NPi2020-con-prim-dir-ncr" \
      buildings \
      /path/to/mb/repo \
      build-solve \
      --climate-scen="ENGAGE_SSP2_v4.1.8/EN_NPi2020_1000f" \
      --dest="ixmp://ixmp-dev/MESSAGEix-GLOBIOM 1.1-BM-R12-NGFS/NPi2020-con-prim-dir-ncr-building"

The following is commented in :file:`MESSAGE-BUILDINGS.py` with the comment “NOTE: this scenario has the updated GLOBIOM matrix.”

.. code-block::

    mix-models \
      --url="ixmp://ixmp-dev/MESSAGEix-GLOBIOM 1.1-M-R12-NGFS/baseline" \
      buildings \
      /path/to/mb/repo \
      build-solve \
      --dest="ixmp://ixmp-dev/MESSAGEix-GLOBIOM 1.1-BM-R12-NGFS/baseline"

The following correspond to :file:`MESSAGE-BUILDINGS-STURM.py`:

.. code-block::

    mix-models \
      --url="ixmp://ixmp-dev/MESSAGEix-GLOBIOM 1.1-M-R12-NGFS/baseline" \
      buildings \
      /path/to/mb/repo \
      build-solve \
      --dest="ixmp://ixmp-dev/MESSAGEix-GLOBIOM 1.1-BM-R12-EFC/baseline"

and:

.. code-block::

    mix-models \
      --url="ixmp://ixmp-dev/MESSAGEix-GLOBIOM 1.1-M-R12-NGFS/baseline" \
      buildings \
      /path/to/mb/repo \
      build-solve \
      --climate-scen="MESSAGEix-GLOBIOM 1.1-M-R12-NGFS/EN_NPi2020_1000" \
      --dest="ixmp://ixmp-dev/MESSAGEix-GLOBIOM 1.1-BM-R12-EFC/baseline"

The following corresponds to :file:`reporting_EFC.py`:

.. code-block::

    mix-models \
      --url="ixmp://ixmp-dev/MESSAGEix-GLOBIOM 1.1-BM-R12-EFC/baseline#24" \
      buildings \
      /path/to/mb/repo \
      report


Configuration
-------------

The code responds to the following keys in ``context["buildings"]``, a :class:`dict` within the :class:`Context`.
Currently, these can be set in the :mod:`.buildings.cli` Python source file, in the variable :data:`DEFAULTS`.

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
     - Climate scenario.
       If :prog:`--climate-scen` is given on the command line, this is set to "2C" automatically.
   * - **max_iteration**
     - 10
     - Maximum number of iterations; set to 1 for once-through mode.
   * - **solve_macro**
     - :obj:`False`
     - Solve scenarios using MESSAGE-MACRO (:obj:`True`) or only MESSAGE.
   * - **clone**
     - :obj:`True`
     - Clone the scenario to be used from a base scenario (:obj:`True`) or load and use an existing scenario directly.
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
