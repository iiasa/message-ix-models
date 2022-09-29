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
=============

The class :class:`.buildings.Config` defines all the options to which the code responds, as well as default values.
Values given in code or on the command line will override these.

.. autoclass:: message_data.model.buildings.Config
   :members:

Code reference
==============

.. currentmodule:: message_data.model

.. autosummary::
   :toctree: _autosummary
   :template: autosummary-module.rst
   :recursive:

   buildings.cli
