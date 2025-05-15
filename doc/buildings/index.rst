MESSAGEix-Buildings
*******************

.. caution:: |gh-350|

MESSAGEix-Buildings refers to a set of models including a specific configuration of MESSAGEix-GLOBIOM.

Code is maintained in the `iiasa/MESSAGE_Buildings <https://github.com/iiasa/MESSAGE_Buildings>`_ repository.

Function
========

This section briefly describes how the contents of the MESSAGE_Buildings repo and :mod:`message_data` interact, as a guide to reading the code.

ACCESS and STURM
----------------

The MESSAGE_Buildings contains two models (collectively the **“buildings models”**):

- **ACCESS**: includes cooking end-use in the residential sector.
- **STURM**: includes some residential and other end-uses, as well as the construction and demolition of buildings.

“MESSAGEix-Buildings” is thus the combination of ACCESS, STURM, and MESSAGEix-GLOBIOM (i.e. an instance of MESSAGE).

Both of the buildings models require as input prices for energy commodities.
In MESSAGEix-Buildings, this is obtained from an existing MESSAGEix-GLOBIOM scenario.
They produce various outputs; the ones relevant to MESSAGEix-Buildings are described below.

Build and solve
---------------

There are two phases to ‘running’ MESSAGEix-Buildings.
These are handled by :func:`.buildings.build_and_solve`.

1. **Build.**
   This refers to building a MESSAGE scenario with structure capturing buildings technologies, commodities, etc., from a base scenario that lacks these features.
   During this process, data is also needed to parametrize these new technologies; this data is obtained as output from ACCESS and STURM.

   Thus, the order of operations is:

   1. Prices are retrieved from the base scenario.
   2. ACCESS is run, and its output stored temporarily in variables and/or files.
   3. STURM is run, and its output stored temporarily in variables and/or files.
   4. The base MESSAGE scenario is modified to add buildings structure and parameter data derived from (2) and (3).

   These steps are handled by :func:`.buildings.pre_solve`.

2. **Solve.**
   Because prices are endogenous in MESSAGE, solving the MESSAGE scenario produced by (1.4) can result in prices that are *different* from the ones provided to ACCESS and STURM in steps (1.2) and (1.3).

   For this reason, MESSAGEix-Buildings is set up to use the iterative solve feature of :meth:`ixmp.Scenario.solve`.
   :func:`.buildings.pre_solve` precedes the solution of the MESSAGE LP; afterwards :func:`.buildings.post_solve` computes a convergence criterion.

   If the activity levels (demand) have not converged, the iteration loop repeats, up until :attr:`.max_iterations`.

   .. note:: As of 2023-01-10, this is not in active use; the models are run in a once-through fashion with :attr:`.max_iterations` set to 1.
      See also the :ref:`NAVIGATE workflow <navigate-workflow>`, wherein a second iteration is run manually after a policy scenario is solved.


Report
------

Reporting for MESSAGEix-Buildings involves the following pieces:

1. In the “Build” phase above, STURM is run produces its own reporting output files.
   These are *different* from temporary files used in step (1.4) above to set up the buildings details.

   ACCESS does not produce reporting output files.

2. :mod:`.buildings.report` contains (:mod:`genno`-based) reporting code (:func:`.buildings.report.callback`), i.e. extending the built-in :doc:`/reference/reporting` features of :mod:`message_data`, :mod:`message_ix`, and :mod:`ixmp`.

   As with the other reporting, this is decomposed into multiple functions arranged in a graph of tasks.
   *Inter alia*, computing the key ``buildings all`` will:

   - Read the STURM output files and subset some contents, e.g. service levels.
   - Compute custom aggregates based on standard :mod:`message_data` reporting, e.g. for final energy.
   - Transform these into the IAMC structure, i.e. by collapsing multiple dimensions into “Variable” strings.
   - Store these as time series data on the scenario being reported.
   - Write the final and some intermediate data to files.

   These reporting features handle quantities including final energy, but not emissions.

3. For certain quantities, notably emissions, the legacy reporting code must be used.
   (This is because, as of 2023-01-10, there is no :mod:`genno`-based reporting that provides adjustments necessary for certain representations in main MESSAGEix-GLOBIOM scenarios, e.g. blending in gas supply.)

   :func:`.buildings.report.configure_legacy_reporting` sets certain entries in :data:`.default_tables.TECHS`; for instance, the entry `"rc coal"` contains a list of technology IDs for residential and commercial (hence **rc** or **RC**) buildings technologies which consume the commodity `coal`.
   By default (in the base global model), this is a list with a single entry, `"coal_rc"`.
   :func:`.buildings.report.configure_legacy_reporting` replaces this with multiple entries for various technologies.

   The legacy reporting also performs aggregation over quantities, including final energy, emissions, and others, computed by itself or found as time series data on the scenario, e.g. that stored by (2).
   This functionality is not documented here in its entirety.

.. _buildings-usage:

Usage
=====

1. Clone the main MESSAGE_Buildings repo, linked above.

   Either use a directory named :file:`buildings` in the same directory containing :mod:`message_data`; or, note the path and set this in the :ref:`ixmp configuration file <ixmp:configuration>`::

     ixmp config set "message buildings dir" /path/to/cloned/message-buildings/repo

2. Invoke the model using, for example:

   .. code-block::

      mix-models \
        --url="ixmp://ixmp-dev/model name/base scenario" \
        buildings build-solve \
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
      buildings build-solve \
      --dest="ixmp://ixmp-dev/MESSAGEix-GLOBIOM 1.1-BM-R12-NGFS/NPi2020-con-prim-dir-ncr-building"

If using ``clim_scen="2C"``, the following scenario is used as the reference scenario for prices:

.. code-block::

    mix-models \
      --url="ixmp://ixmp-dev/MESSAGEix-GLOBIOM 1.1-BM-R12-NGFS/NPi2020-con-prim-dir-ncr" \
      buildings build-solve \
      --climate-scen="ENGAGE_SSP2_v4.1.8/EN_NPi2020_1000f" \
      --dest="ixmp://ixmp-dev/MESSAGEix-GLOBIOM 1.1-BM-R12-NGFS/NPi2020-con-prim-dir-ncr-building"

The following is commented in :file:`MESSAGE-BUILDINGS.py` with the comment “NOTE: this scenario has the updated GLOBIOM matrix.”

.. code-block::

    mix-models \
      --url="ixmp://ixmp-dev/MESSAGEix-GLOBIOM 1.1-M-R12-NGFS/baseline" \
      buildings build-solve \
      --dest="ixmp://ixmp-dev/MESSAGEix-GLOBIOM 1.1-BM-R12-NGFS/baseline"

The following correspond to :file:`MESSAGE-BUILDINGS-STURM.py`:

.. code-block::

    mix-models \
      --url="ixmp://ixmp-dev/MESSAGEix-GLOBIOM 1.1-M-R12-NGFS/baseline" \
      buildings build-solve \
      --dest="ixmp://ixmp-dev/MESSAGEix-GLOBIOM 1.1-BM-R12-EFC/baseline"

and:

.. code-block::

    mix-models \
      --url="ixmp://ixmp-dev/MESSAGEix-GLOBIOM 1.1-M-R12-NGFS/baseline" \
      buildings build-solve \
      --climate-scen="MESSAGEix-GLOBIOM 1.1-M-R12-NGFS/EN_NPi2020_1000" \
      --dest="ixmp://ixmp-dev/MESSAGEix-GLOBIOM 1.1-BM-R12-EFC/baseline"

The following corresponds to :file:`reporting_EFC.py`:

.. code-block::

    mix-models \
      --url="ixmp://ixmp-dev/MESSAGEix-GLOBIOM 1.1-BM-R12-EFC/baseline#24" \
      report -m model.buildings "buildings all"

Configuration
=============

The class :class:`.buildings.Config` defines all the options to which the code responds, as well as default values.
Values given in code or on the command line will override these.

.. autoclass:: message_ix_models.model.buildings.Config
   :members:

Code reference
==============

.. currentmodule:: message_ix_models.model

.. autosummary::
   :toctree: _autosummary
   :template: autosummary-module.rst
   :recursive:

   buildings
