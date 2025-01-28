MESSAGEix-Transport (:mod:`.model.transport`)
*********************************************

.. warning::

   1. MESSAGEix-Transport is **under development**.
      For details, see:

      - `Issues and PRs labeled 'transport' <https://github.com/iiasa/message-ix-models/issues?q=label%3Atransport>`_ on the :mod:`.message_ix_models` GitHub repository.
      - The `project board <https://github.com/orgs/iiasa/projects/29>`_ (IIASA only).

   2. The code and data documented on these pages were recently :doc:`migrated </migrate>` from the private   :mod:`.message_data` repository to the public :mod:`message_ix_models` repository.
      The text may still contain references to the old location.

:mod:`message_ix_models.model.transport` adds a technology-rich representation of transport to models in the MESSAGEix-GLOBIOM family.
The resulting “model variant” is variously referred to as:

- **MESSAGEix-Transport**,
- “MESSAGEix-GLOBIOM ‘T’” or, with other variants like :mod:`.buildings` and :mod:`~message_ix_models.model.material`, “MESSAGEix-GLOBIOM BMT”, or
- “MESSAGEix-XX-Transport” where built on a single-country base model (again, in the MESSAGEix-GLOBIOM family) named like “MESSAGEix-XX”.

MESSAGEix-Transport extends the formulation described by McCollum et al. (2017) :cite:`mccollum-2017` for the older, MESSAGE V framework that predated MESSAGEix.
Some inherited information about the older model is collected at :doc:`old`.

This documentation of MESSAGEix-Transport, its inputs, configuration, implementation, and output, are organized according to this diagram:

.. figure:: https://raw.githubusercontent.com/khaeru/doc/main/image/data-stages.svg

- :doc:`input` (separate page)—line (1) in the diagram.
- :ref:`transport-implementation` (below)—between lines (1) and (3) in the diagram.
- :doc:`output` (separate page)—between lines (3) and (4) in the diagram.

.. toctree::
   :hidden:
   :maxdepth: 2

   input
   output

Other topics covered on this page:

.. contents::
   :local:

.. _transport-implementation:

Implementation
==============

Summary
-------

The code:

- Operates on a base model with a particular structure, the standard MESSAGEix-GLOBIOM representation of transport.
  See :ref:`transport-base-structure`.
- **Builds** MESSAGEix-Transport on the base model:

  - Use the :func:`.apply_spec` pattern, with a :class:`.Spec` that identifies:

    - Required set elements in the base model, for instance ``commodity`` elements representing the fuels used by MESSAGEix-Transport technologies.
    - Set elements to be removed, for instance the ``technology`` elements for base model/aggregate transport technologies.
      (Removing these elements also removes all parameter data indexed by these elements.)
    - Set elements to be added.
      These are generated dynamically based on configuration setting from files and code; see :ref:`transport-config`.

  - Use a :class:`genno.Computer` (from :func:`.build.get_computer`) to:

    - Read data from :ref:`transport-data-files` and other sources,
    - Prepares the parametrization of MESSAGEix-Transport through an extensive set of computations, and
    - Add these data to the target :class:`.Scenario`.

- **Solves** the :class:`.Scenario`.
- Provides :mod:`message_ix_models.report` extensions to **report or post-process** the model solution data and prepare detailed transport outputs in various formats (see :doc:`output`).

Details
-------

.. toctree::
   :hidden:
   :maxdepth: 2

   disutility

On other page(s): :doc:`disutility`.

- For light-duty vehicle technologies annotated with ``historical-only: True``, parameter data for ``bound_new_capacity_up`` are created with a value of 0.0.
  These prevent new capacity of these technologies from being created during the model horizon, but allow pre-horizon installed capacity (represented by ``historical_new_capacity``) to continue to be operated within its technical lifetime.
  (:pull:`441`)

.. _transport-usage:

Usage
=====

Automated workflow
------------------

:func:`.transport.workflow.generate` returns a :class:`.Workflow` instance.
This can be invoked or modified by other code, or through the command-line::

  $ mix-models transport run --help
  Usage: mix-models transport run [OPTIONS] TARGET

    Run the MESSAGEix-Transport workflow up to step TARGET.

    Unless --go is given, the workflow is only displayed. --from is interpreted
    as a regular expression.

  Options:
    --future TEXT                   Transport futures scenario.
    --fast                          Skip removing data for removed set elements.
    --model-extra TEXT              Model name suffix.
    --scenario-extra TEXT           Scenario name suffix.
    --key TEXT                      Key to report.
    --dest TEXT                     Destination URL for created scenario(s).
    --dry-run                       Only show what would be done.
    --nodes [ADVANCE|B210-R11|ISR|R11|R12|R14|R17|R20|R32|RCP|ZMB]
                                    Code list to use for 'node' dimension.
    --quiet                         Show less or no output.
    --go                            Actually run the workflow.
    --from TEXT                     Truncate workflow at matching step(s).
    --help                          Show this message and exit.

This workflow can be used in the following ways:

Run the entire workflow
   This is the method used by the :file:`transport.yaml` GitHub Actions workflow (see :ref:`transport-ci`) on a daily schedule, and thus always known to work for the combinations of arguments used in that workflow.

   For the same reason, *it should not be necessary to run this manually*; simply trigger the workflow.

   .. code-block:: shell

      mix-models \
        --platform=ixmp-dev \
        transport run \
        --base=auto --nodes=R12 --model-extra="ci nightly" \
        --from="" \
        "SSP2 policy reported" \
        --go

   The options result in the following behaviour:

   - :program:`--platform=ixmp-dev`: store MESSAGEix-Transport scenarios on the :mod:`ixmp` platform named "ixmp-dev".
   - :program:`--base=auto`: identify the base scenario URLs using :func:`.base_scenario_url` / the file :ref:`CL_TRANSPORT_SCENARIO`, according to other config settings.
   - :program:`--model-extra="ci nightly"`: append the string " ci nightly" to the model name of any created Scenario.
     This avoids accidentally producing new versions of ‘production’ (model name, scenario name) combinations.
   - :program:`--from=""`: start from the very first step in the workflow—load the identified base scenario—and perform all subsequent workflow steps, up to and including…
   - :program:`"SSP2 policy reported"`: the target step to reach.
   - :program:`--go`: Actually execute the workflow.
     Omit this to show a preview/dry-run.

Debug the build for a single scenario
   This emulates the build step, but does not use any ‘real’ base scenario; only a :mod:`.bare` RES scenario that contains structure but no data.

   .. code-block:: shell

      mix-models \
        transport run \
        --nodes=R12 \
        "SSP2 debug build" \
        --go

   This produces output files (:file:`.csv`, :file:`.pdf`) for debugging the build.
   The output for this particular command is in the directory :file:`{local-data-path}/transport/debug-ICONICS:SSP(2024).2-R12-B/`.
   With a different target or :program:`--nodes` option, the directory name will differ accordingly.

Debug the build for all scenarios
   This performs the above debug build step for all SSPs, and then runs :func:`.debug_multi` to generate plots that compare the contents of the debug :file:`.csv` files for all 5 SSPs.
   The plots are output to the directory :file:`{local-data-path}/transport/`.

   .. code-block:: shell

      mix-models \
        transport run \
        --nodes=R12 \
        "debug build" \
        --go

Run only reporting
   .. code-block:: shell

      export URL="ixmp://ixmp-dev/MESSAGEix-GLOBIOM 1.1-T-R12 ci nightly/SSP_2024.2 baseline#154"
      mix-models \
        --url="$URL" \
        transport run \
        --nodes=R12 --model-extra="ci nightly" \
        --from="SSP2 solved" \
        "SSP2 reported" \
        --key="in:nl-t-ya-c:transport+units" \
        --go

   This:

   - First sets a shell environment variable (``$URL``) that points to an (existing model name, scenario name, version)—for instance, one produced by the GitHub Actions workflow.
   - :program:`--url="$URL"`: runs the workflow with this as a base URL.
   - :program:`--from="SSP2 solved"`: starts *after* the step SSP2 solved.
     In other words, assume that all steps up to and including this step have already run, and have produced a scenario with the given ``$URL``.
   - :program:`"SSP2 reported"`: run the reporting step.
   - :program:`--key="in:nl-t-ya-c:transport+units"`: compute only this reporting key; display; and exit.
     This can be changed to any other key, or omitted entirely for the default action, which is to produce *all* output files (IAMC-structured output in :file:`.csv` and :file:`.xlsx` formats, plots in :file:`.pdf` format, and files for base MESSAGEix-GLOBIOM calibration in :file:`.csv` format) and also store the IAMC-structured output as time series data with the scenario.

Manual
------

This subsection contains an older, manual, step-by-step workflow.

**Preliminaries.**
Check the list of :doc:`pre-requisite knowledge <message-ix:prereqs>` for working with :mod:`.message_ix_models`.

.. note:: One pre-requisite is basic familiarity with using a shell/command line.

   Specifically: ``export BASE="…"``, seen below, is a built-in command of the Bash shell (Linux or macOS) to set an environment variable.
   ``$BASE`` refers to this variable.
   In the Windows Command Prompt, use ``set BASE="…"`` to set and ``%BASE%`` to reference.
   Variables with values containing spaces must be quoted when referencing, as in the example commands below.

   To avoid using environment variables altogether, insert the URL directly in the command, for instance::

       $ mix-models --url="ixmp://mt/Bare RES/baseline" res create-bare

**Choose a platform.**
This example uses a platform named ``mt``.
If not already configured on your system, create the configuration for the platform to be used; something like::

    $ ixmp platform add mt jdbc hsqldb /path/to/db

.. note:: See the :ref:`ixmp documentation <ixmp:configuration>` for how to use the ``ixmp`` command to add or edit configuration for specific platforms and databases.

**Identify the base scenario.**
One option is to create the ‘bare’ RES; the following is equivalent to calling :func:`.bare.create_res`::

    $ export BASE="ixmp://mt/Bare RES/baseline"
    $ mix-models --url="$BASE" res create-bare

For other possibilities, see :ref:`transport-base-scenarios`.

**Build the model.**
The following is equivalent to cloning ``BASE`` to ``URL``, and then calling :func:`.transport.build.main` on the scenario stored at ``URL``::

    $ export URL=ixmp://mt/MESSAGEix-Transport/baseline
    $ mix-models --url="$BASE" transport build --dest="$URL"

**Solve the model.**
The following is equivalent to calling :meth:`message_ix.Scenario.solve`::

    $ message-ix --url="$URL" solve

**Report the results.**
The ``-m model.transport`` option indicates that additional reporting calculations from :mod:`.model.transport.report` should be added to the base reporting configuration for MESSAGEix-GLOBIOM::

    $ mix-models --url="$URL" report -m model.transport "transport plots"

Utilities
---------

There are several other sub-commands of :program:`mix-models transport` available.
Use ``--help`` overall or for a particular command to learn more.

:command:`gen-activity`
  Generate projected activity data without building a full scenario::

    $ mix-models transport gen-demand --ssp-update=2

  This command produces:

  - Files in :file:`{MESSAGE_LOCAL_DATA}/transport/gen-activity/{scenario}` (see :ref:`local data <local-data>`), where `scenario` reflects the command options:

    - :file:`pdt.csv`, :file:`pdt-cap.csv`: projected activity data.
    - :file:`demand-exo.pdf`, :file:`demand-exo-cap.pdf`: plots of the same.

  - :file:`{MESSAGE_LOCAL_DATA}/transport/gen-activity/compare-[pdt,pdt-cap].pdf`: plots that contrast the data output by the current command invocation *and* any others in other sub-directories of :file:`…/gen-demand`, that is, from previous invocations.

  Thus, to prepare :file:`compare-pdt.pdf` containing projections for multiple scenarios, invoke the command repeatedly, for instance::

    $ mix-models transport gen-demand --ssp=2
    $ mix-models transport gen-demand --ssp-update=1
    $ mix-models transport gen-demand --ssp-update=2
    $ mix-models transport gen-demand --ssp-update=3
    $ mix-models transport gen-demand --ssp-update=4
    $ mix-models transport gen-demand --ssp-update=5

:command:`refresh`
  .. deprecated:: 2023-11-21
     Use :program:`ixmp platform copy` from the :mod:`ixmp` :doc:`ixmp:cli` instead.

Scenarios
=========

.. _transport-base-scenarios:

Base scenarios
--------------

The following existing scenarios are targets for the MESSAGEix-Transport code to operate on:

``ixmp://ixmp-dev/MESSAGEix-GLOBIOM 1.1-R12/baseline_DEFAULT#21``
  nodes=R12, years=B.

  Current development target as of 2023-12-13, and used in the :file:`transport.yaml` CI workflow.

``ixmp://ene-ixmp/CD_Links_SSP2_v2/baseline``
   regions=R11, years=A.

``ixmp://ixmp-dev/ENGAGE_SSP2_v4.1.7/baseline#3``
   regions=R11, years=B.

``ixmp://ixmp-dev/ENGAGE_SSP2_v4.1.7_ar5_gwp100/EN_NPi2020_1000_emif_new#5``
   regions=R11, years=B.
   This scenario has a “hybrid” or “dual” implementation of emissions accounting: it includes *both*:

   - the ‘old’ structure, in which emissions are accounted using :mod:`message_ix` ``relation_activity`` and related parameter, but ``emission_factor`` is unused/empty, **and**
   - a ‘new’ structure in which the ``emission_factor`` parameter is actually used.

``ixmp://ixmp-dev/MESSAGEix-GLOBIOM_R12_CHN/baseline#17``
   years=B.
   Based on ENGAGE, without MACRO calibration.
   This scenario has a non-standard ``node`` code list: there are 12 nodes, as in the ``R12`` list, but their IDs are e.g. ``R11_CHN``, ``R11_RCPA``, etc.

``ixmp://ixmp-dev/MESSAGEix-GLOBIOM_R12_CHN/baseline_macro#3``
   regions=R12, years=B. Includes MACRO calibration

``ixmp://ixmp-dev/MESSAGEix-Materials/NoPolicy_GLOBIOM_R12_s#1``
  regions=R12, years=B. Includes :doc:`/material/index` detail.

``ixmp://ixmp-dev/MESSAGEix-Materials/NoPolicy_2305#?``
  regions=R12, years=B. Includes :doc:`/material/index` detail.

.. _transport-base-structure:

Structure of base scenarios
---------------------------

The MESSAGEix-GLOBIOM RES (e.g. :mod:`.model.bare` or :mod:`message_data.model.create`) contains a representation of transport with lower resolution.
Some documentation is in the base-model documentation (:py:`message_doc`; see also `iiasa/message-ix-models#107 <https://github.com/iiasa/message-ix-models/pull/107>`_).
This section gives additional details missing there, which are relevant to MESSAGEix-Transport.

- Demand (``commodity=transport``, ``level=useful``) is denoted in **energy units**, i.e. GWa.
- Technologies producing this output; all at ``m=M1``, except where noted.
  This is the same set as in :doc:`MESSAGE V <old>`, i.e. in MESSAGE V, the aggregate transport representation is inactive but still present.

  - ``coal_trp``
  - ``foil_trp``
  - ``loil_trp``
  - ``gas_trp``
  - ``elec_trp``
  - ``meth_ic_trp``
  - ``eth_ic_trp``
  - ``meth_fc_trp``
  - ``eth_fc_trp``
  - ``h2_fc_trp``
  - ``back_trp`` — at modes M1, M2, M3
  - ``Trans_1``
  - ``Trans_2``
  - ``Trans_3``
  - ``Trans_4``
  - ``Trans_5``
- ``historical_activity`` and ``ref_activity`` indicates which of these technologies were active in the model base year.
  - Some, e.g. ``back_trp``, are not (zero values)
  - Disaggregated technologies must match these totals.

SSP scenarios (2024 update)
---------------------------

The code responds to :attr:`.transport.Config.ssp` (equivalently, :py:`context["transport"].ssp`) by computing and applying a variety of **factors**.
These are defined in :data:`.factor.COMMON`.
Each :class:`.Factor` is built-up from 1 or more **layers** that each represent a different assumption or choice.
When MESSAGEix-Transport is built, these assumptions are **quantified** and combined into a single, concrete :class:`.Quantity` object with at least the dimensions :math:`(n, y)`, sometimes :math:`(n, y, t)`.
These specific values are applied in (usually) multiplicative or other ways to other values produced by the model build process.

Here we explain one example:

.. code-block:: python

    LMH = Map(
        "setting", L=Constant(0.8, "n y"), M=Constant(1.0, "n y"), H=Constant(1.2, "n y")
    )
    OMIT_2020 = Omit(y=[2020])

    ...

        "ldv load factor": Factor(
            [
                LMH,
                OMIT_2020,
                ScenarioSetting.of_enum(SSP_2024, "1=H 2=M 3=M 4=L 5=L", default="M"),
            ]
        ),

This example has three layers.
The first two are reused from variables, because they also appear in other factors.

- The first layer sets constant values (the same for every label on the dimensions :math:`(n, y)`) for three different labels on a ‘setting’ dimension.
  These labels are merely :class:`str`: their meaning or interpretation **must** be clearly explained in code comments or by linked documentation.
  Otherwise they may be ambiguous ("'H'igh energy intensity" means the same thing as "'L'ow efficiency": what precisely is measured by the quantity to which the factor should apply?)
  The ‘M’ setting has a value of 1.0.
  Because this particular factor is used multiplicatively, in effect choosing the ‘M’ setting results in :py:`value * 1.0 = value`: that is, no change or a **no-op**.
- The second layer indicates to omit or mask values for :math:`y \in \{2020\}`.
  In effect, this results in values of 1.0 for this period, with the same no-op effect described above.
- The last layer is a “scenario setting”.
  In effect, this transforms a ‘scenario’ identifier from an enumeration (something like :py:`SSP_2024["2"]`) into one of the ‘setting’ labels from the first layer.
  This allows the same setting to be specified for multiple scenarios: in this example, SSP2 and SSP3 have the same setting.
  If the constant values in the first layer are changed, the values applied for SSP2 and SSP3 will both change.

  The string :py:`"1=H …"` **must** contain every member of (in this case) :data:`~message_ix_models.project.ssp.SSP_2024`; every setting label that appears **must** be provided by the previous layers of the factor.
  (To be clear: this does *not* mean that all defined settings must be used; it is valid to use, for instance, :py:`"1=M 2=M 3=M 4=M 5=M"`.)

To change the assumptions that are modeled via any particular factor:

- Add or remove layers.
- Change the :class:`.Constant` values that appear.
- Change the :class:`.ScenarioSetting` mapping.
- Adjust where and how the factor is applied in the transport build process.
  This option requires more technical skill.

.. _transport-ci:

Testing and validation
======================

MESSAGEix-Transport includes a GitHub Actions workflow defined in the file :file:`.github/workflows.transport.yaml`.
A list of past runs of this workflow appears `here <https://github.com/iiasa/message_data/actions/workflows/transport.yaml>`_.
This workflow:

- Runs on a schedule trigger, daily.
- Runs on a GitHub Actions **self-hosted runner**.
  This is hosted on a server within the IIASA network, so is able to access the ``ixmp-dev`` :mod:`ixmp` Oracle database.
- Uses, as its starting point, the first scenario listed under :ref:`transport-base-scenarios`, above.
- Runs multiple jobs; currently, one job for each :data:`~message_ix_models.project.ssp.SSP_2024`.
  Each job takes about 30 minutes, and the jobs run in sequence, so the entire workflow takes 2.5 hours to run.
- Produces an **artifact**: aside from the logs, certain files generated during the run are combined in a ZIP archive and stored by GitHub.
  This artifact contains, *inter alia*:

  - One directory per job.
  - In each directory, files :file:`transport.csv` and :file:`transport.xlsx` containing :doc:`MESSAGEix-Transport reporting output <output>`.
  - In each directory, files :file:`demand.csv` and :file:`bound_activity_{lo,up}.csv` containing data suitable for parametrizing the base MESSAGEix-GLOBIOM model.
- May be triggered manually.
  Use the “Run workflow” button and choose a branch; the code and data on this branch will be the ones used to build, solve, and report MESSAGEix-Transport.
- May be altered to aid with development:

  - Run on every commit on a pull request branch:

    .. code-block:: yaml

       # Uncomment these lines for debugging, but leave them commented on 'main'/'dev'
       pull_request:
       branches: [ main, dev ]

  - Run only some steps; not the full build–solve–report sequence.
    For instance:

    .. code-block:: yaml

       env:
         # Starting point of the workflow.
         # Use this value to build from a certain scenario:
         # base: --url="ixmp://ixmp-dev/MESSAGEix-GLOBIOM 1.1-R12/baseline_DEFAULT#21"
         # Use this value to allow the workflow to determine model & scenario names
         # and versions:
         base: --platform="ixmp-dev"

         # Set this to a particular step to truncate the workflow
         from-step: ".* solved"

  Per the comments, **do not** merge such changes to ``dev`` or ``main``.
  Instead, make them with a commit message like "TEMPORARY Adjust 'transport' CI workflow for PR"; then later :program:`git rebase -i` and ``drop`` the temporary commit.

Code reference
==============

The entire module and its contents are documented recursively:

.. currentmodule:: message_ix_models.model

.. autosummary::
   :toctree: _autosummary
   :template: autosummary-module.rst
   :recursive:

   message_ix_models.model.transport

Other documents
===============

.. toctree::
   :maxdepth: 2

   old
