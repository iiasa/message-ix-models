MESSAGEix-Materials (:mod:`.model.material`)
********************************************

Description
===========

This module adds material stock and flow accounting in MESSAGEix-GLOBIOM.
The implementation models currently four key energy- & emission-intensive industries explicitly:

- Iron & Steel
- Aluminum
- Cement
- Chemicals

The original generalized industry representation in MESSAGEix-GLOBIOM is re-purposed to represent "other industries" that are not modelled with the explicit material module, but still require a representation in the energy system.
The methodology and model structure of MESSAGEix-Materials is described in Ünlü et al. (2024) :cite:`unlu_2024_materials`.

.. contents::

Data preparation
----------------

The model build uses a set of data preparation scripts to prepare and read the data into the model.
Each industry sector has a dedicated data preparation script that generates representative MESSAGEix parameter data.
The basis of the parameter data is read from a set of input files, which are described in :doc:`data`.

.. toctree::
   :hidden:
   :maxdepth: 2

   data

Usage
=====
Use the :doc:`CLI </cli>` command ``mix-data material-ix`` to invoke the commands defined in :mod:`.material.cli`.

.. code-block:: shell

  $ mix-models material-ix --help
  Usage: mix-models material-ix [OPTIONS] {LED|SSP1|SSP2|SSP3|SSP4|SSP5} COMMAND
                                [ARGS]...

    MESSAGEix-Materials variant.

  Options:
    --help  Show this message and exit.

  Commands:
    build   Build a scenario.
    report  Run materials specific reporting, then legacy reporting.
    solve   Solve a scenario.

Use ``mix-models materials-ix {SSP} build`` to add the material implementation on top of existing standard global (R12) scenarios, also giving the base scenario and indicating the relevant data location, e.g.:

.. code-block:: shell

    mix-models \
        --url="ixmp://ixmp_dev/MESSAGEix-GLOBIOM 1.1-R12/baseline_DEFAULT#21" \
        material-ix SSP2 build --tag test --nodes R12

The output scenario name will be baseline_DEFAULT_test. An additional tag ``--tag`` can be used to add an additional suffix to the new scenario name.
The mode option ``--mode`` has two different inputs 'by_url' (by default) or 'by_copy'.
The first one uses the provided ``--url`` to add the materials implementation on top of the scenario from the url.
This is the default option. The latter is used to create a 2 degree mitigation scenario with materials by copying carbon prices to the scenario that is specified by ``--scenario_name``:

.. code-block:: shell

    mix-models --url="ixmp://ixmp_dev/MESSAGEix-Materials/scenario_name" material-ix \
     build --tag test --mode by_copy

This command line only builds the scenario but does not solve it.
To solve the scenario, use ``mix-models materials-ix solve``, giving the scenario name, e.g.:


.. code-block:: shell

    mix-models --url="ixmp://ixmp_dev/MESSAGEix-Materials/scenario_name" material-ix \
     SSP2 solve --add_calibration False --add_macro False

.. note::
    To include endogenous modelling of material stocks from power sector :mod:`message_ix` version needs to be:

    - greater than 3.11
    - or the latest ``main`` build
      (See: `install message_ix from source <https://docs.messageix.org/en/latest/install-adv.html#install-from-a-git-clone-of-the-source-code>`_).

The solve command has the ``--add_calibration`` option to add MACRO calibration to a baseline scenario with a valid calibration file specified with ``--macro-file``.
The ``--add_macro`` option determines whether the scenario should be solved with MESSAGE or MESSAGE-MACRO.
MESSAGEix-Materials provides one calibration file that is only compatible with scenarios with first model year 2025 and the common model structure of a MESSAGEix-GLOBIOM scenario.
To first calibrate the scenario and then solve that scenario with MACRO both options should be set to :any`True`.

It is also possible to shift the first model year and solve a clone with shifted years with ``--shift_model_year``.
If ``--shift_model_year`` is set together with the macro options the model year will be shifted before the MACRO calibration.

All three options are :any:`False` by default.

Reporting
---------

The reporting generates specific variables related to materials, mainly Production and Final Energy.
The resulting reporting file is generated under :file:`message_ix_models/data/material/reporting_output` with the name “New_Reporting_MESSAGEix-Materials_scenario_name.xlsx”.
More detailed variables related to the whole energy system and emissions are not included in this reporting.

Reporting is executed by the following command:

.. code-block:: shell

    mix-models --url="ixmp://ixmp_dev/MESSAGEix-Materials/scenario_name" \
        --local-data "./data" material-ix SSP2 report

To remove any existing timeseries in the scenario the following command can be used:

.. code-block:: shell

    mix-models --url="ixmp://ixmp_dev/MESSAGEix-Materials/scenario_name" material-ix \
        SSP2 report --remove_ts True

Code reference
==============

.. autosummary::
   :toctree: _autosummary
   :template: autosummary-module.rst
   :recursive:

   message_ix_models.model.material

Release notes
=============

This is the list of changes to MESSAGEix-Materials between each release.

.. toctree::
   :maxdepth: 2

   v1.1.0
   v1.2.0
   v1.2.1

.. note::
   See also :pull:`130`/the archived branch `materials-migrate <https://github.com/iiasa/message-ix-models/tree/migrate-materials>`_ for a distinct version of :mod:`.material`.
   That earlier PR was superseded by :pull:`188`, but contains the 1.0.0 version of MESSAGEix-Materials, which was used for the first submission of :cite:`unlu_2024_materials`.
   The model structure is almost identical to the default model that was added by :pull:`188`.
   Compared to :pull:`188` this version differs particularly in the following areas:

   - Older base year calibration of "other industries" using outdated IEA EWEB data.
   - Material demands computed in R through ``rpy2``, instead of Python implementation.
   - Less accurate regional allocation/aggregation of base year demands for cement and steel.
   - No use of :mod:`.tools.costs`.