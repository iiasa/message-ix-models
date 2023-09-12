What's new
**********

.. Next release
.. ============

v2023.9.12
==========

All changes
-----------

- New module :mod:`.project.ssp` (:pull:`122`) to generate SDMX codelists for the 2017/original SSPs and the 2024 update, and provide these as :class:`~.enum.Enum` to other code.
- New module :mod:`.tools.exo_data` to retrieve exogenous data for, among others, population and GDP (:pull:`122`).
  This module has a general API that can be implemented by provider classes.
- New function :func:`.model.emissions.get_emission_factors` and associated data file to provide data from `this table <https://docs.messageix.org/projects/global/en/latest/emissions/message/index.html#id15>`__ in the MESSAGEix-GLOBIOM documentation (:pull:`122`).
- New functions in :mod:`.util.sdmx` (:pull:`122`):

  - :func:`~.util.sdmx.read`, :func:`~.util.sdmx.write` to retrieve/store package data in SDMX-ML.
  - :func:`~.util.sdmx.make_enum` to make pure-Python :class:`~.enum.Enum` (or subclass) data structures based on SDMX code lists.

- :func:`.same_node` also fills "node_shares", "node_loc", and "node", as appropriate (:pull:`122`).

Deprecations
------------

- :func:`.eval_anno` is deprecated; code should instead use :meth:`sdmx.model.common.AnnotableArtefact.eval_annotation`, which provides the same functionality.

v2023.9.2
=========

- New module :mod:`message_ix_models.report` for reporting (:pull:`116`).
  Use of this module requires ixmp and message_ix version 3.6.0 or greater.
- Add documentation on :ref:`migrate-filter-repo` using :program:`git filter-repo` and helper scripts (:pull:`89`).

v2023.7.26
==========

- Add code and CLI commands to :doc:`fetch and load MESSAGEix-GLOBIOM snapshots <api/model-snapshot>` (:pull:`102`).
  Use of this module requires ixmp and message_ix version 3.5.0 or greater.
- Add :func:`.util.pooch.fetch`, a thin wrapper for using :doc:`Pooch <pooch:about>` (:pull:`102`).
- New module :mod:`message_ix_models.model.macro` with utilities for calibrating :mod:`message_ix.macro` (:pull:`104`).
- New method :meth:`.Workflow.guess_target` (:pull:`104`).
- Change in behaviour of :meth:`.Workflow.add_step`: the method now returns the name of the newly-added workflow step, rather than the :class:`.WorkflowStep` object added to carry out the step (:pull:`104`).
  The former is more frequently used in code that uses :class:`.Workflow`.
- Add the :ref:`R17` node code list (:pull:`109`).
- Add the :ref:`R20` node code list (:pull:`109`).

v2023.5.31
==========

- Adjust :mod:`sdmx` usage for version 2.10.0 (:pull:`101`).

v2023.5.13
==========

- Adjust :func:`.generate_product` for pandas 2.0.0 (:pull:`98`).

2023.4.2
========

- Add :doc:`/water/index` (:pull:`88`, :pull:`91`).
- New utility function :func:`.replace_par_data` (:pull:`90`).
- :func:`.disutility.get_spec` preserves all :class:`Annotations <sdmx.model.common.Annotation>` attached to the :class:`~sdmx.model.common.Code` object used as a template for usage technologies (:pull:`90`).
- Add ``CO2_Emission_Global_Total`` to the :ref:`“A” relation codelist <relation-yaml>` (:pull:`90`).
- :class:`.Adapter` and :class:`.MappingAdapter` can be imported from :mod:`message_ix_models.util` (:pull:`90`).
- Bump :mod:`sdmx` requirement from v2.2.0 to v2.8.0 (:pull:`90`).

2023.2.8
========

- Codelists for the ``relation`` :ref:`MESSAGEix set <message-ix:section_set_def>` (:pull:`85`):

  - Add :ref:`three relation codelists <relation-yaml>`.
  - The :doc:`“bare” reference energy system <api/model-bare>` now includes relations from the codelist indicated by :attr:`.model.Config.relations`; default "A".

- :ref:`commodity-yaml` (:pull:`85`):

  - Add "biomass", "non-comm", "rc_spec", and "rc_therm".
  - Add "report" annotations for some items.
    These include string fragments to be used in variable names when reporting data in the IAMC data structure.

- :func:`.generate_product` (and :func:`.generate_set_elements`) can handle a :doc:`regular expression <python:library/re>` to select a subset of codes for the Cartesian product (:pull:`85`).
- New utility method :meth:`.Context.write_debug_archive` writes a ZIP archive containing files listed by :attr:`.Config.debug_paths` (:pull:`85`).
- :class:`.WorkflowStep` can store and apply keyword options for the optional :meth:`~.message_ix.Scenario.clone` step at the start of the step execution (:pull:`85`).
- Bugfix: :meth:`.WorkflowStep.__call__` ensures that :attr:`.Config.scenario_info` on the :class:`.Context` instance passed to its callback matches the target scenario (:pull:`85`).

2022.11.7
=========

- Add the :ref:`ZMB` node code list (:pull:`83`).
- Add the utility :func:`.same_time`, to copy the set time in parameters (:pull:`83`).
- New :class:`~message_ix_models.Config` and :class:`.model.Config` :py:mod:`dataclasses` for clearer description/handling of recognized settings stored on :class:`.Context` (:pull:`82`).
  :class:`.ConfigHelper` for convenience/utility functionality in :mod:`.message_ix_models`-based code.
- New functions :func:`.generate_product`, :func:`.generate_set_elements`, :func:`.get_region_codes` in :mod:`.model.structure` (:pull:`82`).
- Revise and improve the :doc:`Workflow API </api/workflow>` (:pull:`82`).
- Adjust for pandas 1.5.0 (:pull:`81`).

2022.8.17
=========

- Add :func:`.nodes_ex_world` and use this in :func:`.disutility.data_conversion` instead of expected a "World" node ID to be the first element in :attr:`.ScenarioInfo.N` (:pull:`78`).
- Add example files and documentation for :doc:`pkg-data/iiasa-se` (:pull:`78`).
- Expand :file:`~` (i.e. ``$HOME``) in the ``"message local data"`` :ref:`configuration setting <local-data>` (:pull:`78`).

2022.7.25
=========

- Add :func:`.get_advance_data`, and related tools for data from the ADVANCE project, including the :ref:`node codelist <ADVANCE-nodes>` for the data (:pull:`76`).
- Add unit annotations to :ref:`commodity-yaml` (:pull:`76`).
- New utility methods :meth:`.ScenarioInfo.io_units` to derive units for ``input`` and ``output`` parameters from :meth:`.units_for` commodity stocks and technology activities (:pull:`76`).
- Transfer :func:`.add_tax_emission` from :mod:`message_data`, improve, and add tests (:pull:`76`).
- Unit annotations on commodity and technology codes are copied to child codes using :func:`.process_units_anno` (:pull:`76`).
- :func:`.make_matched_dfs` accepts :class:`pint.Quantity` to set both magnitude and units in generated data (:pull:`76`).
- :func:`.strip_par_data` also removes the set element for which data is being stripped (:pull:`76`).
- The common CLI options :program:`--verbose` and :program:`--dry-run` are stored on :class:`.Context` automatically (:pull:`76`).
- New utility method :meth:`.Context.set_scenario` (:pull:`76`).
- :data:`iam_units.registry` is the default unit registry even when :mod:`message_data` is not installed (:pull:`76`).
- Expand :func:`.broadcast` to allow :class:`~.pandas.DataFrame` with multiple dimensions as input (:pull:`74`).

2022.5.6
========

- Bump minimum required version of :mod:`.message_ix` to v3.4.0 from v3.2.0 (:pull:`71`).
- Add a documentation page on :doc:`distrib` (:pull:`59`).
- Add :func:`.testing.not_ci` for marking tests not to be run on continuous integration services; improve :func:`~.testing.session_context` (:pull:`62`).
- :func:`.apply_spec` also adds elements of the "node" set using :meth:`.ixmp.Platform.add_region` (:pull:`62`).
- Add new logo the documentation (:pull:`68`).
- Add :class:`.Workflow`; see :doc:`api/workflow` (:pull:`60`).

2022.3.30
=========

- Add :obj:`.adapt_R11_R12`, a function for adapting data from the :ref:`R11` to the :ref:`R12` node lists (:pull:`56`).
- Work around `iiasa/ixmp#425 <https://github.com/iiasa/ixmp/issues/425>`__ in :func:`.disutility.data_conversion` (:ref:`docs <disutility-units>`, :pull:`55`).

2022.3.3
========

- Change the node name in R12.yaml from R12_CPA to R12_RCPA (:pull:`49`).
- Register “message local data” ixmp configuration file setting and use to set the :attr:`.Context.local_path <.Config.local_data>` when provided.
  See :ref:`local-data` (:pull:`47`)

2022.1.26
=========

- New :class:`.Spec` class for easier handling of specifications of model (or model variant) structure (:pull:`39`)
- New utility function :func:`.util.local_data_path` (:pull:`39`).
- :func:`.repr` of :class:`.Context` no longer prints a (potentially very long) list of all keys and settings (:pull:`39`).
- :func:`.as_codes` accepts a :class:`.dict` with |Code| values (:pull:`39`).

Earlier releases
================

2021.11.24
----------

- Add :command:`--years` and :command:`--nodes` to :func:`.common_params` (:pull:`35`).
- New utility function :func:`.structure.codelists` (:pull:`35`).

2021.7.27
---------

- Improve caching using  mod:`genno` v1.8.0 (:pull:`29`).

2021.7.22
---------

- Migrate utilities :func:`cached`, :func:`.check_support`, :func:`.convert_units`, :func:`.maybe_query`, :func:`.series_of_pint_quantity` (:pull:`27`)
- Add :data:`.testing.NIE`.
- Add the ``--jvmargs`` option to :command:`pytest` (see :func:`.pytest_addoption`).
- Remove :meth:`.Context.get_config_file`, :meth:`.get_path`, :meth:`.load_config`, and :meth:`.units`, all deprecated since 2021-02-28.

2021.7.6
--------

- Add :func:`identify_nodes`, a function for identifying a :doc:`pkg-data/node` based on a |Scenario| (:pull:`24`).
- Add :obj:`.adapt_R11_R14`, a function for adapting data from the :ref:`R11` to the :ref:`R14` node lists (:pull:`24`).
- Add :func:`.export_test_data` and :command:`mix-models export-test-data` command (:pull:`16`).
  See :ref:`export-test-data`.
- Allow use of pytest's persistent cache across test sessions (:pull:`23`).
  See :doc:`repro`.
- Add the :ref:`R12` node code list (:pull:`14`).

2021.4.7
--------

- Add :mod:`.model.disutility`, code for setting up structure and data for generalized consumer disutility (:pull:`13`)

2021.3.24
---------

- Add :doc:`pkg-data/year`, YAML data files, :meth:`.ScenarioInfo.year_from_codes` and associated tests (:issue:`11`, :pull:`12`)

2021.3.22
---------

- Migrate :mod:`.model.bare`, :mod:`.model.build`, :mod:`.model.cli`, and associated documentation (:pull:`9`)
- Migrate utilities: :class:`.ScenarioInfo`, :func:`.add_par_data`, :func:`.eval_anno`, :func:`.iter_parameters`, and :func:`.strip_par_data`.

2021.3.3
--------

- Migrate :mod:`.util.click`, :mod:`.util.logging`; expand documentation (:pull:`8`:).
- :meth:`.Context.clone_to_dest` method replaces :func:`clone_to_dest` function.
- Build PDF documentation on ReadTheDocs.
- Allow CLI commands from both :mod:`message_ix_models` and :mod:`message_data` via :program:`mix-models`.
- Migrate :program:`mix-models techs` CLI command.

2021.2.28
---------

- Migrate :class:`.Context` class and :mod:`.testing` module from :mod:`message_data` (:pull:`5`:).
- Add :func:`.load_private_data`, :func:`.package_data_path`, :func:`.private_data_path`.
- Document: :doc:`data` and :doc:`cli`.
- Update :doc:`node codelists <pkg-data/node>` to ensure they contain both current and former ISO 3166 codes for countries that have changed status (:pull:`6`:).
  For instance, ANT dissolved into BES, CUW, and SXM in 2010; all four are included in R11_LAM so this list can be used to handle data from either before or after 2010.

2021.2.26
---------

- Add :func:`.get_codes` and related code lists (:pull:`2`:).
- Add :class:`.MessageDataFinder` and document :doc:`migrate` (:pull:`3`:).

2021.2.23
---------

Initial release.
