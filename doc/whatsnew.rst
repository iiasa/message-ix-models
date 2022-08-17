What's new
**********

.. Next release
.. ============

2022.8.17
=========

- Add :func:`.nodes_ex_world` and use this in :func:`.disutility.data_conversion` instead of expected a "World" node ID to be the first element in :attr:`.ScenarioInfo.N` (:pull:`78`).
- Add example files and documentation for :doc:`pkg-data/iiasa-se` (:pull:`78`).
- Expand :file:`~` (i.e. ``$HOME``) in the ``"message local data"`` :ref:`configuration setting <local-data>` (:pull:`78`).

2022.7.25
=========

- Add :func:`get_advance_data`, and related tools for data from the ADVANCE project, including the :ref:`node codelist <ADVANCE-nodes>` for the data (:pull:`76`).
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

- Bump minimum required version of :mod:`message_ix` to v3.4.0 from v3.2.0 (:pull:`71`).
- Add a documentation page on :doc:`distrib` (:pull:`59`).
- Add :func:`.testing.not_ci` for marking tests not to be run on continuous integration services; improve :func:`~.testing.session_context` (:pull:`62`).
- :func:`.apply_spec` also adds elements of the "node" set using :meth:`.ixmp.Platform.add_region` (:pull:`62`).
- Add new logo the documentation (:pull:`68`).
- Add :class:`.Workflow`; see :doc:`api/workflow` (:pull:`60`).

2022.3.30
=========

- Add :func:`adapt_R11_R12`, a function for adapting data from the :ref:`R11` to the :ref:`R12` node lists (:pull:`56`).
- Work around `iiasa/ixmp#425 <https://github.com/iiasa/ixmp/issues/425>`__ in :func:`.disutility.data_conversion` (:ref:`docs <disutility-units>`, :pull:`55`).

2022.3.3
========

- Change the node name in R12.yaml from R12_CPA to R12_RCPA (:pull:`49`).
- Register “message local data” ixmp configuration file setting and use to set the :attr:`.Context.local_path` when provided.
  See :ref:`local-data` (:pull:`47`)

2022.1.26
=========

- New :class:`.Spec` class for easier handling of specifications of model (or model variant) structure (:pull:`39`)
- New utility function :func:`.util.local_data_path` (:pull:`39`).
- :func:`.repr` of :class:`.Context` no longer prints a (potentially very long) list of all keys and settings (:pull:`39`).
- :func:`.as_codes` accepts a :class:`.dict` with :class:`.Code` values (:pull:`39`).

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

- Add :func:`identify_nodes`, a function for identifying a :doc:`pkg-data/node` based on a :class:`.Scenario` (:pull:`24`).
- Add :func:`adapt_R11_R14`, a function for adapting data from the :ref:`R11` to the :ref:`R14` node lists (:pull:`24`).
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
