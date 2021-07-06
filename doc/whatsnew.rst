What's new
**********

.. Next release
.. ============

2021.7.6
========

- Add :func:`identify_nodes`, a function for identifying a :doc:`pkg-data/node` based on a :class:`.Scenario` (:pull:`24`).
- Add :func:`adapt_R11_R14`, a function for adapting data from the :ref:`R11` to the :ref:`R14` (:pull:`24`).
- Add :func:`.export_test_data` and :command:`mix-models export-test-data` command (:pull:`16`).
  See :ref:`export-test-data`.
- Allow use of pytest's persistent cache across test sessions (:pull:`23`).
  See :doc:`repro`.
- Add the :ref:`R12` node code list (:pull:`14`).

2021.4.7
========

- Add :mod:`.model.disutility`, code for setting up structure and data for generalized consumer disutility (:pull:`13`)

2021.3.24
=========

- Add :doc:`pkg-data/year`, YAML data files, :meth:`.ScenarioInfo.year_from_codes` and associated tests (:issue:`11`, :pull:`12`)

2021.3.22
=========

- Migrate :mod:`.model.bare`, :mod:`.model.build`, :mod:`.model.cli`, and associated documentation (:pull:`9`)
- Migrate utilities: :class:`.ScenarioInfo`, :func:`.add_par_data`, :func:`.eval_anno`, :func:`.iter_parameters`, and :func:`.strip_par_data`.

2021.3.3
========

- Migrate :mod:`.util.click`, :mod:`.util.logging`; expand documentation (:pull:`8`:).
- :meth:`.Context.clone_to_dest` method replaces :func:`clone_to_dest` function.
- Build PDF documentation on ReadTheDocs.
- Allow CLI commands from both :mod:`message_ix_models` and :mod:`message_data` via :program:`mix-models`.
- Migrate :program:`mix-models techs` CLI command.

2021.2.28
=========

- Migrate :class:`.Context` class and :mod:`.testing` module from :mod:`message_data` (:pull:`5`:).
- Add :func:`.load_private_data`, :func:`.package_data_path`, :func:`.private_data_path`.
- Document: :doc:`data` and :doc:`cli`.
- Update :doc:`node codelists <pkg-data/node>` to ensure they contain both current and former ISO 3166 codes for countries that have changed status (:pull:`6`:).
  For instance, ANT dissolved into BES, CUW, and SXM in 2010; all four are included in R11_LAM so this list can be used to handle data from either before or after 2010.

2021.2.26
=========

- Add :func:`.get_codes` and related code lists (:pull:`2`:).
- Add :class:`.MessageDataFinder` and document :doc:`migrate` (:pull:`3`:).

2021.2.23
=========

Initial release.
