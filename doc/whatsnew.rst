What's new
**********

.. Next release
.. ============

v2025.7.23
==========

- Update :mod:`.add_AFOLU_CO2_accounting`
  and :mod:`.add_alternative_TCE_accounting`
  for |ssp-scenariomip| (:pull:`354`).
- New module :mod:`.tools.policy` for representing ‘policy’ information (:pull:`349`).
- New workflow step :func:`.model.workflow.step_0` (:pull:`354`).

By topic:

.. contents::
   :local:
   :backlinks: none

Base model and variants
-----------------------

- Add ``TCE`` to :ref:`emission-yaml` (:pull:`349`).
- Add 9 ``*_bunker`` technologies to :ref:`technology-yaml` (:pull:`349`).
- Improve and extend :doc:`/material/index`.
  See :doc:`/material/v1.2.0` for details.

  - Improve :mod:`.material.data_cement` (:pull:`361`).

- Improve :doc:`/transport/index` (:pull:`349`).

  - Use version "3.2.beta" of SSP socioeconomic data.
  - Force scale-1 to 1.0 for (technology=F ROAD, commodity=gas).
  - :attr:`Config.policy <.transport.config.Config.policy>` is a :class:`set` of :class:`.Policy` instances,
    instead of :class:`bool`.
  - New classes :class:`~.transport.policy.ExogenousEmissionPrice`, :class:`~.transport.policy.TaxEmission`.
  - Add version 1.1.0 of the ``IIASA_ECE:CL_TRANSPORT_SCENARIO`` code list.
  - Update input data for :data:`.act_non_ldv`,
    :data:`.activity_freight`,
    :data:`.elasticity_f`,
    :data:`.load_factor_ldv`,
    passenger mode share,
    :data:`.pdt_cap`,
    :data:`.pdt_cap_ref`, and
    :data:`.t_share_ldv`.
  - Add snapshot/reference ``PRICE_EMISSION`` data from |ssp-scenariomip| scenarios.
- New :class:`.model.emissions.PRICE_EMISSION` to use snapshot data from file (:pull:`349`).

Projects and applications
-------------------------

- Update :mod:`.navigate.workflow` (:pull:`349`).
  :func:`~.navigate.workflow.tax_emission` uses :mod:`message_ix_models.tools.add_tax_emission`,
  not the version in :mod:`message_data`.
- :class:`.SSPUpdate` supports version "3.2.beta" of the SSP socioeconomic data (:pull:`349`).
- Improve :mod:`.ssp.transport` (:pull:`355`):

  - Modify data for variable=“Final Energy|Bunkers|International Aviation” and related (:issue:`308`).
  - Omit year=2020 from modified data.
  - New CLI utility :program:`mix-models ssp ceds-data-for-transport`.

Utilities, tools, and housekeeping
----------------------------------

- Simplify API and usage of :class:`.ExoDataSource` and its subclasses (:pull:`349`).

  - Most data source classes now have a corresponding :class:`.BaseOptions` subclass
    that indicate the options/arguments understood by that data source.
  - Adjust existing ExoDataSource classes and usage.

- Improve :class:`.URNLookupEnum`, add :class:`.ItemSchemeEnumType`
  and use these for :any:`.SSP_2017` and :any:`SSP_2024` (:pull:`349`).
  This class supports type checking and use of SDMX item scheme members as Python :class:`.Enum` members.
- Recompress :doc:`/project/advance` using LZMA (:pull:`349`).
- Remove :py:`message_ix_model.tools.advance` (:pull:`349`)
  that had been deprecated since :ref:`v2023.11.24`.
- :func:`.iamc_like_data_for_query` handles LZMA-compressed TAR files (:pull:`349`).
- Adjust type hints for `iiasa/ixmp#581 <https://github.com/iiasa/ixmp/pull/581>`_ (:pull:`349`).

v2025.5.28
==========

- In accordance with the :ref:`policy-upstream-versions`:

  - :mod:`message_ix_models` supports and is tested against :mod:`ixmp` and :mod:`message_ix_models` versions 3.10 (:pull:`299`) and 3.11 (:pull:`356`).
  - Support for :mod:`ixmp` and :mod:`message_ix` versions 3.4, 3.5, 3.6, and 3.7 is dropped  (:pull:`288`, :pull:`289`, :pull:`356`).
    The minimum supported version of both packages is 3.8.0.

- Update :class:`.IEA_EWEB` to support:

  - :py:`transform="B"` / :func:`.transform_B` (:issue:`230`, :pull:`259`).
  - :py:`transform="C"` / :func:`.transform_C` (:issue:`229`, :pull:`300`).
  - The :class:`~.web.TRANSFORM` enumeration for specifying and validating multiple transformations.

- Improve :class:`.ExoDataSource` with :attr:`~.ExoDataSource.use_test_data`,
  :attr:`~.ExoDataSource.where`, and :meth:`~.ExoDataSource._where` (:pull:`341`).
- Improve :class:`.MappingAdapter` with :py:`on_missing=…` option (:pull:`328`).
- New class :class:`.Dataflow` for describing input and/or output data flows (:pull:`300`) that are read from file and attached to a :class:`.Computer`.
  Generalized from former :py:`.transport.files.ExogenousDataFile`.
- New method :meth:`.Config.regions_from_scenario` (:pull:`300`),
  extracted/generalized from :meth:`.transport.Config.from_context`.
- New general-purpose :mod:`genno` operators in :mod:`.report.operator`:

  - :func:`.broadcast_wildcard` and :func:`.merge_data` (:pull:`300`),
    extracted/generalized from :mod:`.transport.operator`.
  - :func:`~.operator.call`, :func:`~.operator.select_allow_empty` (:pull:`328`).

- New utilities:

  - :class:`.genno.Collector` (:pull:`300`).
  - :class:`.sdmx.AnnotationsMixIn` (:pull:`259`).
  - :class:`.util.WildcardAdapter` (:pull:`328`).
  - :func:`.check.verbose_check` (:pull:`300`).

- Display entire result quantity in :func:`.report.report` / :program:`mix-models report` with :py:`verbose=True` (:pull:`300`).
- New test fixtures (:pull:`341`):
  :func:`.advance_test_data`,
  :func:`.gea_test_data`,
  :func:`.gfei_test_data`,
  :func:`.iea_eei_user_data`,
  :func:`.iea_eweb_test_data`,
  :func:`.iea_eweb_user_data`,
  :func:`.shape_test_data`,
  :func:`.ssp_test_data`,
  :func:`.ssp_user_data`.
- Migrate code, data, and documentation for :doc:`buildings/index`, :doc:`ENGAGE project <project/engage>`, :doc:`NAVIGATE project <project/navigate>`, and 9 :mod:`.tools` submodules (:pull:`350`, :pull:`351`):
  :mod:`.add_AFOLU_CO2_accounting`,
  :mod:`.add_CO2_emission_constraint`,
  :mod:`.add_FFI_CO2_accounting`,
  :mod:`.add_alternative_TCE_accounting`,
  :mod:`.add_budget`,
  :mod:`.add_emission_trajectory`,
  :mod:`.add_tax_emission`,
  :mod:`.remove_emission_bounds`, and
  :mod:`.update_h2_blending`.
- Bug fix: :program:`mix-models --verbose` command-line option was not stored on :class:`.Context`/:class:`~.util.Config` (:pull:`300`).
- Bug fix: adjust or guard some Python usage that was not compatible with Python 3.9
  —the earliest version supported by :mod:`message_ix_models` (:pull:`295`, :issue:`294`).
- Drop obsolete :py:`series_of_pint_quantity()` (:pull:`289`).

By topic:

.. contents::
   :local:
   :backlinks: none

SSP :ref:`ssp-2024`/ScenarioMIP
-------------------------------

- Add code in :mod:`.ssp.script` (:pull:`340`).

- Improve :mod:`.ssp.transport`:

  - Add :func:`~.ssp.transport.method_B` and make this the default (:pull:`259`, :pull:`330`).
  - Add :func:`~.ssp.transport.method_C` (:issue:`305`, :pull:`325`, :pull:`330`).
  - Add :func:`~.ssp.transport.process_df` (:pull:`303`);
    handle data frames containing :py:`np.NaN` (:pull:`330`).
  - Adapt to revised ‘variable’ codes (:pull:`309`, :issue:`304`).
- Add :func:`~.ssp.transport.method_B` and make this the default (:pull:`259`, :pull:`330`).
- Add :func:`~.ssp.transport.method_C` (:issue:`305`, :pull:`325`, :pull:`330`).
- Add :func:`~.ssp.transport.process_df` (:pull:`303`);
  handle data frames containing :py:`np.NaN` (:pull:`330`).
- Adapt to revised ‘variable’ codes (:pull:`309`, :issue:`304`).
- Expand covered emission species (:pull:`333`, :issue:`307`)
  with values derived from `CEDS <https://www.pnnl.gov/projects/ceds>`_.

Materials
---------

- Improve and extend :doc:`/material/index` (:pull:`345`).

Transport
---------

Update :doc:`/transport/index` (:pull:`259`, :pull:`289`, :pull:`300`, :pull:`328`).

- Adjust constraints on :py:`t="conm_ar"`.
- Drop some :attr:`.minimum_activity` entries; recompute values and add to :data:`.act_non_ldv`.
- Use :func:`message_ix.tools.migrate.initial_new_capacity_up_v311` in :mod:`.transport.workflow`.
- Improve freight representation:

  - Adjust freight activity, freight and passenger mode shares for some regions.
  - Add dynamic constraints on activity of freight technologies.
  - Fix alignment of freight technology outputs with demand |l|.
  - Add :data:`.elasticity_f`, analogous to :data:`.elasticity_p`.
    See :func:`.freight.demand`.
  - Simplify some technology IDs using the pattern ``f road {input commodity}``

- Implement LED override using exogenous passenger activity data from :ref:`transport-pdt-cap-proj`.
- New input data flow :data:`.constraint_dynamic`,
  replacing former :py:`transport.Config.constraint` setting,
  and module :mod:`.transport.constraint`,
  replacing parallel functions in :mod:`~.transport.freight`,
  :mod:`~.transport.ldv`, and
  :mod:`~.transport.passenger`.
- Update input data for :data:`.activity_freight`,
  :data:`.elasticity_p`,
  :data:`.ikarus_inv_cost`,
  :data:`.inv_cost_ldv`,
  :data:`.mode_share_freight`,
  :data:`.pdt_cap_ref`, and
  :data:`.speed`.
- Add LED-specific parametrization for :data:`.activity_ldv`,
  :data:`.lifetime_ldv`,
  :data:`.load_factor_ldv`, and
  :data:`.pdt_cap_proj`.
- Drop :file:`base-scenario-url.json`; store base scenario URLs in :ref:`CL_TRANSPORT_SCENARIO`.
- Generate SDMX-ML structural metadata, including data flow definitions, and SDMX-{CSV,ML} data outputs for certain reported quantities.

  - New input data flow :data:`.input_share`.
  - Add :py:`scenario` dimension to :data:`.activity_ldv`, :data:`.lifetime_ldv`, input data flow and files.
- Expand use of fixed/shared keys from :mod:`.transport.key`.
- Improve documentation:

  - Expand documentation of :doc:`input data flows <transport/input>` to include *all* input data flows.
  - Document some :doc:`output data flows <transport/output>`.
- New operator :func:`.indexer_scenario`.
- New plot :class:`.Scale1Diff`.
- Simplify signature of :func:`.transport.build.main`, :func:`.transport.build.get_computer`.
- Simplify and consolidate tests.
- Improve :func:`.simulated_solution` to load ‘simulated’ solution data from file to reduce test durations.

Documentation
-------------

- New :doc:`/howto/index` documentation sub-tree (:pull:`291`, :pull:`326`).
- New guide on HOWTO :doc:`/howto/unicc` (:pull:`279`) and supporting command :program:`mix-models sbatch` in :mod:`.util.slurm` (:pull:`291`).
- New summary pages for projects
  :doc:`project/alps`,
  :doc:`project/carbon-direct`,
  :doc:`project/cfr`,
  :doc:`project/ecemf`,
  :doc:`project/elevate`,
  :doc:`project/geidco`,
  :doc:`project/genie`,
  :doc:`project/guide`,
  :doc:`project/hyway`,
  :doc:`project/newpathways`,
  :doc:`project/nextgen-carbon`,
  :doc:`project/ngfs`,
  :doc:`project/prisma`,
  :doc:`project/sparccle`, and
  :doc:`project/uptake` (:pull:`282`, :pull:`312`).
- Expand the :ref:`costs-usage` section of the :mod:`.tools.costs` documentation to describe the requirement for SSP input data (:issue:`313`, :pull:`322`).
- Reorganize and improve the :doc:`data` documentation page (:pull:`326`).

.. _v2025.1.10:

v2025.1.10
==========

- :mod:`message_ix_models` is tested and compatible with `Python 3.13 <https://www.python.org/downloads/release/python-3130/>`__ (:pull:`250`).
- Support for Python 3.8 is dropped (:pull:`250`), as it has reached end-of-life.
- Expand :doc:`repro` with sections on :ref:`repro-doc` and :ref:`versioning`, including :ref:`a list of external model names and ‘versions’ <model-names>` like “MESSAGEix-GLOBIOM 2.0” (:issue:`224`, :pull:`226`).
- New code list :ref:`emission-yaml` (:pull:`225`).
- Designate :ref:`code-owners` for certain files (:pull:`272`) and document development practices.

By topic:

.. contents::
   :local:
   :backlinks: none

Materials
---------

- Improve and extend :doc:`/material/index` (:pull:`218`, :pull:`253`).
- Release of MESSAGEix-Materials 1.1.0 (:doc:`/material/v1.1.0`).

Transport
---------

- Update :doc:`/transport/index` (:pull:`213`, :pull:`225`).
- Rework :mod:`~.transport.freight`, :mod:`~.transport.ldv`, and :mod:`~.transport.non_ldv` to use :mod:`genno` consistently.
- Adopt consistent terms "F RAIL" and "F ROAD" for freight service/modes.
- New technologies: "f rail {electr,lightoil}", "f road electr".
- Extend unit annotations to all transport commodities.
- New input :doc:`files </transport/input>` :file:`pdt-cap.csv` and :file:`load-factor-ldv.csv`.
- Add :program:`mix-models ssp transport` CLI command to postprocess aviation emissions data.
- New SDMX Codelist ``IIASA:CL_TRANSPORT_SCENARIO`` to distinguish “Low energy demand (LED)” and :doc:`/project/edits` scenarios from :doc:`/project/ssp` baseline scenarios.
- Adjust R12 baseline settings:

  - :file:`mode-share/default.csv`: adjust ``R12_NAM`` values for AIR and LDV.
  - :file:`pdt-cap-ref.csv`: adjust ``R12_NAM`` value.
- Implement LED scenarios via :file:`load-factor-ldv.csv` and :file:`pdt-cap.csv`.
- Use y=2019 data from IEA EWEB 2024 edition to align MESSAGEix-Transport with base model calibration.
- New :mod:`genno` operators: :func:`~.transport.operator.broadcast`,  :func:`~.transport.operator.broadcast_wildcard`,  :func:`~.transport.operator.broadcast_t_c_l`, :func:`~.transport.operator.freight_usage_output`.
- New utility methods :func:`~.transport.config.get_cl_scenario`, :func:`~.transport.util.wildcard`.
- Replace :class:`DataSourceConfig.LDV <.transport.config.DataSourceConfig>` setting with :attr:`Config.dummy_LDV <.transport.config.Config.dummy_LDV>`.

Water/Nexus
-----------

- Update water availability data and major code editing to allow a new test suite for doc:`/water/index` (:pull:`106`).
- Fix the nexus/cooling function and add test for checking some input data (:pull:`236`).
- Make setup of constraints for cooling technologies flexible and update solar CSP technology name (:pull:`242`).
- Connect :mod:`.model.water` to :mod:`.tools.costs` for cooling technologies (:pull:`245`).
- Introduce an SSP configuration for cooling technologies with share constraints (:pull:`256`).
- Change SSP parameters and add missing CSP data (:pull:`262`).
- Add additional final constraints and data on cooling technologies for the SSP runs (:pull:`268`).

Investment and fixed costs
--------------------------

- Fix naming of GDP and population columns in SSP data aggregation for :doc:`/api/tools-costs` (:pull:`219`).
- Edit inputs for storage, CSP, hydrogen, and industry technologies (:pull:`206`).
- Replace solar and wind technologies with new ones (:pull:`206`).
- Reorganize input files and incorporate `first_year.csv` data into `tech_map.csv` (:pull:`221`).
- Reconfigure use and implementation of technology variants/modules to be more agnostic (:pull:`221`).
- Change cost decay to reach reduction percentage specified on the year 2100 (:pull:`227`).
- Add `cooling` technology variant/module (:pull:`222`).
- Add functionality to specify cost reduction values and cost reduction scenarios in a module (:issue:`251`, :pull:`255`).
- Fix technology mapping of ammonia CCS technologies in materials module (:pull:`258`).
- Remove unused `technology_type` column from tool (:pull:`269`).

Others
------

- Add "LED", "SSP4", and "SSP5" as values for the :program:`--ssp=…` option in :func:`.common_params` (:pull:`233`).
- Add :doc:`/project/circeular` project code and documentation (:pull:`232`).
- :mod:`.iea.web` handles the 2024 edition and fixed-width file format published by the IEA directly (:pull:`225`).

  - Code lists for the ``COUNTRY``, ``FLOW``, and ``PRODUCT`` concepts are included with :mod:`message_ix_models`.
  - Add :data:`~.iea.web.COUNTRY_NAME` to map particular labels appearing in these data.
  - Fuzzed data and tests for this functionality.

- Add :any:`.types.ParameterData` and :any:`.types.MutableParameterData` to type the common internal data structure in which a :class:`dict` maps from MESSAGE parameter names to :mod:`message_ix`-structured :class:`pandas.DataFrame` (:pull:`225`).
- :class:`message_ix_models.Config`—the “core” configuration class—gains methods specific to its settings (:pull:`225`).
  These were formerly on :class:`.Context`.
- :class:`.Context` is no longer a subclass of :class:`dict` (:pull:`225`).
  This avoids its mishandling by :mod:`dask` version 2024.11.0 or later when Context is used with :class:`ixmp.Reporter`.

  - Add :data:`.MODULE_WITH_CONFIG_DATACLASS`;
    add attributes such as :attr:`.Context.model` that are typed to the respective class such as :class:`.model.Config`.
  - Add :meth:`.Context.asdict` for serialization.
  - Former methods such as :meth:`.Context.get_scenario` are aliased to their new locations, e.g. :meth:`.Config.get_scenario`.

- Improve :class:`.ScenarioInfo` (:pull:`225`):

  - Implement the :py:`|` (logical OR/union) operator: `si_a | si_b` is a new ScenarioInfo instance with the union of the contents of the operands.
  - New method :meth:`.substitute_codes` to replace string codes (for instance, determined from a Scenario object) with Codes from the corresponding code list, including all annotations.

- :class:`.MappingAdapter` skips missing labels in the input data without raising an exception (:pull:`225`).
- :meth:`.Workflow.visualize` displays in left-to-right rank direction by default (:pull:`225`).
- :func:`.convert_units` can handle MESSAGE-scheme :class:`pandas.DataFrame` (:pull:`225`).
- :func:`.util.sdmx.make_enum` uses :class:`.URNLookupEnum` by default (:pull:`225`).
  This allows to call, for instance, :py:`SSP_2024.by_urn("…Code=ICONICS:SSP(2024).2")` to retrieve an enumeration number.
- :func:`.make_matched_dfs` accepts :class:`dict` for its :py:`par_value` arg, allowing replacement of values for particular dimensions of output data frames (:pull:`225`).
- New reporting operator :func:`.quantity_from_iamc` (:pull:`225`).
- :func:`.same_node` and :func:`.same_time` can handle :any:`.ParameterData` (:pull:`225`).
- :func:`.report.register` becomes :meth:`.report.Config.register` which populates :attr:`.report.Config.callback` (:pull:`225`).
  The set of callback functions used to configure a class:`.Reporter` instance is thus now particular to a :class:`.report.Config` instance attached to a :class:`.Context` instance.
  This allows better isolation of code/tests that use different sets of callbacks.

v2024.8.6
=========

- Add :doc:`/material/index` (:pull:`188`, :pull:`189`).
- Update :doc:`/material/index` (:pull:`201`).
- Add :doc:`/transport/index` (:pull:`207`, :pull:`208`, :issue:`210`, :pull:`212`).
- Add :doc:`/project/edits` project code and documentation (:pull:`204`).
- Reduce log verbosity of :func:`.apply_spec` (:pull:`202`).
- Fix and update :doc:`/api/tools-costs` (:pull:`186`, :pull:`187`, :pull:`190`, :pull:`195`).

  - Fix jumps in cost projections for technologies with first technology year that's after than the first model year (:pull:`186`).
  - Change the use of base_year to mean the year to start modeling cost changes (:pull:`186`).
  - Update cost assumptions for certain CCS technologies (:pull:`186`).
  - Change the default fixed O&M reduction rate to 0 (:pull:`186`).
  - Modify to use 2023 release of IEA WEO data and to use 2022 historic data for the base year (:pull:`187`).
  - Change the default final year to 2110 (:pull:`190`).
  - Add :attr:`~.costs.Config.use_vintages` to control whether vintages are used in computing fixed O&M costs (:pull:`195`).

v2024.4.22
==========

- Incorporate the :doc:`/global/index` documentation (:pull:`107`, :pull:`110`).
  This documentation formerly lived at https://docs.messageix.org/global/ and in a separate repository at `iiasa/message_doc <https://github.com/iiasa/message_doc>`_.
- Improve tool for :ref:`migrate-filter-repo` (:pull:`174`); expand documentation.
- New module for :doc:`/api/tools-costs` (:pull:`99`).
- Update investment cost assumptions in :doc:`/api/tools-costs` for wind and solar technologies (:pull:`176`).

  - Remove manually specified base year reference region costs for solar_pv_ppl and solar_pv_RC so that 2021 IEA WEO costs are used for these technologies.
  - Fix the manually specified base year reference region cost for wind_ppf.
  - Update cost reduction rates and scenarios for wind_ppf so that it follows the same narratives as wind_ppl.
- Convert Intratec data for :doc:`/api/tools-costs` from Excel to a simpler CSV format. (:pull:`167`).
- Migrate :doc:`/api/report/legacy` to provide post-processing functionality for the :doc:`global model snapshot </api/model-snapshot>` (:pull:`159`).
- Migrate and improve code for four sources of exogenous data (:pull:`162`): :mod:`.project.gea`, :mod:`.project.shape`, :mod:`.tools.gfei`, and :mod:`.tools.iea.eei`.
- Add a :doc:`quickstart` (:pull:`157`).
- Expand :doc:`data` (:pull:`161`).
- Add an explicit :ref:`policy-upstream-versions` (:pull:`162`).

v2024.4.2
=========

- The :class:`.SSPUpdate` data provider pulls data from the SSP 2024 "Release 3.0" data files, and handles both the earlier and current structures (:pull:`156`).
- Improve :class:`.ExoDataSource` with :meth:`.raise_on_extra_kw` utility method, automatic copy of source keyword arguments (:pull:`156`).
- Expose :func:`.node.nodes_ex_world` for use as a genno (reporting) operator.
- Raise DeprecationWarning from :func:`.util.sdmx.eval_anno`; remove internal usage of this deprecated method (:pull:`156`).
- Reduce verbosity when using the :program:`mix-models` CLI when :mod:`message_data` is not installed (:issue:`37`, :pull:`156`).
- Improve logging (:pull:`156`).

  - Use multi-threaded logging for better performance.
    Logging to stdout and file is on a separate thread and does not block operations on the main thread.
  - Add automatic file logging.
    Log versions of packages to file when using :func:`.workflow.make_click_command`.
  - New CLI command :program:`mix-models last-log` to retrieve the location of the latest log file.
- Update :doc:`cli` (:pull:`156`).
- Improve performance in :func:`.disutility.data_conversion` (:pull:`156`).
- Use :func:`platformdirs.user_cache_path` in more places; remove cache-path handling code (:pull:`156`).
- Add :func:`.util.datetime_now_with_tz` (:pull:`156`).
- Add :func:`.util.show_versions`, wrapping :func:`ixmp.util.show_versions` and returning its output as :class:`str` (:pull:`156`).
- :func:`.util.private_data_path` returns an alternate, local data path if :mod:`message_data` is not installed (:pull:`156`).
- Annotate :py:`c="transport"` in :ref:`the commodity code list <commodity-yaml>` with associated :ref:`IEA (E)WEB <tools-iea-web>` flows (:pull:`153`).

v2024.1.29
==========

- Add :ref:`tools-iea-web` for handling data from the International Energy Agency (IEA) Extended World Energy Balances (:issue:`25`, :pull:`75`).
- Add :ref:`tools-wb` and :func:`.assign_income_groups` to assign MESSAGE regions to World Bank income groups (:pull:`144`).
- Adjust :mod:`.report.compat` for genno version 1.22 (:issue:`141`, :pull:`142`).
- Raise informative exception from :meth:`.ScenarioInfo.io_units` (:pull:`151`).

.. _v2023.11.24:

v2023.11.24
===========

Migration notes
---------------
Update code that imports from the following modules:

- :py:`message_ix_models.report.computations` → use :py:`message_ix_models.report.operator`.

Code that imports from the old locations will continue to work, but will raise :class:`DeprecationWarning`.

Data for :doc:`water/index` is no longer included in the PyPI distributions for :mod:`message_ix_models`.
This reduces the package size from >20 MB to <5 MB.
To automatically download and unpack these data into a local directory, use :program:`mix-models fetch MESSAGEix-Nexus`.

All changes
-----------

- Improve :class:`.ExoDataSource` (:pull:`137`):

  - New attributes :attr:`~.ExoDataSource.name`, :attr:`~.ExoDataSource.extra_dims`.
  - New method :meth:`~.ExoDataSource.transform` that can be overridden by subclasses.
  - New arguments :py:`archive_member`, :py:`non_iso_3166` to :func:`.iamc_like_data_for_query`.

- New provider for exogenous data from the :class:`.ADVANCE` project (:pull:`137`).
  This module, :mod:`.project.advance`, supersedes :mod:`.tools.advance` and its idiosyncratic API, which are deprecated.
- New CLI commands (:pull:`137`):

  - :program:`mix-models testing fuzz-private-data`, superseding :program:`mix-models ssp make-test-data`.
  - :program:`mix-models fetch`, superseding :program:`mix-models snapshot fetch`.

- New utility functions  (:pull:`137`).

  - :func:`.tools.iamc.describe` to generate SDMX code lists that describe the structure of particular IAMC-format data (:pull:`137`).
  - :func:`.workflow.make_click_command` to generate :mod:`click` commands for any :class:`.Workflow`.
  - :func:`.util.minimum_version` to ensure compatibility with upstream packages and aid test writing.
  - :func:`.util.iter_keys` to generate keys for chains of :mod:`genno` computations.

- Add :mod:`message_ix_models.report.compat` :ref:`for emulating legacy reporting <report-legacy>` (:pull:`134`).
- Rename :mod:`message_ix_models.report.operator` (:pull:`137`).
- Deprecate :py:`iter_parameters()` in favour of :meth:`ixmp.Scenario.par_list` with :py:`indexed_by=...` argument from ixmp v3.8.0 (:pull:`137`).


v2023.10.16
===========

- New providers for exogenous data from the :class:`.SSPOriginal` and :class:`.SSPUpdate` (:pull:`125`) sources.
- Improved :class:`.ScenarioInfo` (:pull:`125`):

  - New attributes :attr:`~.ScenarioInfo.model`, :attr:`~.ScenarioInfo.scenario`, :attr:`~.ScenarioInfo.version`, and (settable) :attr:`~.ScenarioInfo.url`; class method :meth:`~.ScenarioInfo.from_url` to allow storing :class:`.Scenario` identifiers on ScenarioInfo objects.
  - New property :attr:`~.ScenarioInfo.path`, giving a valid path name for scenario-specific file I/O.

- Improvements to :mod:`~message_ix_models.report` (:pull:`125`):

  - New :class:`.report.Config` class collecting recognized settings for the module.
  - :py:`context["report"]` always exists as an instance of :class:`.report.Config`.
  - New submodule :mod:`.report.plot` with base class and 5 plots of time-series data stored on Scenarios.
  - Submodule :mod:`.report.sim` provides :func:`.add_simulated_solution` for testing reporting configuration.
  - New operator :func:`.filter_ts`.

- New reusable command-line option :program:`--urls-from-file` in :mod:`.util.click` (:pull:`125`).
- Add `pyarrow <https://pypi.org/project/pyarrow/>`_ to dependencies (:pull:`125`).

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

- Add :func:`~.util.node.nodes_ex_world` and use this in :func:`.disutility.data_conversion` instead of expected a "World" node ID to be the first element in :attr:`.ScenarioInfo.N` (:pull:`78`).
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
- :func:`.as_codes` accepts a :class:`.dict` with :class:`.Code` values (:pull:`39`).

Earlier releases
================

2021.11.24
----------

- Add :command:`--years` and :command:`--nodes` to :func:`.common_params` (:pull:`35`).
- New utility function :func:`.structure.codelists` (:pull:`35`).

2021.7.27
---------

- Improve caching using :mod:`genno` v1.8.0 (:pull:`29`).

2021.7.22
---------

- Migrate utilities :func:`.cached`, :func:`.check_support`, :func:`.convert_units`, :func:`.maybe_query`, :py:`series_of_pint_quantity()` (:pull:`27`)
- Add :data:`.testing.NIE`.
- Add the ``--jvmargs`` option to :command:`pytest` (see :func:`.pytest_addoption`).
- Remove :py:`.Context.get_config_file()`, :py:`.get_path()`, :py:`.load_config()`, and :py:`.units`, all deprecated since 2021-02-28.

2021.7.6
--------

- Add :func:`.identify_nodes`, a function for identifying a :doc:`pkg-data/node` based on a :class:`.Scenario` (:pull:`24`).
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
- Migrate utilities: :class:`.ScenarioInfo`, :func:`.add_par_data`, :func:`.eval_anno`, :py:`iter_parameters()`, and :func:`.strip_par_data`.

2021.3.3
--------

- Migrate :mod:`.util.click`, :mod:`.util.logging <.util._logging>`; expand documentation (:pull:`8`:).
- :meth:`.Context.clone_to_dest` method replaces :py:`clone_to_dest()` function.
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
