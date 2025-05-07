Configuration and input data
****************************

This page describes the structure and format of inputs required for building MESSAGEix-Transport.

.. contents::
   :local:
   :backlinks: none

Both input data and configuration are stored in files under :file:`message_ix_models/data/transport/`.

In most cases, these files are read from a subdirectory like :file:`…/data/transport/{nodes}/`, where `nodes` denotes the :mod:`message_ix_models` :doc:`node code list </pkg-data/node>`—for instance, "R12"—for which MESSAGEix-Transport will be built.
This value is retrieved from the :attr:`.model.Config.regions` setting on a :class:`.Context` object.

- If the file data or configuration settings have a node (|n|) dimension, the file **must** be placed in such a subdirectory.
  Data for one node list is not usable for base models using a different node list.
- For other data, a node list–specific file **may** be used.
  If none exists, the file of the same name in :file:`…/data/transport/` is used as a default.
  For example, :file:`…/data/transport/R12/set.yaml` is used if it exists; if not, then :file:`…/data/transport/set.yaml` is used.

.. _transport-config:

Configuration and model structure
=================================

General (:file:`config.yaml`, required)
---------------------------------------

The contents of this configuration file exactly map to the attributes of the class :class:`transport.Config <.transport.config.Config>`.
The class stores all the settings understood by the code for building, solving, and reporting MESSAGEix-Transport.
The class also defines the default values for each setting (there is no file :file:`…/data/transport/config.yaml` containing defaults.)
It also has methods for reading the configuration from file; see the detailed description of :meth:`.Config.from_context`.

The following is the configuration file for a base model with R12 nodes:

→ View :source:`data/transport/R12/config.yaml` on GitHub

Technology code list (:file:`technology.yaml`)
----------------------------------------------

This file gives the code list for the MESSAGE ``technology`` concept/set/dimension.
Some annotations (``iea-eweb-flow``, ``input``, ``report``) and the :attr:`~sdmx.model.common.Code.child` hierarchy give information about technologies' grouping according to transport modes.

→ View :source:`message_ix_models/data/transport/technology.yaml` on GitHub

Code lists for other MESSAGE sets (:file:`set.yaml`)
----------------------------------------------------

This file gives code lists for other MESSAGE concepts/sets/dimensions.

→ View :source:`message_ix_models/data/transport/set.yaml` on GitHub

.. _CL_TRANSPORT_SCENARIO:

Code list ``CL_TRANSPORT_SCENARIO``
-----------------------------------

This code list, stored in the file :file:`message_ix_models/data/sdmx/IIASA_ECE_CL_TRANSPORT_SCENARIO(1.0.0).xml`, contains an SDMX code list for distinct MESSAGEix-Transport scenarios.
The codes have IDs like ``LED-SSP1`` that give a short identifier used in :mod:`.transport.workflow` and elsewhere, and names that give a complete, human-readable description.
Every code has all of following annotations:

``SSP-URN``
   Complete URN of a code in ``ICONICS:SSP(2024)`` or another code list for the SSP used for sociodemographic input data and to control other settings in :mod:`.transport.build`.

   Example annotation text: ``'urn:sdmx:org.sdmx.infomodel.codelist.Code=ICONICS:SSP(2024).1'``

``is-LED-Scenario``
   Example annotation text: ``True``

   :func:`repr` of Python :any:`True` or :any:`False`, the former indicating that "Low Energy Demand (LED)" settings should be used.
   See also :attr:`Config.project <.transport.config.Config.project>`.

``EDITS-activity-id``
   Example annotation text: ``'HA'``

   For :doc:`/project/edits`, the identity of an ITF PASTA scenario providing exogenous transport activity.

``base-scenario-URL``
   Example annotation text: ``'ixmp://ixmp-dev/SSP_SSP1_v1.1/baseline_DEFAULT_step_13'``

   URL of a base scenario used to build the corresponding MESSAGEix-Transport scenario.


.. _transport-data-files:

Input data flows
================

The module :mod:`.transport.data` contains a number of :class:`.Dataflow` instances, listed below, that each describe an input or output data flow.
For each of the input data flows:

- the :attr:`.Dataflow.path` attribute gives a *file path* where a CSV file with input data is expected.
- the :attr:`.Dataflow.key` attribute gives the :class:`~.genno.Key` where loaded and transformed data from the file is available.
  (See also :data:`.transport.key.exo`, which allows access to all of these keys.)
  The key also expresses the dimensions of the input data flow.
- The additional metadata explains the measure concept, units of measure, etc.

Through :func:`.transport.build.main` (ultimately, :func:`.transport.build.add_exogenous_data` and :meth:`.Dataflow.add_tasks`), each of these files is connected to a :class:`genno.Computer` used for building MESSAGEix-Transport.
Its contents are available as a quantity at the corresponding key, which is used as an input for further model-building computations.

.. admonition:: Example: :data:`~.data.mode_share_freight`

   - Contents of the file :file:`freight-mode-share-ref.csv` are available at the key ``freight mode share:n-t:ref``.
   - The key indicates the dimensionality of this quantity is :math:`(n, t)`.
   - The corresponding CSV file has column headers "node", "technology", and "value".

Not all files are currently or always used in model-building computations.
Some submodules of :mod:`~.model.transport` use additional data files loaded or processed via other methods; see below under “Other data sources.”
Most of the files have a header comment including the source of the data and units of measurement.
In some cases—where a header comment would be too long—extended information is below.
The :program:`git` history of files, or the GitHub "blame" view can also be used to inspect the edit history of each file, line by line.

Quick links to each of the data flows:
:data:`~.data.act_non_ldv`
:data:`~.data.activity_freight`
:data:`~.data.activity_ldv`
:data:`~.data.age_ldv`
:data:`~.data.cap_new_ldv`
:data:`~.data.class_ldv`
:data:`~.data.constraint_dynamic`
:data:`~.data.disutility`
:data:`~.data.demand_scale`
:data:`~.data.elasticity_f`
:data:`~.data.elasticity_p`
:data:`~.data.emi_intensity`
:data:`~.data.energy_other`
:data:`~.data.fuel_emi_intensity`
:data:`~.data.ikarus_availability`
:data:`~.data.ikarus_fix_cost`
:data:`~.data.ikarus_input`
:data:`~.data.ikarus_inv_cost`
:data:`~.data.ikarus_technical_lifetime`
:data:`~.data.ikarus_var_cost`
:data:`~.data.input_adj_ldv`
:data:`~.data.input_base`
:data:`~.data.input_ref_ldv`
:data:`~.data.input_share`
:data:`~.data.lifetime_ldv`
:data:`~.data.load_factor_ldv`
:data:`~.data.load_factor_nonldv`
:data:`~.data.mer_to_ppp`
:data:`~.data.mode_share_freight`
:data:`~.data.pdt_cap_proj`
:data:`~.data.pdt_cap_ref`
:data:`~.data.pop_share_attitude`
:data:`~.data.pop_share_cd_at`
:data:`~.data.pop_share_driver`
:data:`~.data.population_suburb_share`
:data:`~.data.speed`
:data:`~.data.t_share_ldv`

.. autodata:: message_ix_models.model.transport.data.act_non_ldv
.. autodata:: message_ix_models.model.transport.data.activity_freight
.. autodata:: message_ix_models.model.transport.data.activity_ldv

   node = R12_AFR [1]_
     Obtained from literature, based on estimates from South Africa. The reported value for South Africa is lower (18000 km/year, `source <https://blog.sbtjapan.com/car-info/what-mileage-is-good-for-a-used-car#:~:text=Average%20Mileage%20in%20South%20Africa,is%20just%20a%20general%20guideline>`__) than the one for Kenya (22000 km/year, `source <https://www.changing-transport.org/wp-content/uploads/2019_Updated-transport-data-in-Kenya.pdf>`__).

   node = R12_FSU [1]_
     Based on Russia estimates (`source <https://eng.autostat.ru/news/17616/>`__).

   node = R12_NAM [1]_
     Based on US estimates (`source <https://afdc.energy.gov/data/10309>`__`), Canada estimates tend to [be] lower in general.

   node = R12_PAO [1]_
     Estimates for AU is 11000 in 2020, it's a sharp decrease from 12600 in 2018 (maybe a Covid effect?).
     Whereas JP is 8532 (`source <https://www.mlit.go.jp/road/road_e/statistics.html>`__) in 2016.

   node = R12_PAS [1]_
     Based on Singapore by `Chong et al. (2018) <https://doi.org/10.1016/j.enconman.2017.12.083>`__.

   node = R12_SAS [1]_
     Based on India, mainly Delhi estimate by `Goel et al. (2015) <https://doi.org/10.1016/j.tbs.2014.10.001>`__.

   .. [1] A. Javaid, `message_data#180 (comment) <https://github.com/iiasa/message_data/issues/180#issuecomment-1944227441>`__.

.. autodata:: message_ix_models.model.transport.data.age_ldv
.. autodata:: message_ix_models.model.transport.data.cap_new_ldv
.. autodata:: message_ix_models.model.transport.data.class_ldv
.. autodata:: message_ix_models.model.transport.data.constraint_dynamic

   The values for ``growth_*`` are allowable *annual* decrease or increase (respectively)
   in activity of each technology.
   For example,
   a value of 0.01 means the activity may increase by 1% from one year to the next.
   For periods of length >1 year, MESSAGE compounds the value.
   Some values used include:

   - ±0.0192 = (1.1 ^ (1 / 5)) - 1.0; or ±10% each 5 years.
   - ±0.0371 = (1.2 ^ (1 / 5)) - 1.0; or ±20% each 5 years.
   - ±0.0539 = (1.3 ^ (1 / 5)) - 1.0; or ±30% each 5 years.
   - ±0.0696 = (1.4 ^ (1 / 5)) - 1.0; or ±40% each 5 years.

   Values for ``initial_*_up`` are initial values for growth constraints.
   If these values are not large enough,
   they can cause infeasibilities in the base period
   for technologies that do not have ``historical_activity``.

   See also:

   - :func:`ldv.constraint_data` that handles values for :py:`technology="LDV"`.
   - :func:`non_ldv.growth_new_capacity` that handles values for :py:`technology="P ex LDV"`.

.. autodata:: message_ix_models.model.transport.data.disutility
.. autodata:: message_ix_models.model.transport.data.demand_scale
.. autodata:: message_ix_models.model.transport.data.elasticity_f
.. autodata:: message_ix_models.model.transport.data.elasticity_p

   Codes on the ‘scenario’ dimension are partial URNs for codes in the :class:`.SSP_2024` code list.
   Used via :func:`.pdt_per_capita`, which interpolates on the |y| dimension.

.. _transport-input-emi-intensity:

.. autodata:: message_ix_models.model.transport.data.emi_intensity

   See the file :source:`on GitHub <message_ix_models/data/transport/emi-intensity.csv>` for inline comments and commit history.

   Currently only used in :mod:`.ssp.transport`.

.. autodata:: message_ix_models.model.transport.data.energy_other
.. autodata:: message_ix_models.model.transport.data.fuel_emi_intensity
.. autodata:: message_ix_models.model.transport.data.ikarus_availability
.. autodata:: message_ix_models.model.transport.data.ikarus_fix_cost
.. autodata:: message_ix_models.model.transport.data.ikarus_input
.. autodata:: message_ix_models.model.transport.data.ikarus_inv_cost
.. autodata:: message_ix_models.model.transport.data.ikarus_technical_lifetime
.. autodata:: message_ix_models.model.transport.data.ikarus_var_cost
.. autodata:: message_ix_models.model.transport.data.input_adj_ldv
.. autodata:: message_ix_models.model.transport.data.input_base
.. autodata:: message_ix_models.model.transport.data.input_ref_ldv
.. autodata:: message_ix_models.model.transport.data.input_share
.. autodata:: message_ix_models.model.transport.data.lifetime_ldv
.. autodata:: message_ix_models.model.transport.data.load_factor_ldv

   The code that handles this file interpolates on the |y| dimension.

   Original source for the R12 version: duplicate of :file:`R11/load-factor-ldv.csv` with R12_CHN and R12_RCPA values filled from R11_CPA.

   Values for :py:`scenario="LED"` added in :pull:`225`, prepared using a method described in `this Slack message <https://iiasa-ece.slack.com/archives/CCFHDNA6P/p1731914351904059?thread_ts=1730218237.960269&cid=CCFHDNA6P>`_.

   .. todo:: Transcribe the method into this document.

.. autodata:: message_ix_models.model.transport.data.load_factor_nonldv
.. autodata:: message_ix_models.model.transport.data.mer_to_ppp
.. autodata:: message_ix_models.model.transport.data.mode_share_freight

.. _transport-pdt-cap-proj:
.. autodata:: message_ix_models.model.transport.data.pdt_cap_proj

   This file is only used for :math:`s` values such as :py:`scenario="LED"`, in which case it is the source for projected PDT per capita.

   Values for :py:`scenario="LED"` added in :pull:`225` using a method described in `this Slack message <https://iiasa-ece.slack.com/archives/CCFHDNA6P/p1731510626983289?thread_ts=1730218237.960269&cid=CCFHDNA6P>`__.

   .. todo:: Transcribe the method into this document.

.. autodata:: message_ix_models.model.transport.data.pdt_cap_ref

   node = R12_CHN [4]_
      Based on the vehicle activity method `Liu, et al. 2022`_ estimate the total PDT for R12_CHN for year (2017) is 9406 billion pkm.
      This is the latest corrected estimate available from Liu, et al. 2022.
      Based on similar estimates for 2013 & 2015, I estimate the average growth of PDT to be 8% per year.
      Using the growth rate and 2017 estimate, the total PDT for year (2020) comes out to be 11848.9 billion pkm.

      R12_CHN population estimate from IMAGE: 1.4483 billion

      Thus PDT/capita = 11848.9 / 1.4483

   .. [4] A. Javaid, `message_data#538 (comment) <https://github.com/iiasa/message_data/issues/538#issuecomment-1934663340>`__.

.. autodata:: message_ix_models.model.transport.data.pop_share_attitude
.. autodata:: message_ix_models.model.transport.data.pop_share_cd_at
.. autodata:: message_ix_models.model.transport.data.pop_share_driver
.. autodata:: message_ix_models.model.transport.data.population_suburb_share
.. autodata:: message_ix_models.model.transport.data.speed

   In MESSAGE(V)-Transport, values from Schäefer et al. (2010) were used.

.. autodata:: message_ix_models.model.transport.data.t_share_ldv

Other data sources
==================

:mod:`~.model.transport` makes use of the :mod:`.tools.exo_data` mechanism to retrieve data from common (not transport-specific) sources.
:class:`.DataSourceConfig`, :attr:`.transport.Config.ssp`, and other settings determine which sources and quantities are used.

These include:

- GDP and population from the :mod:`.project.ssp` data sources or other sources including the ADVANCE project, the Global Energy Assessment project, the SHAPE project, etc.

  .. note:: Formerly, file :file:`gdp.csv` was used.

   This is no longer supported; instead, use databases via :func:`.exo_data.prepare_computer` or introduce quantities with the same dimensions and units into the :class:`.Computer` used for model building/reporting.

- Energy from the IEA Extended World Energy Balances.
- :class:`.IEA_Future_of_Trucks`.
- :class:`.MERtoPPP`.

:file:`ldv-fix_cost.csv`, :file:`ldv-inv_cost.csv`, :file:`ldv-fuel-economy.csv`
--------------------------------------------------------------------------------

Data on costs and efficiencies of LDV technologies.

Formerly this data was read from :file:`ldv-cost-efficiency.xlsx`, a highly-structured spreadsheet that performs some input calculations.
The function :func:`.get_USTIMES_MA3T` reads data from multiple sheets in this file.
To understand the sheet names and cell layout expected, see the code for that function.

As the name implies, the data for :doc:`MESSAGE (V)-Transport <old>` was derived from the US-TIMES and MA³T models.

:file:`mode-share/default.csv`
------------------------------

Measure
   Share of each mode in passenger transport activity in the model base year
Dimensions
   :math:`(n, t)` with transport modes expressed in the :math:`t` dimensions.
Units
   dimensionless

Notes
~~~~~

node = R12_AFR [2]_
   These new estimates are mainly based on IMAGE regional estimates (average of EA, WA, and SA) after discussion with Jarmo as well as an additional literature search + guesstimates from vehicle count etc.
   Still, no comprehensive source to validate these.
   Only broad qualitative impressions formed from the literature.
   More details in [other] notes.

node = R12_CHN [3]_
   Based on the total pdt and mode share breakdown from 2017^ as reported in `Liu, et al. 2022 <https://doi.org/10.1016/j.accre.2022.01.009>`_, and extrapolating to 2020 (assuming the mode share in 2020 is the same as the one in 2017).

   Subtracting Waterways from PDT.
   RAIL includes both urban PT & RAIL.
   BUS includes both local buses and COACH.

node = R12_MEA [2]_
   These new estimates are mainly based on IMAGE regional estimates (average of ME & NA) guesstimates from vehicle count etc. Same as [R12_AFR].

node = R12_PAO [2]_
   Estimated from weighing Japan (0.80) & Aus/NZ (0.2) by population.
   JP source is ATO, Statistics Japan, IEA.
   AU source is BITRE 2021.
   Motorcycle share is guess-timate based on no. of motorbikes, load factor, and comparing it to cars.
   BUS estimate for Japan is based on ATO data, less certain as source is missing.
   More details in [other] notes.

node = R12_SAS [2]_
   Estimated from India ATO & OECD sources.
   Rest of SA is likely to have lower RAIL share.
   2W share also includes 3W (Auto rickshaw).

.. [2] A. Javaid, `message_data#180 (comment) <https://github.com/iiasa/message_data/issues/180#issuecomment-1941860412>`_.
.. [3] A. Javaid, `message_data#538 (comment) <https://github.com/iiasa/message_data/issues/538#issuecomment-1934663340>`__.
