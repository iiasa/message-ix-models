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

Configuration
=============

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

.. _transport-data-files:

Input data files
================

:data:`.transport.files.FILES` gives a list of all data files.
Through :func:`.transport.build.main` (ultimately, :func:`.transport.build.add_exogenous_data`), each of these files is connected to a :class:`genno.Computer` used for model-building, and its contents appear at the key given in the list below.

.. admonition:: Example

   Contents of the file :file:`freight-mode-share-ref.csv` are available at the key ``freight mode share:n-t:ref``.
   The indicates the dimensionality of this quantity is :math:`(n, t)`.
   The file has column headers "node", "technology", and "value".

Not all files are currently or always used in model-building computations.
Some submodules of :mod:`~.model.transport` use additional data files via other mechanisms.
Most of the files have a header comment including a precise description of the quantity, source of the data, and units of measurement.
In some cases extended information is below (where a header comment would be too long).
The :program:`git` history of files, or the GitHub "blame" view can also be used to inspect the edit history of each file, line by line.

:file:`ldv-activity.csv` → ``ldv activity:n:exo``
-------------------------------------------------

Measure
   Activity (driving distance) per light-duty vehicle
Units
   kilometre / year

Notes
~~~~~

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

:file:`pdt-cap.csv` → ``P activity:scenario-n-t-y:exo``
-------------------------------------------------------

Measure
   Projected PDT per capita
Dimensions
   :math:`(s, n, t, y)`
Units:
   km / passenger / year

- This file is only used for :math:`s` values such as :py:`scenario="LED"`, in which case it is the source for projected
PDT per capita.
- Values for :py:`scenario="LED"` added in :pull:`225`.
  Method described in `this Slack message <https://iiasa-ece.slack.com/archives/CCFHDNA6P/p1731510626983289?thread_ts=1730218237.960269&cid=CCFHDNA6P>`__.

  .. todo:: Transcribe the method into this document.


:file:`pdt-cap-ref.csv` → ``pdt:n:capita+ref``
----------------------------------------------

Measure
   Passenger distance travelled per capita in the model base year
Dimensions
   :math:`(n)`
Units
   km / year

Notes
~~~~~

node = R12_CHN [4]_
   Based on the vehicle activity method `Liu, et al. 2022`_ estimate the total PDT for R12_CHN for year (2017) is 9406 billion pkm.
   This is the latest corrected estimate available from Liu, et al. 2022.
   Based on similar estimates for 2013 & 2015, I estimate the average growth of PDT to be 8% per year.
   Using the growth rate and 2017 estimate, the total PDT for year (2020) comes out to be 11848.9 billion pkm.

   R12_CHN population estimate from IMAGE: 1.4483 billion

   the PDT/capita = 11848.9/1.4483

.. [4] A. Javaid, `message_data#538 (comment) <https://github.com/iiasa/message_data/issues/538#issuecomment-1934663340>`__.

:file:`pdt-elasticity.csv` → ``pdt elasticity:scenario-n:exo``
--------------------------------------------------------------

Measure
   “Elasticity” or multiplier for GDP PPP per capita
Dimensions
   :math:`(n, \text{scenario})`.
   ‘scenario’ identifiers are partial URNs for codes in the :class:`.SSP_2024` code list.
Units
   dimensionless
Where/how used
   :func:`.pdt_per_capita`.

:file:`load-factor-ldv.csv` → ``load factor ldv:scenario-n-y:exo``
------------------------------------------------------------------

- Original source: Duplicate of :file:`…/R11/load-factor-ldv.csv` with R12_CHN and R12_RCPA values filled from R11_CPA.
- Values for :py:`scenario="LED"` added in :pull:`225`.
  Method described in `this Slack message <https://iiasa-ece.slack.com/archives/CCFHDNA6P/p1731914351904059?thread_ts=1730218237.960269&cid=CCFHDNA6P>`_.

  .. todo:: Transcribe the method into this document.

Other files
-----------
- :file:`demand-scale.csv` → ``demand scale:n-y:exo``
- :file:`disutility.csv` → ``disutility:n-cg-t-y:per vehicle``
- :file:`energy-other.csv` → ``energy:c-n:transport other``
- :file:`freight-activity.csv` → ``freight activity:n:ref``
- :file:`freight-mode-share-ref.csv` → ``freight mode share:n-t:ref``
- :file:`fuel-emi-intensity.csv` → ``fuel emi intensity:c-e:exo``
- :file:`ikarus/availability.csv` → ``ikarus availability:source-t-c-y:exo``
- :file:`ikarus/fix_cost.csv` → ``ikarus fix_cost:source-t-c-y:exo``
- :file:`ikarus/input.csv` → ``ikarus input:source-t-c-y:exo``
- :file:`ikarus/inv_cost.csv` → ``ikarus inv_cost:source-t-c-y:exo``
- :file:`ikarus/technical_lifetime.csv` → ``ikarus technical_lifetime:source-t-c-y:exo``
- :file:`ikarus/var_cost.csv` → ``ikarus var_cost:source-t-c-y:exo``
- :file:`input-base.csv` → ``input:t-c-h:base``
- :file:`ldv-class.csv` → ``ldv class:n-vehicle_class:exo``
- :file:`ldv-new-capacity.csv` → ``cap_new:nl-t-yv:ldv+exo``
- :file:`load-factor-ldv.csv` → ``load factor ldv:n:exo``
- :file:`load-factor-nonldv.csv` → ``load factor nonldv:t:exo``
- :file:`ma3t/attitude.csv` → ``ma3t attitude:attitude:exo``
- :file:`ma3t/driver.csv` → ``ma3t driver:census_division-area_type-driver_type:exo``
- :file:`ma3t/population.csv` → ``ma3t population:census_division-area_type:exo``
- :file:`mer-to-ppp.csv` → ``mer to ppp:n-y:exo``
- :file:`population-suburb-share.csv` → ``population suburb share:n-y:exo``

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
