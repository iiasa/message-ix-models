Tools for specific data sources
*******************************

.. _tools-cepii:

“Centre d’études prospectives et d’informations internationales” (CEPII) (:mod:`.tools.cepii`)
==============================================================================================

:func:`.load_energy_import_value` sums the BACI import values of one country over the HS92 headings in :data:`.ENERGY_HS92` to the products of :class:`.UNSD_ENERGY_BALANCE`, so that they can be divided by imported energy from :ref:`tools-energy-balance` to give an import price per product.

.. currentmodule:: message_ix_models.tools.cepii

.. automodule:: message_ix_models.tools.cepii
   :members:

.. _tools-eurostat:

Eurostat (:mod:`.tools.eurostat`)
=================================

Eurostat publishes complete energy balances for EU member states, candidate countries, and some others in the data flow ``NRG_BAL_C``, through an SDMX web service that requires no registration.
Countries not covered are generally covered by :mod:`.tools.unsd`.

:class:`.ESTAT_ENERGY_BALANCE` returns data with the dimensions and units of :class:`.IEA_EWEB`, with Eurostat's own ``siec`` codes for products and ``nrg_bal`` codes for flows.
:class:`.ESTAT_ENERGY_BALANCE_UNSD` returns the same data converted to the product and flow codes and the sign conventions of :class:`.UNSD_ENERGY_BALANCE`, per :data:`.UNSD_RULES`, so that a country can be handled in the same vocabulary whichever service its data come from; see :ref:`tools-energy-balance`.
The conversion is checked against UNSD's own balance for Serbia: natural gas, electricity, and heat agree in every flow, while for coal and oil the two services publish different values from the same national return.
Both are :class:`.SDMXSource` subclasses; see that class for use with :mod:`genno` or :mod:`pandas`.

.. currentmodule:: message_ix_models.tools.eurostat

.. automodule:: message_ix_models.tools.eurostat
   :members:

.. _tools-energy-balance:

Energy balance of any country (:mod:`.tools.energy_balance`)
============================================================

UNSD publishes an energy balance for every country; for those filing the joint Eurostat/IEA/UNECE questionnaire, Eurostat publishes the same returns, usually one year sooner and often from an earlier year.
:func:`.get_source` chooses Eurostat where its series covers at least the periods UNSD's does (:data:`.ESTAT_GEO`) and UNSD otherwise, and :func:`.energy_balance.load_data` returns the balance as a :class:`pandas.DataFrame` in the vocabulary of :class:`.UNSD_ENERGY_BALANCE`, whichever service it comes from.
Code with a :class:`genno.Computer` can call :meth:`.ExoDataSource.add_tasks` on the class returned by :func:`.get_source`.

.. currentmodule:: message_ix_models.tools.energy_balance

.. automodule:: message_ix_models.tools.energy_balance
   :members:

.. _tools-gem:

Global Energy Monitor (GEM) (:mod:`.tools.gem`)
===============================================

The Global Integrated Power Tracker lists power-generating units worldwide with capacity, status, and start and retirement years.
GEM provides it as a workbook on request, under CC BY 4.0, so :func:`.read_units` reads a local copy.
:func:`.gem.technology` maps a unit to a MESSAGEix-GLOBIOM power technology.

.. currentmodule:: message_ix_models.tools.gem

.. automodule:: message_ix_models.tools.gem
   :members:

.. _tools-gfei:

Global Fuel Economy Initiative (GFEI) (:mod:`.tools.gfei`)
==========================================================

.. currentmodule:: message_ix_models.tools.gfei

.. automodule:: message_ix_models.tools.gfei
   :members:

.. _tools-iea:

International Energy Agency (IEA) (:mod:`.tools.iea`)
=====================================================

The IEA publishes many kinds of data.
Each distinct data source is handled by a separate submodule of :mod:`message_ix_models.tools.iea`.

Documentation for all module contents:

.. currentmodule:: message_ix_models.tools

.. autosummary::
   :toctree: _autosummary
   :template: autosummary-module.rst
   :recursive:

   iea

Energy efficiency indicators (:mod:`.tools.iea.eei`)
----------------------------------------------------

See :class:`.IEA_EEI`.
This data is produced by the IEA and retrieved from the Energy Efficiency Indicators database.
It is proprietary.

The data:

- Has the geographic resolution of individual countries, and scope including 41 countries:

 - 24 IEA member countries for which data covering most end-uses area available: Australia, Austria, Belgium, Canada, Czech Republic, Denmark, Finland, France, Germany, Greece, Hungary, Italy, Japan, Korea, Luxembourg, the Netherlands, New Zealand, Poland, Portugal, Slovak Republic, Spain, Switzerland, the United Kingdom and the United States.
 - Others including Brazil, Chile, Lithuania, Morocco, Armenia, Azerbaijan, Belarus, Georgia, Kazakhstan, Kyrgyzstan, Republic of Moldova, Ukraine, Uzbekistan.

- Includes measures/variables for energy consumption, efficiency, carbon emissions, and others for four conceptual sectors: Residential, Services, Industry and Transport.
- The **December 2020 edition** covers the time periods 2000–2018 with annual resolution.

.. note:: Currently, :mod:`.iea.eei` mainly retrieves and processes data useful for MESSAGEix-Transport.
   To retrieve other end-use sectoral data, the code can be extended.

.. _tools-iea-web:

(Extended) World Energy Balances (:mod:`.tools.iea.web`)
--------------------------------------------------------

.. contents::
   :local:
   :backlinks: none

.. note:: These data are **proprietary** and require a paid subscription.

The approach to handling proprietary data is the same as in :mod:`.project.advance` and :mod:`.project.ssp`:

- Copies of the data are stored in the (private) `message-static-data` repository using Git LFS.
  This respository is accessible only to users who have a license for the data.
- :mod:`message_ix_models` contains only a ‘fuzzed’ version of the data (same structure, random values) for testing purposes.
- Non-IIASA users must obtain their own license to access and use the data; obtain the data themselves; and place it on the system where they use :mod:`message_ix_models`.

The module :mod:`message_ix_models.tools.iea.web` attempts to detect and support both the providers/formats described below.
The code supports using data from any of the above locations and formats, in multiple ways:

- Use :class:`.IEA_EWEB` via :func:`.exo_data.prepare_computer` to use the data in :mod:`genno` structured calculations.
- Use :func:`.iea.web.load_data` to load data as :class:`pandas.DataFrame` and apply further processing using pandas.

The **documentation** for the `2023 edition <https://iea.blob.core.windows.net/assets/0acb1453-1221-421b-9131-632ce71a4c1a/WORLDBAL_Documentation.pdf>`__ of the IEA source/format is publicly available.

Structure
~~~~~~~~~

The data have the following conceptual dimensions, each enumerated by a different list of codes:

- ``FLOW``, ``PRODUCT``: for both of these, the lists of codes appearing in the data are the same from 2021 and 2023 inclusive.
- ``COUNTRY``: The data provided by IEA directly contain codes that are all caps, abbreviated country names, for instance 'DOMINICANR'.
  The data provided by the OECD contain ISO 3166-1 alpha-3 codes, for instance 'DOM'.
  In both cases, there are additional labels denoting country groupings; these are defined in the documentation linked above.

  Changes visible in these lists include:

  - 2022 → 2023:

    - New codes: ASEAN, BFA, GREENLAND, MALI, MRT, PSE, TCD.
    - Removed: MASEAN.

  - 2021 → 2022:

    - New codes: GNQ, MDG, MKD, RWA, SWZ, UGA.
    - Removed: EQGUINEA, GREENLAND, MALI, MBURKINAFA, MCHAD, MMADAGASCA, MMAURITANI, MPALESTINE, MRWANDA, MUGANDA, NORTHMACED.

  See the :py:`transform=...` source keyword argument and :meth:`.IEA_EWEB.transform` for different methods of handling this dimension.
- ``TIME``: always a year.
- ``UNIT_MEASURE`` (not labeled): unit of measurement, either 'TJ' or 'ktoe'.

:mod:`message_ix_models` is packaged with SDMX structure data (stored in :file:`message_ix_models/data/sdmx/`) comprising code lists extracted from the raw data for the COUNTRY, FLOW, and PRODUCT dimensions.
These can be used with other package utilities, for instance:

.. code-block:: python

   >>> from message_ix_models.util.sdmx import read

   # Read a code list from file: codes used in the
   # 2022 edition data from the OECD provider
   >>> cl = read("IEA:PRODUCT_OECD(2022)")

   # Show some of its elements
   >>> print("\n".join(sorted(cl.items[:5])))
   ADDITIVE
   ANTCOAL
   AVGAS
   BIODIESEL
   BIOGASES

The documentation linked above has full descriptions of each code.

IEA provider/format
~~~~~~~~~~~~~~~~~~~

From 2023 (or earlier), the data are provided directly on the IEA website at https://www.iea.org/data-and-statistics/data-product/world-energy-balances.
These data are available in two formats; ‘IVT’ or “Beyond 20/20” format (not supported by this module) or fixed-width text files.
The latter are characterized by:

- Multiple ZIP archives with names like :file:`WBIG[12].zip`, each containing a portion of the data and typically 110–130 MiB compressed size
- …each containing a single, fixed-with TXT file with a name like :file:`WORLDBIG[12].TXT`, typically 3–4 GiB uncompressed,
- …with no column headers, but data resembling::

    WORLD  HARDCOAL  1960  INDPROD  KTOE ..

  …that appear to correspond to, respectively, the COUNTRY, PRODUCT, TIME, FLOW, and MEASURE dimensions and "Value" column of the above data, respectively.

OECD provider/format
~~~~~~~~~~~~~~~~~~~~

Up until 2023, the EWEB data were available from the OECD iLibrary with DOI `10.1787/enestats-data-en <https://doi.org/10.1787/enestats-data-en>`__.
These files were characterized by:

- Single ZIP archives with names like :file:`cac5fa90-en.zip`; typically ~850 MiB compressed size,
- …containing a single CSV file with a name like :file:`WBIG_2022-2022-1-EN-20230406T100006.csv`, typically >20 GiB uncompressed,
- …with a particular list of columns like: "MEASURE", "Unit", "COUNTRY", "Country", "PRODUCT", "Product", "FLOW", "Flow", "TIME", "Time", "Value", "Flag Codes", "Flags",
- …with contents that duplicated code IDs—for instance, in the "FLOW" column—with human-readable labels—for instance in the "Flow" column:

   ============ ===
   Column name  Example value
   ============ ===
   MEASURE [1]_                   KTOE
   Unit                           ktoe
   COUNTRY                         WLD
   Country                       World
   PRODUCT                        COAL
   Product      Coal and coal products
   FLOW                        INDPROD
   Flow                     Production
   TIME                           2012
   Time                           2012
   Value                     1234.5678
   Flag Codes                        M
   Flags        Missing value; data cannot exist
   ============ ===

   .. [1] the column is sometimes labelled "UNIT", but the contents appear to be the same.

This source is discontinued and will not publish subsequent editions of the data.

.. _tools-newclimate:

NewClimate Institute (:mod:`.tools.newclimate`)
===============================================

.. automodule:: message_ix_models.tools.newclimate
   :members:

.. _tools-unsd:

UN Statistics Division (UNSD) (:mod:`.tools.unsd`)
==================================================

UNSD publishes energy statistics for all countries, including those not covered by Eurostat or the IEA, through an SDMX web service (https://unstats.un.org/unsd/energystats/) that requires no registration.
Two data flows are supported:

- ``DF_UNData_EnergyBalance``, via :class:`.UNSD_ENERGY_BALANCE`: energy balances with 9 fuel groups on the ``product`` dimension.
  These are too coarse for uses that distinguish individual products, as :class:`.IEA_EWEB` does with about 70.
- ``DF_UNDATA_ENERGY``, via :class:`.UNSD_ENERGY`: individual commodities in physical units, converted to TJ using the conversion factor attached to each observation.
  Natural gas is converted from gross to net calorific value.

The data have the dimensions and units of :class:`.IEA_EWEB`, with UNSD's own codes for products and flows.
Both are :class:`.SDMXSource` subclasses; see that class for use with :mod:`genno` or :mod:`pandas`.

.. currentmodule:: message_ix_models.tools.unsd

.. automodule:: message_ix_models.tools.unsd
   :members:
