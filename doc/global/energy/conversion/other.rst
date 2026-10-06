.. _other:

Other conversion
================
Beyond electricity and centralized heat generation there are four further subsectors of the conversion sector represented in |MESSAGEix|,
liquid fuel production, gaseous fuel production, hydrogen production and ammonia synthesis.
Liquefaction, gasification and hydrogen and ammonia production,
in addition to refineries,
provide a variety of fuel options for use across the energy system,
several of them central to the transition to lower-carbon energy systems.
Coal and biomass share a gasification step and mainly differ in feedstock,
while gas routes rely on steam methane reforming and electricity routes on electrolysis.
Where the conversion process creates a CO2 stream, CCS variants exist,
which in combination with biomass feedstocks provide net-negative emissions.
Investment costs for all conversion technologies follow the approach described in :ref:`electricity`,
with base-year costs for the North America reference region,
a narrative-specific cost decline towards 2150
and regional cost ratios that converge with economic development.
:numref:`fig-costind` and :numref:`fig-costind-h2` show the resulting investment cost ranges
for synthetic liquid and gaseous fuels and for hydrogen and ammonia
(Fricko et al., 2026 :cite:`fricko_wu_2026`).

Liquid Fuel Production
----------------------

Apart from oil refining as the predominant supply technology for liquid fuels at present,
a number of alternative liquid fuel production routes from different feedstocks are represented in |MESSAGEix|
(see :numref:`tab-liqfuel`).
Refining is represented at the level of individual process units,
from atmospheric and vacuum distillation to hydrotreating, catalytic and hydrocracking, visbreaking, coking and catalytic reforming,
as part of the petrochemical sector representation of
:doc:`MESSAGEix-Materials </conceptual/materials/index>`.
Synthetic liquids, diesel and light oils, act as direct replacements in the transport and industry sectors
and are produced from gasified coal via Fischer-Tropsch synthesis.
Their ease of use, higher energy density and lower sulfur content come at a price premium over methanol,
a liquid fuel and chemical feedstock produced from coal, gas or biomass,
or from hydrogen combined with CO2 (electro-methanol),
which is cheaper to make but requires further processing, or adapted engines,
before it serves as a transport fuel.
Biomass gasification can also be used to derive ethanol,
through either a gasification or a Fischer-Tropsch route.
Several of these technologies include co-generation of electricity,
for example by burning unconverted syngas from a Fischer-Tropsch synthesis in a gas turbine
(c.f. Larson et al., 2012 :cite:`larson_chapter_2012`).

.. _tab-liqfuel:
.. table:: Liquid fuel production technologies in |MESSAGEix| by energy source.

   +----------------+------------------------------------------------------+---------------------------+
   | Energy source  | Technology                                           | Electricity cogeneration  |
   +================+======================================================+===========================+
   | biomass        | Fischer-Tropsch biomass-to-liquids                   | yes                       |
   |                +------------------------------------------------------+---------------------------+
   |                | Fischer-Tropsch biomass-to-liquids with CCS          | yes                       |
   |                +------------------------------------------------------+---------------------------+
   |                | ethanol via biomass gasification                     | yes                       |
   |                +------------------------------------------------------+---------------------------+
   |                | ethanol via biomass gasification with CCS            | no                        |
   |                +------------------------------------------------------+---------------------------+
   |                | methanol from biomass gasification                   | yes                       |
   |                +------------------------------------------------------+---------------------------+
   |                | methanol from biomass gasification with CCS          | no                        |
   +----------------+------------------------------------------------------+---------------------------+
   | coal           | Fischer-Tropsch coal-to-liquids                      | yes                       |
   |                +------------------------------------------------------+---------------------------+
   |                | Fischer-Tropsch coal-to-liquids with CCS             | yes                       |
   |                +------------------------------------------------------+---------------------------+
   |                | methanol from coal gasification                      | yes                       |
   |                +------------------------------------------------------+---------------------------+
   |                | methanol from coal gasification with CCS             | yes                       |
   +----------------+------------------------------------------------------+---------------------------+
   | gas            | methanol from natural gas                            | no                        |
   |                +------------------------------------------------------+---------------------------+
   |                | methanol from natural gas with CCS                   | no                        |
   +----------------+------------------------------------------------------+---------------------------+
   | hydrogen       | methanol from hydrogen and CO2 (electro-methanol)    | no                        |
   +----------------+------------------------------------------------------+---------------------------+
   | oil            | refinery, represented at the level of process units  | no                        |
   +----------------+------------------------------------------------------+---------------------------+

:numref:`fig-costind` (a) shows the investment cost developments of these routes across the narratives,
and :numref:`fig-costind` (b) those of their CCS variants.
Solid-feedstock gasification is more capital intensive than reforming and electro-methanol,
although the low cost of electro-methanol is conditional on cheap electricity and hydrogen.
In the sustainability narratives, LED and SSP1, the coal-based process costs remain highest,
as does bio-methanol, which is parametrised on the same process as coal and sees no further cost improvement.
These costs fall furthest in SSP3 and SSP4, with SSP2 and SSP5 in between.
Bio-ethanol and gas-to-methanol follow the opposite pattern,
improving the most in SSP1, and in SSP4 for bio-ethanol and SSP5 for gas-to-methanol,
and least in SSP3.
In all cases the wide present-day spread across regions narrows towards the end of the century
as costs converge to the reference region,
with little reduction foreseen for these already mature technologies,
a pattern also reflected in the CCS counterparts.

.. _fig-costind:
.. figure:: /_static/synthetic_fuel_investment_costs.png
   :width: 700px

   Other conversion investment cost developments, synthetic liquids and gas.
   Investment cost ranges (USD2005/kW) by technology for synthetic liquid fuels
   (Fischer-Tropsch liquids, methanol, ethanol) and synthetic gas production,
   shown for the base year (black, SSP2) and for 2100 by narrative (LED and SSP1 to SSP5).
   Panel (a) shows technologies without CCS and panel (b) those with CCS.
   Each coloured bar spans the range across the twelve regions
   and the red diamond marks the North America value
   (Fricko et al., 2026 :cite:`fricko_wu_2026`).
   FTL stands for Fischer-Tropsch liquids, gasif. for gasification and FT for Fischer-Tropsch.

Gaseous Fuel Production
-----------------------

Gaseous fuel production technologies represented in |MESSAGEix| are the gasification of coal and of biomass
(:numref:`tab-gasfuel`).
The synthetic gas can be blended into the natural gas network
and substitutes directly for existing gas uses
(see :doc:`Fuel Blending <../fuel_blending>`),
as can hydrogen.
Neither gasification route carries a CCS option in |MESSAGEix|.
The investment costs of both routes are among the lowest of the conversion technologies shown in :numref:`fig-costind` (a),
and change little across the narratives.

.. _tab-gasfuel:
.. table:: Gaseous fuel production technologies in |MESSAGEix| by energy source.

   +----------------+--------------------------------+-------------+
   | Energy source  | Technology                     | CCS option  |
   +================+================================+=============+
   | biomass        | biomass gasification           | no          |
   +----------------+--------------------------------+-------------+
   | coal           | coal gasification              | no          |
   +----------------+--------------------------------+-------------+

Hydrogen Production
-------------------

A number of hydrogen production options are represented in |MESSAGEix|.
These include gasification processes for coal and biomass,
steam methane reforming of natural gas
and electrolysis of water.
The fossil fuel and biomass based options can be combined with CCS to reduce carbon emissions.
Hydrogen serves both as an energy carrier and as a feedstock for synthetic fuels and ammonia,
and can be liquefied or blended into the natural gas network.
:numref:`tab-hydtech` provides a full list of hydrogen production technologies.

.. _tab-hydtech:
.. table:: Hydrogen production technologies in |MESSAGEix| by energy source.

   +----------------+-----------------------------------+---------------------------+
   | Energy source  | Technology                        | Electricity cogeneration  |
   +================+===================================+===========================+
   | coal           | coal gasification                 | yes                       |
   |                +-----------------------------------+---------------------------+
   |                | coal gasification with CCS        | yes                       |
   +----------------+-----------------------------------+---------------------------+
   | biomass        | biomass gasification              | yes                       |
   |                +-----------------------------------+---------------------------+
   |                | biomass gasification with CCS     | yes                       |
   +----------------+-----------------------------------+---------------------------+
   | gas            | steam methane reforming           | yes                       |
   |                +-----------------------------------+---------------------------+
   |                | steam methane reforming with CCS  | no                        |
   +----------------+-----------------------------------+---------------------------+
   | electricity    | electrolysis                      | no                        |
   +----------------+-----------------------------------+---------------------------+

Natural gas steam reforming is the cheapest route to hydrogen,
while biomass gasification is the most expensive,
with coal gasification in between (:numref:`fig-costind-h2`).
Electrolysis is a special case, with a cost that is strongly narrative dependent,
falling to around 200 USD2005/kW in LED and SSP1, where it becomes the cheapest source of hydrogen,
but remaining near 1000 USD2005/kW in SSP3.
The LED and SSP1 values are below the 2050 median of the fast transition probabilistic projections of
Way et al. (2022 :cite:`way_forecasts_2022`),
while the SSP3 value maps to the median of the no transition variant from the same source.
The long-term cost developments follow the same feedstock-dependent storyline as liquid fuels.
The coal-based route stays highest in LED and SSP1, where no cost improvement is assumed,
and is lower in the other narratives,
whereas the gas, biomass and electrolytic routes show the mirror image,
improving most in SSP1 and least in SSP3,
with electrolysis the most pronounced case, falling furthest of all in LED and SSP1.

Ammonia Synthesis
-----------------

Ammonia is synthesised by combining nitrogen with hydrogen,
where the hydrogen is sourced from coal, natural gas, biomass, fuel oil or electrolysis
(:numref:`tab-ammonia`).
It is used mainly as fertilizer feedstock,
and is increasingly considered as a future energy carrier and shipping fuel.
The fertilizer use is resolved in the petrochemical sector representation of
:doc:`MESSAGEix-Materials </conceptual/materials/index>`.
Ammonia costs follow a single trend regardless of feedstock,
staying highest in LED and SSP1 and lowest in SSP3, with the other narratives in between,
while electrolytic ammonia is held constant across all narratives (:numref:`fig-costind-h2`).
As with the other conversion technologies,
the wide present-day regional range narrows toward 2100
as costs are assumed to converge toward the reference region.

.. _tab-ammonia:
.. table:: Ammonia synthesis technologies in |MESSAGEix| by energy source.

   +----------------+----------------------------------------+-------------+
   | Energy source  | Technology                             | CCS option  |
   +================+========================================+=============+
   | coal           | ammonia from coal gasification         | yes         |
   +----------------+----------------------------------------+-------------+
   | gas            | ammonia from natural gas reforming     | yes         |
   +----------------+----------------------------------------+-------------+
   | biomass        | ammonia from biomass gasification      | yes         |
   +----------------+----------------------------------------+-------------+
   | oil            | ammonia from fuel oil                  | yes         |
   +----------------+----------------------------------------+-------------+
   | electricity    | ammonia from electrolytic hydrogen     | no          |
   +----------------+----------------------------------------+-------------+

.. _fig-costind-h2:
.. figure:: /_static/hydrogen_ammonia_investment_costs.png
   :width: 700px

   Other conversion investment cost developments, hydrogen and ammonia.
   Investment cost ranges (USD2005/kW) by technology for hydrogen production and ammonia synthesis,
   shown for the base year (black, SSP2) and for 2100 by narrative (LED and SSP1 to SSP5).
   Panel (a) shows technologies without CCS and panel (b) those with CCS.
   Each coloured bar spans the range across the twelve regions
   and the red diamond marks the North America value
   (Fricko et al., 2026 :cite:`fricko_wu_2026`).
