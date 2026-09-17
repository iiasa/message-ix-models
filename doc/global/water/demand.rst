.. _water-demand:

Water Demand
============

Water demand in MESSAGEix-Nexus is represented across four major sectors: energy, municipal, industrial manufacturing, and agriculture (Awais et al., 2024 :cite:`awais_2024_nexus`). Energy sector demand emerges from the technologies the model builds and operates. Municipal, industrial and agricultural demands are exogenous basin-scale trajectories that differ by SSP, reflecting population, income, urbanisation and efficiency assumptions. Competition between all four for limited water is resolved through the optimisation.

Energy Sector Water Demand
---------------------------

The energy sector is the most explicitly represented water demand in MESSAGEix-Nexus, with water requirements emerging from technology-specific intensities rather than exogenous demand trajectories.

Power Plant Cooling
^^^^^^^^^^^^^^^^^^^

Thermal power plants (coal, gas, nuclear, concentrated solar power, geothermal) require cooling to dissipate waste heat. Cooling water is the largest energy sector water demand in most regions. The cooling technology implementation is described in detail in :ref:`water-cooling`.

Water withdrawal and consumption intensities are not exogenous demands but emerge from the technology parameterisation, and vary by:

* **Power plant type**: Different heat rates imply different quantities of waste heat per unit of electricity
* **Cooling technology**: Once-through cooling withdraws large volumes but returns most of the water to the source; recirculating cooling withdraws far less but consumes most of what it withdraws through evaporation; dry cooling nearly eliminates water use at the cost of an efficiency penalty
* **Ambient conditions**: Temperature and humidity affect cooling performance

Intensities are calibrated to the ranges reported in Meldrum et al., 2013 :cite:`meldrum_2013` and Macknick et al., 2012 :cite:`macknick_2012`. The model endogenously chooses cooling technologies based on water availability, costs and performance (see :ref:`water-cooling`).

Fuel Extraction and Processing
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Water is also required for fossil fuel extraction and processing — coal washing and dust suppression, drilling and processing of oil and gas, and steam injection or hydraulic fracturing for unconventional resources. Biofuel production draws water mainly through crop irrigation, which is captured in the agricultural demand rather than here.

These demands are small relative to cooling, but can matter in water-scarce regions with large extractive industries.

Hydropower
^^^^^^^^^^

Hydropower generation is non-consumptive, but it interacts with the water system through:

* **Reservoir evaporation**: Can be significant in arid regions with large reservoirs
* **Flow timing**: Alters seasonal patterns of water availability downstream
* **Environmental flows**: Minimum release requirements constrain generation

Municipal and Industrial Water Demand
--------------------------------------

Municipal demand covers residential, commercial and public sector water use in urban and rural areas. Industrial demand covers manufacturing and mining processes, distinct from the energy sector demands already counted in power generation.

Data Source
^^^^^^^^^^^

Unlike energy sector water use, municipal and industrial demands are **exogenous inputs**, not quantities derived inside the model. Basin-level withdrawal projections are taken from Khan et al. (2022) and supplied per SSP for three sectors:

* Urban domestic withdrawal
* Rural domestic withdrawal
* Manufacturing and mining withdrawal

The projections run from 2010 to 2100 and reflect the socioeconomic drivers of each SSP — population, urbanisation, income growth, and assumed improvements in water use efficiency. Because the data are already SSP-differentiated at the basin level, the model does not re-estimate demand from population and GDP; it takes the trajectory as given and resolves the competition for water that results.

Return Flows
^^^^^^^^^^^^

Municipal and industrial water use returns a substantial share of withdrawals to the system. Return flows are derived by applying a **per-basin return ratio** to the corresponding withdrawal. These ratios are fixed characteristics of the basin and are applied uniformly across all SSPs.

Return flows can be released to rivers, adding to downstream availability; treated and recycled back into supply, subject to the treatment and recycling rates described in :doc:`supply`; or used to meet environmental flow requirements.

SDG6 Water Access Constraints
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The Sustainable Development Goals framework includes targets for universal access to safely managed drinking water (SDG 6.1). Access rates are supplied as exogenous regional projections, with an urban/rural split where the source data allow one.

SDG constraints can be activated in MESSAGEix-Nexus scenarios:

:math:`access_{b,t} \geq access_{target}(t)`

where :math:`access_{target}(t)` is the target access rate trajectory. Raising access rates increases the population served, which raises municipal demand and requires investment in water supply infrastructure — creating additional competition for water with the energy sector, most acutely in the regions where current access rates are lowest.

Agricultural Irrigation Demand
-------------------------------

Agricultural irrigation is the largest water demand globally (~70% of total withdrawals) and exhibits strong seasonal variability. In MESSAGEix-Nexus, irrigation demand is derived from the GLOBIOM land-use model linkage.

GLOBIOM Linkage
^^^^^^^^^^^^^^^

Irrigation water demand is calculated in GLOBIOM based on:

* **Crop area**: Irrigated area for each crop type
* **Crop water requirements**: Climate-dependent evapotranspiration
* **Irrigation efficiency**: Technology-dependent water delivery and application efficiency
* **Rainfall**: Effective precipitation reduces irrigation needs

GLOBIOM provides basin-scale irrigation demand to MESSAGEix-Nexus, which must be satisfied by available water resources. Water scarcity in MESSAGEix-Nexus can feed back to GLOBIOM by:

* Increasing irrigation costs (water pricing)
* Constraining irrigated area expansion
* Incentivizing efficiency improvements

Seasonal Patterns
^^^^^^^^^^^^^^^^^

Irrigation demand varies seasonally based on:

* **Crop calendars**: Planting and growing season timing
* **Evapotranspiration**: Peak during warm, dry periods
* **Monsoon patterns**: Low irrigation during rainy seasons

Example monthly demand pattern (Northern India):

* **January-March**: High (wheat, vegetables)
* **April-June**: Very high (summer crops, pre-monsoon)
* **July-September**: Low (monsoon period)
* **October-December**: Moderate (post-monsoon crops)

Seasonal variability creates critical periods when irrigation competes strongly with other demands and water availability is lowest (Awais et al., 2024 :cite:`awais_2024_nexus`).

Irrigation Technologies
^^^^^^^^^^^^^^^^^^^^^^^

Irrigation efficiency depends on the delivery and application technology, rising from flood and furrow irrigation through sprinkler systems to drip and micro-irrigation. Higher efficiency technologies cost more per hectare but reduce the water required for the same crop production, and can enable irrigated area to expand in water-constrained basins.

Climate Change Impacts
^^^^^^^^^^^^^^^^^^^^^^

Climate change affects irrigation demand through:

* **Evapotranspiration changes**: Generally increases with temperature
* **Precipitation changes**: Regional increases or decreases affect irrigation needs
* **Crop calendar shifts**: Earlier springs, longer growing seasons
* **CO₂ fertilization**: Higher CO₂ can reduce crop water requirements

In most regions, climate change increases net irrigation demand despite CO₂ effects (Awais et al., 2024 :cite:`awais_2024_nexus`).

Sectoral Competition and Allocation
------------------------------------

When water is scarce, the model allocates it across competing sectors as part of the same least-cost optimisation that solves the energy system. There is no separate allocation rule and no exogenous ranking of sectors by economic value: water goes to the use where displacing it would be most expensive for the system as a whole, given the alternatives available in that basin.

What differs between sectors is how costly it is to go without, and how quickly they can adjust.

Infrastructure and Flexibility
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Existing infrastructure creates rigidities:

* Power plants require cooling or must reduce generation
* Urban populations require minimum municipal supply
* Agricultural demands are flexible (can fallow fields, deficit irrigate)

The model accounts for costs of:

* Not meeting demand (scarcity costs, value of lost load for electricity)
* Adjusting to constraints (switching technologies, deficit irrigation)

Temporal Flexibility
^^^^^^^^^^^^^^^^^^^^

Some demands are temporally flexible:

* **Irrigation**: Can shift timing within crop growth period
* **Industrial**: Some processes can shift to wet season
* **Energy**: Flexible generation can be scheduled to water availability
* **Municipal**: Relatively inflexible, requires continuous supply

Storage (reservoirs, aquifer storage) provides temporal flexibility to match seasonal supply and demand.

Regional Differences
^^^^^^^^^^^^^^^^^^^^

Water scarcity and sectoral competition vary greatly by region:

* **Arid regions** (Middle East, North Africa, Central Asia): Scarcity is norm, high competition
* **Monsoon regions** (South Asia, Southeast Asia): Seasonal scarcity, competition in dry season
* **Temperate regions** (Europe, North America): Generally abundant, localized scarcity
* **Tropical regions** (Sub-Saharan Africa, Latin America): Variable, infrastructure-limited

Scenarios with stringent climate change and rapid development can increase water scarcity and sectoral competition significantly (Awais et al., 2024 :cite:`awais_2024_nexus`).

Demand Projections
------------------

Future water demand depends on scenario assumptions:

Shared Socioeconomic Pathways (SSPs)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Different SSPs imply different demand trajectories:

* **SSP1 (Sustainability)**: 

  * Lower population growth
  * High efficiency and water productivity
  * Strong environmental regulations
  * Lowest demand growth

* **SSP2 (Middle-of-the-road)**:

  * Medium population and economic growth
  * Moderate efficiency improvements
  * Continued irrigation expansion
  * Medium demand growth

* **SSP3 (Regional rivalry)**:

  * High population growth in developing regions
  * Slow efficiency improvements
  * Irrigation expansion constrained by water scarcity
  * Highest demand growth but supply-limited

* **SSP5 (Fossil-fueled development)**:

  * Rapid economic growth and urbanization
  * High energy demands = high cooling water demand
  * Efficient water use in high-income regions
  * High total demand but technology-enabled supply

Climate Change Impacts
^^^^^^^^^^^^^^^^^^^^^^

Climate change affects demands through:

* **Temperature**: Higher cooling demands (energy, buildings)
* **Precipitation**: Changed irrigation requirements
* **Extremes**: Droughts increase marginal value of water

Demands are read for the SSP of the run, and water availability for the selected climate forcing scenario, so a scenario combines both drivers. The available forcing scenarios are listed in :doc:`climate_impacts`.

.. footbibliography::

