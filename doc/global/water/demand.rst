.. _water-demand:

Water Demand
============

Water demand in MESSAGEix-Nexus is represented across four major sectors: energy, municipal, industrial manufacturing, and agriculture (Awais et al., 2024 :cite:`awais_2024_nexus`). Demands are specified at the basin scale and evolve over time based on socioeconomic drivers (population, GDP, urbanization) and technological change. Competition between sectors for limited water resources is explicitly resolved through the optimization.

Energy Sector Water Demand
---------------------------

The energy sector is the most explicitly represented water demand in MESSAGEix-Nexus, with water requirements emerging from technology-specific intensities rather than exogenous demand trajectories.

Power Plant Cooling
^^^^^^^^^^^^^^^^^^^

Thermal power plants (coal, gas, nuclear, concentrated solar power, geothermal) require cooling to dissipate waste heat. Cooling water requirements are the largest energy sector water demand in most regions. The cooling technology implementation is described in detail in :ref:`water-cooling`.

Water withdrawal and consumption intensities vary by:

* **Power plant type**: Different heat rates and cooling requirements
* **Cooling technology**: Once-through, recirculating (wet tower), dry cooling
* **Climate conditions**: Ambient temperature affects cooling requirements

Typical water intensities (Meldrum et al., 2013 :cite:`meldrum_2013`):

.. list-table:: Power plant cooling water intensities
   :widths: 30 25 25
   :header-rows: 1

   * - Technology
     - Withdrawal (m³/MWh)
     - Consumption (m³/MWh)
   * - Coal - once-through
     - 100-150
     - 1-2
   * - Coal - recirculating
     - 2-3
     - 2-3
   * - Coal - dry cooling
     - 0.05-0.10
     - 0.05-0.10
   * - Gas combined cycle - once-through
     - 40-80
     - 0.5-1
   * - Gas combined cycle - recirculating
     - 0.5-1.5
     - 0.5-1.5
   * - Nuclear - once-through
     - 100-200
     - 1.5-2.5
   * - Nuclear - recirculating
     - 2.5-4
     - 2.5-4
   * - Concentrated solar power - recirculating
     - 2.5-3.5
     - 2.5-3.5

Once-through cooling withdraws large volumes but returns most water to the source (albeit warmer). Recirculating cooling withdraws less but consumes most of what is withdrawn through evaporation. Dry cooling eliminates water use but has efficiency penalties and higher capital costs.

The model endogenously chooses cooling technologies based on water availability, costs, and performance impacts (see :ref:`water-cooling`).

Fuel Extraction and Processing
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Water is required for fossil fuel extraction and processing:

* **Coal mining**: 0.05-0.30 m³/GJ (washing, dust suppression)
* **Conventional oil and gas**: 0.02-0.10 m³/GJ (drilling, processing)
* **Unconventional oil (oil sands, shale)**: 0.50-2.00 m³/GJ (steam injection, hydraulic fracturing)
* **Biofuel production**: 1-5 m³/GJ (crop irrigation, processing) - mainly captured through agricultural demand

These demands are relatively small compared to cooling but can be significant in water-scarce regions with large extractive industries.

Hydropower
^^^^^^^^^^

Hydropower generation does not consume water (it is non-consumptive) but affects water availability through:

* **Reservoir evaporation**: Can be significant in arid regions with large reservoirs
* **Flow timing**: Alters seasonal patterns of water availability downstream  
* **Environmental flows**: Minimum release requirements affect energy generation

Reservoir evaporation is calculated based on:

:math:`Evap = A_{reservoir} \cdot E_{rate} \cdot f_{exposure}`

where :math:`A_{reservoir}` is surface area, :math:`E_{rate}` is evaporation rate (mm/year, climate-dependent), and :math:`f_{exposure}` is the fraction of time the reservoir is full.

Typical evaporation from reservoirs ranges from 1-3 m/year in temperate climates to 2-4 m/year in arid regions.

Municipal Water Demand
----------------------

Municipal water demand includes residential, commercial, and public sector water use in urban and rural areas.

Demand Drivers
^^^^^^^^^^^^^^

Municipal water demand is driven by:

* **Population**: Total population in each basin/region
* **Urbanization rate**: Urban populations have higher per-capita demand
* **Income level**: Water use increases with GDP per capita (up to saturation)
* **Water access rates**: Connection to piped water systems
* **Water use efficiency**: Technological change and policy-driven improvements

Demand Estimation
^^^^^^^^^^^^^^^^^

Municipal water demand is projected using a regression-based approach:

:math:`D_{municipal,b,t} = Pop_{b,t} \cdot \left( f_{urban,b,t} \cdot d_{urban}(GDP_{pc,t}) + (1-f_{urban,b,t}) \cdot d_{rural}(GDP_{pc,t}) \right) \cdot access_{b,t}`

where:

* :math:`D_{municipal,b,t}` is municipal demand in basin :math:`b`, time :math:`t`
* :math:`Pop_{b,t}` is population
* :math:`f_{urban,b,t}` is urbanization rate
* :math:`d_{urban}`, :math:`d_{rural}` are per-capita demand functions of GDP per capita
* :math:`access_{b,t}` is the fraction of population with access to improved water supply

Per-Capita Demand Patterns
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Per-capita municipal water demand follows an income-dependent pattern:

* **Low income** (<5,000 USD/capita/year): 20-50 liters/capita/day
* **Middle income** (5,000-20,000 USD/capita/year): 100-200 liters/capita/day  
* **High income** (>20,000 USD/capita/year): 150-300 liters/capita/day (saturates)

The relationship is typically modeled as a logarithmic or logistic function that saturates at high income levels. Urban demand is typically 2-3 times rural demand at similar income levels due to:

* Access to piped water systems
* Water-using appliances
* Commercial and public sector demands
* Landscape irrigation

Regional variations exist based on climate (outdoor water use), culture, and water pricing.

SDG6 Water Access Constraints
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The Sustainable Development Goals framework includes targets for universal water access (SDG 6.1):

* **Universal access** to safely managed drinking water by 2030
* Requires infrastructure investment proportional to unserved population
* Creates minimum demand for municipal water infrastructure

SDG constraints can be activated in MESSAGEix-Nexus scenarios:

:math:`access_{b,t} \geq access_{target}(t)`

where :math:`access_{target}(t)` is the target access rate trajectory (e.g., reaching 100% by 2030).

Achieving universal access requires substantial investment in water supply infrastructure, particularly in sub-Saharan Africa and South Asia where current access rates are 50-70% (Awais et al., 2024 :cite:`awais_2024_nexus`).

Return Flows
^^^^^^^^^^^^

Municipal water use has significant return flows:

* **Wastewater return rate**: 70-90% of withdrawals return as wastewater
* **Treatment level**: Determines usability for reuse or environmental release
* **Timing**: Return flows available in same period as withdrawal (no storage)

Return flows can be:

* Released to rivers (adding to downstream availability)
* Treated and reused locally
* Used for environmental flows

Industrial Manufacturing Demand
--------------------------------

Industrial water demand includes manufacturing processes, cooling, and product incorporation. It is distinct from energy sector industrial demands (already counted in power generation).

Demand Drivers
^^^^^^^^^^^^^^

Industrial water demand is driven by:

* **Manufacturing output**: GDP from industrial sector
* **Industrial structure**: Heavy vs. light industry have different water intensities
* **Technology and efficiency**: Water recycling and process improvements
* **Water pricing**: Higher prices incentivize efficiency

Demand Estimation
^^^^^^^^^^^^^^^^^

Industrial demand is estimated using a water intensity approach:

:math:`D_{industrial,b,t} = GDP_{ind,b,t} \cdot I_{water}(t)`

where:

* :math:`GDP_{ind,b,t}` is industrial GDP in basin :math:`b`, time :math:`t`
* :math:`I_{water}(t)` is water intensity (m³ per USD of industrial output)

Water intensity typically declines over time due to:

* **Technological improvement**: More efficient processes and water recycling
* **Structural change**: Shift from heavy to light industry
* **Regulations**: Water use restrictions and pricing

Historical trends show water intensity declining at 1-2% per year in developed economies.

Sectoral Water Intensities
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Different industrial sectors have very different water requirements:

* **Chemicals and petrochemicals**: 10-50 m³/1000 USD
* **Paper and pulp**: 50-300 m³/1000 USD
* **Steel and metals**: 20-100 m³/1000 USD
* **Food and beverages**: 10-50 m³/1000 USD
* **Textiles**: 50-200 m³/1000 USD
* **Electronics**: 5-20 m³/1000 USD

Aggregate industrial water intensity depends on the sectoral composition of manufacturing in each region.

Return Flows and Recycling
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Industrial water use has variable return flows:

* **High-recycling industries** (steel, chemicals): 50-90% return rate
* **Low-recycling industries** (food, textiles): 20-40% return rate
* **Product incorporation** (beverages): 5-10% consumed in products

Industrial wastewater may require treatment before reuse or environmental release, depending on:

* Pollutant loads (organic, inorganic, thermal)
* Discharge regulations
* Reuse opportunities

Industrial demand is relatively stable seasonally compared to agricultural demand.

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

Irrigation efficiency depends on technology:

* **Flood/furrow irrigation**: 40-60% efficiency (large conveyance and field losses)
* **Sprinkler irrigation**: 60-75% efficiency
* **Drip/micro irrigation**: 75-90% efficiency

Efficiency improvements reduce demand for the same crop production:

:math:`D_{irrigation} = \dfrac{CWR \cdot Area}{Eff_{irrigation}}`

where :math:`CWR` is crop water requirement, :math:`Area` is irrigated area, and :math:`Eff_{irrigation}` is irrigation efficiency.

Higher efficiency technologies have higher capital costs but reduce water demand and can enable expansion of irrigated area in water-constrained regions.

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

When water is scarce (demand exceeds availability), the model must allocate water across competing sectors. Allocation is determined by:

Economic Value
^^^^^^^^^^^^^^

Sectors with higher economic value per unit water receive priority:

* **Industrial/municipal**: High value (1-10 USD/m³)
* **Energy (cooling)**: Medium-high value (0.50-5 USD/m³)
* **Irrigation**: Variable value (0.01-1 USD/m³ depending on crop and productivity)

The model balances marginal values across sectors to maximize total economic welfare.

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

Combined SSP-RCP scenarios (SSP2-4.5, SSP5-8.5, etc.) capture both socioeconomic and climate drivers (Awais et al., 2024 :cite:`awais_2024_nexus`).

Global Demand Outlook
^^^^^^^^^^^^^^^^^^^^^^

Baseline global water demand projections (2020-2100):

* **Municipal**: 50-150% increase (driven by population and urbanization)
* **Industrial**: 100-300% increase (driven by economic growth)
* **Irrigation**: 10-70% increase (limited by water availability and efficiency)
* **Energy**: 50-200% increase (depends on generation mix and cooling choices)

Regional patterns vary substantially, with largest growth in South Asia, Middle East, and Sub-Saharan Africa.

.. footbibliography::

