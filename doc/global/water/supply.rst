.. _water-supply:

Water Supply
============

Water supply in MESSAGEix-Nexus is represented through multiple technology options that extract, treat, and distribute freshwater from surface and groundwater sources, as well as non-conventional sources such as desalination and treated wastewater reuse (Awais et al., 2024 :cite:`awais_2024_nexus`). Each basin has specific renewable water availability derived from hydrological model outputs, which constrains total water extraction.

Surface Water
-------------

Surface water resources include runoff from precipitation, snowmelt, and glacier melt aggregated at the river basin scale. Surface water availability is represented as a time-varying resource potential for each basin.

Hydrological Data Sources
^^^^^^^^^^^^^^^^^^^^^^^^^^

Basin-scale surface water availability is derived from global hydrological models that simulate the terrestrial water cycle:

* **PCR-GLOBWB 2** (Sutanudjaja et al., 2018 :cite:`sutanudjaja_2018_pcrglobwb`): A global hydrological model at 5 arcmin resolution (~10km at equator) that simulates river discharge, soil moisture, and groundwater recharge
* **CWatM** (Community Water Model; Burek et al., 2020 :cite:`burek_2020_cwatm`): A spatially distributed hydrological model representing water demand, supply, and environmental flows
* **Historical data** (1971-2000): Used for calibration and baseline water availability
* **Future projections** (2020-2100): Derived from hydrological models forced by climate model outputs from CMIP5/CMIP6

The hydrological model outputs provide monthly or seasonal water availability data that are spatially aggregated from grid cells to MESSAGE basins using area-weighted averages. For MESSAGE applications, seasonal or 5-yearly average values are typically used (Awais et al., 2024 :cite:`awais_2024_nexus`).

Temporal Variability
^^^^^^^^^^^^^^^^^^^^

Surface water availability exhibits strong seasonal and interannual variability:

* **Seasonal patterns**: Monsoon regions show pronounced wet/dry seasons; snow-dominated basins have spring snowmelt peaks
* **Interannual variability**: Represented through statistical analysis of multi-year hydrological simulations
* **Climate trends**: Long-term changes in mean availability and variability under different climate scenarios
* **Extreme events**: Droughts represented as low quantiles (e.g., 10th percentile) of flow distributions

For sub-annual MESSAGE implementations, seasonal water availability is explicitly represented. For annual implementations, average annual availability is used with optional constraints on reliability (e.g., water available in 90% of years).

Environmental Flow Requirements
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Not all renewable surface water can be extracted for human use. Environmental flow requirements (EFRs) are subtracted from gross water availability to determine the extractable potential:

:math:`SW_{extract,b,t} \leq SW_{available,b,t} - EFR_{b,t}`

where :math:`SW_{extract,b,t}` is extractable surface water in basin :math:`b` and time period :math:`t`, :math:`SW_{available,b,t}` is total renewable surface water, and :math:`EFR_{b,t}` is the environmental flow requirement.

Environmental flows are calculated using the Variable Monthly Flow (VMF) method (Pastor et al., 2014 :cite:`pastor_2014_efr`), which sets minimum flows as a percentage of mean monthly natural flow, with higher percentages for low-flow months to protect aquatic ecosystems. Typical EFR values range from 20-40% of mean annual flow depending on the basin and flow regime.

Surface Water Extraction Technologies
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Surface water extraction is represented through technology archetypes with associated costs and infrastructure requirements:

* **River/lake extraction**: Direct abstraction with intake structures, screening, and pumping
* **Small-scale reservoirs**: Storage for seasonal regulation and reliability
* **Large-scale reservoir storage**: Represented through hydropower technologies in MESSAGE
* **Inter-basin transfers**: Explicit connections between basins where infrastructure exists

Extraction costs include:

* Capital costs for intake structures, pumps, and basic treatment
* Operating costs for energy (pumping), maintenance, and operation
* Conveyance costs proportional to distance from source to demand location
* Treatment costs to achieve required water quality

Typical costs range from 0.01-0.05 USD/m³ for surface water extraction and basic treatment (Awais et al., 2024 :cite:`awais_2024_nexus`).

Groundwater
-----------

Groundwater provides a critical buffer against surface water variability and is explicitly represented in MESSAGEix-Nexus with depth-dependent extraction costs and sustainability constraints.

Groundwater Resources
^^^^^^^^^^^^^^^^^^^^^^

Groundwater resources are characterized by:

* **Renewable groundwater**: Annual recharge from precipitation infiltration and river seepage
* **Non-renewable (fossil) groundwater**: Deep aquifers with negligible recharge on human timescales
* **Groundwater storage**: Cumulative volume in aquifers (not fully represented in current implementation)

Renewable groundwater recharge is derived from the same hydrological models as surface water (PCR-GLOBWB, CWatM), which simulate infiltration, percolation, and recharge processes. Basin-scale recharge rates are typically 10-30% of precipitation in humid regions and <5% in arid regions.

Groundwater Extraction
^^^^^^^^^^^^^^^^^^^^^^

Groundwater extraction costs depend on:

1. **Aquifer depth**: Pumping costs increase with depth (energy requirements)
2. **Extraction rate**: Higher rates require more/deeper wells
3. **Water quality**: Treatment requirements for brackish or contaminated groundwater

The extraction cost function is represented as:

:math:`Cost_{GW} = c_0 + c_1 \cdot d + c_2 \cdot d^2`

where :math:`d` is the effective extraction depth and :math:`c_0`, :math:`c_1`, :math:`c_2` are cost parameters. Depths range from shallow (<50m) to deep (>500m) groundwater.

Energy requirements for groundwater pumping create a water-energy feedback loop:

:math:`E_{pump} = \dfrac{\rho \cdot g \cdot d \cdot V}{\eta}`

where :math:`E_{pump}` is pumping energy, :math:`\rho` is water density, :math:`g` is gravitational acceleration, :math:`d` is depth, :math:`V` is volume pumped, and :math:`\eta` is pump efficiency (~0.6-0.8).

Typical groundwater extraction costs range from 0.02 USD/m³ for shallow groundwater to 0.30 USD/m³ for deep groundwater (Awais et al., 2024 :cite:`awais_2024_nexus`), plus energy costs for pumping.

Groundwater Sustainability Constraints
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Groundwater extraction is constrained to sustainable levels to prevent aquifer depletion:

:math:`\sum_{t'=t_0}^{t} GW_{extract,b,t'} \leq \sum_{t'=t_0}^{t} GW_{recharge,b,t'} + GW_{buffer,b}`

This ensures that cumulative extraction does not exceed cumulative recharge plus an allowable buffer representing accessible storage. This constraint prevents the model from mining groundwater unsustainably, which is a major concern in regions such as:

* Northwest India and Pakistan (Indus-Ganges basin)
* North China Plain
* Arabian Peninsula
* High Plains Aquifer (USA)
* Mexico City basin

Aquifer Storage and Recovery
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In some basins, managed aquifer recharge (MAR) technologies are represented:

* **Excess surface water** during wet periods can be used to recharge aquifers
* **Stored water** can be extracted during dry periods or drought
* Provides a form of inter-seasonal and inter-annual water storage

This technology is particularly valuable in basins with strong seasonal variability and available aquifer storage capacity.

Desalination
------------

Desalination technologies convert saline water (seawater or brackish groundwater) into freshwater, providing a climate-independent water source for coastal regions. Desalination is critical for water-scarce regions and is explicitly represented in MESSAGEix-Nexus (Awais et al., 2024 :cite:`awais_2024_nexus`).

Desalination Technologies
^^^^^^^^^^^^^^^^^^^^^^^^^^

Two main desalination technology categories are represented:

**Reverse Osmosis (RO)**: Membrane-based separation

* Lower energy consumption: 3-4 kWh/m³ for seawater, 1-2 kWh/m³ for brackish water
* Requires electrical energy (high-quality energy)
* Modular and scalable
* Suitable for small to large plants
* Current technology of choice for new capacity

**Thermal Desalination**: Evaporation-based processes (MSF, MED)

* Higher energy consumption: 15-25 kWh/m³ thermal energy equivalent
* Can use waste heat from power plants (cogeneration)
* Historically dominant, now mostly in Middle East
* Often coupled with thermal power generation

The technology choice depends on:

* Availability of waste heat from power generation
* Cost of electricity vs. thermal energy
* Plant size and water demand patterns
* Feedwater salinity and quality

Energy Requirements and Costs
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Desalination is energy-intensive, creating a water-energy nexus feedback:

* **RO energy**: 3-4 kWh_e/m³ for seawater (~0.50-0.70 USD/m³ at typical electricity prices)
* **Thermal desalination**: 50-80 MJ_th/m³ heat (~0.30-0.50 USD/m³ with waste heat)
* **Additional costs**: Chemicals, membranes, maintenance, brine disposal

Total levelized costs for desalinated water:

* Seawater RO: 0.50-1.50 USD/m³ (decreasing with technology improvements)
* Brackish RO: 0.30-0.80 USD/m³ (lower salinity = lower costs)
* Thermal desalination: 1.00-2.50 USD/m³ (decreasing with scale)

Costs have declined significantly (>50% reduction since 2000) due to:

* Improved membrane technology and energy recovery devices
* Economies of scale in large plants
* Operational experience and optimization

Regional Availability
^^^^^^^^^^^^^^^^^^^^^

Desalination is only available in basins with access to:

* **Coastal regions**: Seawater desalination
* **Inland brackish groundwater**: Brackish water desalination

The model includes geographical constraints limiting desalination to appropriate basins. Transport costs increase with distance from coast to demand centers.

Current and Projected Capacity
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Historical desalination capacity (base year ~2020):

* Global total: ~100 million m³/day
* Middle East and North Africa: ~70% of global capacity
* Growing rapidly in water-scarce regions

Projected capacity expansion is endogenous in MESSAGEix-Nexus based on:

* Water scarcity and availability of alternatives
* Energy costs and availability
* Economic development and water demands
* Climate change impacts on conventional water sources

In water-stressed scenarios, desalination can grow to provide 10-20% of urban water supply in coastal MESSAGE regions by 2050-2100 (Awais et al., 2024 :cite:`awais_2024_nexus`).

Wastewater Treatment and Reuse
-------------------------------

Treated wastewater provides an additional water source, particularly for non-potable uses such as industrial cooling, irrigation, and environmental flows.

Treatment Technologies
^^^^^^^^^^^^^^^^^^^^^^

Multiple treatment levels are represented:

* **Primary treatment**: Solids removal (~30% pollutant removal)
* **Secondary treatment**: Biological treatment (~85% pollutant removal)
* **Tertiary treatment**: Advanced treatment for reuse (~95% pollutant removal)

Energy and cost requirements increase with treatment level:

* Primary: 0.1-0.2 kWh/m³, 0.02-0.05 USD/m³
* Secondary: 0.3-0.6 kWh/m³, 0.10-0.20 USD/m³
* Tertiary: 0.5-1.0 kWh/m³, 0.30-0.60 USD/m³

Reuse Applications
^^^^^^^^^^^^^^^^^^

Treated wastewater can be used for:

* **Industrial cooling**: Requires secondary treatment
* **Agricultural irrigation**: Requires secondary or tertiary treatment depending on crop type
* **Environmental flows**: Return to rivers with minimum treatment
* **Groundwater recharge**: Requires tertiary treatment
* **Potable reuse**: Requires advanced treatment (not currently represented)

The economic attractiveness of wastewater reuse depends on:

* Cost of alternative water sources
* Stringency of discharge regulations
* Proximity of treatment plant to reuse location
* Seasonal patterns of supply and demand

Water reuse can provide 5-15% of total water supply in water-scarce urban regions (Awais et al., 2024 :cite:`awais_2024_nexus`).

Water Supply Portfolio
-----------------------

The model endogenously selects the optimal portfolio of water supply technologies based on:

* Resource availability and variability
* Technology costs and energy requirements
* Water quality requirements for different demands
* Infrastructure constraints and existing capacity
* Climate change impacts on conventional sources
* Sustainability constraints on groundwater use

In baseline scenarios, surface water typically provides 60-80% of total supply, groundwater 20-35%, and desalination/reuse 0-10% globally. In water-stressed climate scenarios, these shares shift substantially toward groundwater and desalination (Awais et al., 2024 :cite:`awais_2024_nexus`).

.. footbibliography::

