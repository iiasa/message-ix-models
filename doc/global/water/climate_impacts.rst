.. _water-climate-impacts:

Climate Change Impacts
======================

A key innovation of the MESSAGEix-Nexus module is the explicit representation of climate change impacts on both water availability and energy systems (Awais et al., 2024 :cite:`awais_2024_nexus`). This enables analysis of climate change adaptation strategies, compound risks at the water-energy nexus, and interactions between climate impacts and mitigation policies.

Overview
--------

Climate change affects the water-energy-land nexus through multiple pathways:

**Direct Impacts on Water**:

* Changes in precipitation patterns and amounts
* Shifts in snowmelt timing and magnitude
* Altered groundwater recharge rates
* Increased evapotranspiration
* More frequent and severe droughts
* Changes in seasonal water availability

**Direct Impacts on Energy**:

* Reduced thermal power plant efficiency due to higher ambient temperatures
* Increased cooling water requirements
* Higher cooling water temperatures constraining once-through cooling
* Changes in hydropower generation potential
* Shifts in electricity demand (heating vs. cooling)
* Impacts on renewable energy resources (wind, solar)

**Nexus Interactions**:

* Water scarcity constrains thermal power plant operation
* Temperature and water stress compound during heat waves
* Competing demands for limited water resources intensify
* Adaptation measures in one sector affect the other

MESSAGEix-Nexus represents these impacts through:

* Time-varying water availability from climate-driven hydrological models
* Temperature-dependent power plant efficiency
* Cooling technology performance degradation
* Changed sectoral water demands

Climate Forcing and Scenarios
------------------------------

Climate impacts are derived from climate model projections and impact models forced by these projections.

Representative Concentration Pathways (RCPs)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Climate forcing is characterized by Representative Concentration Pathways (van Vuuren et al., 2011 :cite:`vanvuuren_2011_rcp`):

* **RCP 2.6**: Strong mitigation, ~2°C warming by 2100
* **RCP 4.5**: Moderate mitigation, ~2.5°C warming by 2100  
* **RCP 6.0**: Moderate-high emissions, ~3°C warming by 2100
* **RCP 8.5**: High emissions, ~4-5°C warming by 2100

Each RCP implies different:

* Global mean temperature increase
* Regional temperature patterns
* Precipitation changes (regional and seasonal)
* Extreme event frequency and intensity

Shared Socioeconomic Pathways (SSPs)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The MESSAGEix-Nexus module is typically applied within the SSP-RCP scenario framework (O'Neill et al., 2014 :cite:`oneill_2014_ssp`; Riahi et al., 2017 :cite:`riahi_2017_ssp`):

* **SSP1-2.6**: Sustainability pathway with strong mitigation
* **SSP2-4.5**: Middle-of-the-road with moderate mitigation
* **SSP3-7.0**: Regional rivalry with weak mitigation
* **SSP5-8.5**: Fossil-fueled development with no mitigation

This framework enables exploration of how socioeconomic development pathways interact with climate change impacts on the water-energy nexus.

Climate Model Ensembles
^^^^^^^^^^^^^^^^^^^^^^^^

To account for climate model uncertainty, impacts are derived from ensembles of global climate models (GCMs):

* **CMIP5**: Coupled Model Intercomparison Project Phase 5 (used in IPCC AR5)
* **CMIP6**: CMIP Phase 6 (used in IPCC AR6)
* **Ensemble median**: Typical approach to represent central tendency
* **Ensemble spread**: Can be used to explore uncertainty

Typically, 5-10 GCMs are used to force hydrological models, and ensemble statistics (median, quantiles) are calculated at the basin scale.

Hydrological Impacts
--------------------

Changes in water availability are the most direct climate impact on the water-energy nexus and are represented through outputs from global hydrological models.

Hydrological Model Framework
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Two global hydrological models are primarily used:

**PCR-GLOBWB 2** (Sutanudjaja et al., 2018 :cite:`sutanudjaja_2018_pcrglobwb`):

* 5 arcmin spatial resolution (~10 km at equator)
* Simulates full terrestrial water cycle
* Includes surface water, soil moisture, groundwater
* Forced by climate model outputs (temperature, precipitation, etc.)
* Provides runoff, groundwater recharge, and river discharge

**CWatM** - Community Water Model (Burek et al., 2020 :cite:`burek_2020_cwatm`):

* Variable resolution (typically 5 arcmin)
* Represents water availability, demand, and allocation
* Includes reservoirs and water management
* Can simulate environmental flows
* Provides similar outputs to PCR-GLOBWB

Both models are forced by bias-corrected climate model outputs to simulate historical (1971-2000) and future (2020-2100) water availability.

Spatial Aggregation
^^^^^^^^^^^^^^^^^^^

Hydrological model outputs are spatially aggregated to MESSAGE basins:

1. **Grid cell outputs** (5 arcmin resolution)
2. **Basin delineation** using HydroSHEDS
3. **Area-weighted aggregation** to ~200 MESSAGE basins
4. **Mapping to MESSAGE regions** (R12) for consistency

This multi-scale approach preserves spatial heterogeneity while enabling computational tractability.

Temporal Aggregation
^^^^^^^^^^^^^^^^^^^^

Hydrological model outputs are temporally aggregated:

* **Native resolution**: Daily or monthly
* **Sub-annual MESSAGE**: Seasonal or monthly averages
* **Annual MESSAGE**: Annual mean with optional reliability constraints

For climate impact studies, sub-annual resolution is critical to capture seasonal dynamics.

Key Hydrological Impact Patterns
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Climate change impacts on water availability exhibit strong regional patterns (Awais et al., 2024 :cite:`awais_2024_nexus`):

**Wetting Regions** (increased water availability):

* High northern latitudes (more precipitation, earlier snowmelt)
* Parts of East Africa (intensified monsoons)
* Some tropical regions (increased convective precipitation)

**Drying Regions** (decreased water availability):

* Mediterranean basin (reduced precipitation, increased evaporation)
* Middle East and North Africa (lower precipitation)
* Southern Africa (decreased precipitation)
* Parts of South America (Amazon, Northeast Brazil)
* Southwestern USA (reduced snowpack, increased evaporation)

**Seasonal Shifts** (changed timing of availability):

* Snow-dominated basins (earlier snowmelt peak, lower summer flows)
* Monsoon regions (potential shifts in monsoon timing and intensity)
* Mediterranean climate regions (drier summers, wetter winters)

**Increased Variability**:

* More frequent and intense droughts
* Increased interannual variability
* Higher flood risks (not directly represented in MESSAGE)

Drought Representation
^^^^^^^^^^^^^^^^^^^^^^^

Droughts are represented through:

* **Low flow quantiles**: 10th or 20th percentile of flow distribution
* **Multi-year sequences**: Persistent dry periods from climate model runs
* **Statistical characterization**: Changes in drought frequency, duration, and severity

The model can use low-flow quantiles to represent water availability under drought conditions, testing system resilience.

Energy System Impacts
---------------------

Climate change directly affects energy system performance through temperature-dependent efficiency and cooling constraints.

Thermal Power Plant Efficiency
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Thermal power plant efficiency declines with higher ambient temperature through:

1. **Thermodynamic efficiency**: Carnot efficiency ∝ (T_hot - T_cold); higher T_cold reduces efficiency
2. **Cooling system performance**: Less effective heat rejection at high ambient temperatures
3. **Auxiliary loads**: Increased cooling system energy requirements

The efficiency penalty is represented as:

:math:`\eta(T) = \eta_0 \cdot \left(1 - \alpha \cdot (T - T_0)\right)`

where:

* :math:`\eta(T)` is efficiency at ambient temperature :math:`T`
* :math:`\eta_0` is reference efficiency at reference temperature :math:`T_0`
* :math:`\alpha` is temperature sensitivity coefficient (~0.2-0.5% per °C)

Typical efficiency penalties:

* **+1°C ambient temperature**: 0.2-0.5% efficiency loss
* **+3°C (RCP 4.5 by 2100)**: 0.6-1.5% efficiency loss
* **+5°C (RCP 8.5 by 2100)**: 1.0-2.5% efficiency loss

This translates to increased fuel consumption and emissions for the same electricity output.

Cooling Technology Performance
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Different cooling technologies respond differently to temperature increases:

**Once-Through Cooling**:

* Moderate efficiency penalty from higher ambient/water temperature
* Potentially binding constraints on intake/discharge water temperature
* Forced curtailment or shutdown during extreme heat events
* Affected by low flow conditions (reduced dilution capacity)

**Recirculating (Wet Tower) Cooling**:

* Moderate efficiency penalty
* Performance degrades at high wet-bulb temperature (limiting evaporation)
* Can operate at higher ambient temperatures than once-through
* Increased water consumption due to higher evaporation rates

**Dry Cooling**:

* Severe efficiency penalty at high temperatures (5-10% at 40°C)
* No water availability constraint
* Performance critical during heat waves when electricity demand peaks
* May require capacity derating at extreme temperatures

Climate change thus creates differential impacts, making dry cooling relatively less attractive in hot climates despite eliminating water use.

Cooling Water Temperature Limits
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Environmental regulations often limit:

* **Intake temperature**: Maximum temperature of water that can be withdrawn
* **Discharge temperature**: Maximum temperature of water returned to environment
* **Delta-T**: Maximum temperature increase between intake and discharge

Typical limits:

* Discharge temperature: 30-35°C (varies by jurisdiction and water body)
* Delta-T: 3-5°C for once-through cooling

As river and lake temperatures increase with climate change:

* More frequent violations of discharge limits
* Required curtailment or shutdown during hot periods
* Economic incentive to retrofit to recirculating or dry cooling

This is represented through temperature-dependent availability constraints on once-through cooling.

Hydropower Generation
^^^^^^^^^^^^^^^^^^^^^^

Hydropower is affected by changes in:

* **Annual runoff**: Determines total generation potential
* **Seasonal patterns**: Affects capacity factor and firm capacity
* **Reservoir inflows**: Impacts storage and regulation capability
* **Competing water uses**: Irrigation, municipal, environmental flows

Regional hydropower impacts:

* **Increases**: High latitudes, some tropical regions with increased precipitation
* **Decreases**: Snow-dominated basins (reduced summer flows), drying regions
* **Seasonal shifts**: Earlier spring peak, lower summer generation in snow basins

Hydropower impacts are implicitly represented through changed water availability in basins with hydropower resources.

Electricity Demand
^^^^^^^^^^^^^^^^^^

Climate change shifts electricity demand patterns through:

* **Increased cooling demand**: Higher temperatures increase air conditioning loads
* **Decreased heating demand**: Milder winters reduce heating loads
* **Peak demand shifts**: More summer peaks, fewer winter peaks in many regions

The net effect varies by region:

* **Hot regions**: Large increase in cooling demand
* **Cold regions**: Decreased heating demand may offset cooling increases
* **Temperate regions**: Mixed effects depending on baseline climate

Demand impacts are represented through temperature-dependent demand adjustments in MESSAGE.

Compound Events and Cascading Impacts
--------------------------------------

A critical insight from MESSAGEix-Nexus is that climate impacts at the water-energy nexus can compound and cascade, creating risks greater than the sum of individual impacts.

Heat-Drought Compound Events
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Heat waves and droughts often co-occur, creating compounding stresses:

**Simultaneous Impacts**:

* High electricity demand (cooling loads)
* Reduced power plant efficiency (high temperature)
* Low water availability (drought)
* High water temperature (constrains once-through cooling)
* Competing water demands (irrigation for stressed crops)

**Cascading Effects**:

* Water scarcity forces power plant curtailment
* Reduced electricity supply during peak demand
* Higher electricity prices and potential shortages
* Reduced economic output from energy-intensive industries
* Water allocation conflicts between sectors

Historical examples:

* **2003 European heat wave**: Nuclear plants curtailed due to high river temperatures
* **2012 US drought**: Thermal plants constrained by low water availability and high temperatures
* **2010 Russian heat wave**: Energy and water systems both severely stressed

Climate change increases the frequency and severity of such events (Satoh et al., 2022 :cite:`satoh_2022_drought`).

Nexus Stress Indicators
^^^^^^^^^^^^^^^^^^^^^^^^

MESSAGEix-Nexus can quantify nexus stress through indicators:

* **Water scarcity index**: Ratio of demand to availability
* **Energy-water stress**: Frequency of water constraints on energy generation
* **Compound event frequency**: Co-occurrence of heat, drought, and high demand
* **Adaptation costs**: Investment required to maintain energy and water security

These indicators show nonlinear increases under high-emission scenarios, with stress intensifying after mid-century (Awais et al., 2024 :cite:`awais_2024_nexus`).

Regional Vulnerability
^^^^^^^^^^^^^^^^^^^^^^

Regions with high vulnerability to compound water-energy-climate risks:

* **Middle East and North Africa**: Already water-scarce, extreme heat, high cooling demands
* **South Asia**: High population, monsoon variability, irrigation demands
* **Mediterranean**: Drying trend, summer heat, tourism-driven peak demand
* **Southwestern USA**: Declining Colorado River, heat waves, competing demands
* **Australia**: Droughts, heat, limited water resources

These regions show the largest impacts in MESSAGEix-Nexus scenarios and require substantial adaptation investment.

Adaptation Strategies
---------------------

MESSAGEix-Nexus represents multiple adaptation strategies that can be endogenously selected or exogenously imposed.

Water Supply Adaptation
^^^^^^^^^^^^^^^^^^^^^^^^

Expanding water supply through:

* **Desalination**: Coastal regions can invest in seawater desalination (energy-intensive)
* **Groundwater expansion**: Where sustainable reserves exist (depth-dependent costs)
* **Wastewater reuse**: Treated effluent for non-potable uses
* **Inter-basin transfers**: Where infrastructure exists or can be built

The model selects the least-cost portfolio of supply options based on:

* Resource availability and costs
* Energy requirements and availability
* Competing demands
* Infrastructure constraints

Energy System Adaptation
^^^^^^^^^^^^^^^^^^^^^^^^^

Adapting energy systems to water constraints:

* **Cooling technology shifts**: Move from water-intensive to dry cooling
* **Generation technology shifts**: Increase wind, solar PV (no cooling water)
* **Plant siting**: Locate new thermal plants near reliable water sources
* **Operational flexibility**: Dispatch based on water availability and temperature

The model endogenously optimizes technology choice and dispatch.

Demand-Side Adaptation
^^^^^^^^^^^^^^^^^^^^^^^

Reducing water demands through:

* **Irrigation efficiency**: Drip irrigation, scheduling optimization
* **Industrial water recycling**: Closed-loop systems
* **Municipal efficiency**: Leak reduction, efficient appliances
* **Energy efficiency**: Reduces cooling water requirements

Some efficiency improvements are represented through exogenous technology improvement; others can be investment options.

Integrated Nexus Adaptation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Optimal adaptation often involves coordinated strategies:

* **Renewable energy + desalination**: Use solar PV to power desalination
* **Wastewater reuse for cooling**: Close water loop in industrial areas
* **Seasonal coordination**: Align energy maintenance with low water periods
* **Portfolio diversification**: Mix of generation and water supply options

The integrated optimization in MESSAGEix-Nexus can identify such synergistic solutions.

Adaptation Costs and Limits
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Adaptation is not costless:

* **Desalination**: 0.50-1.50 USD/m³ + energy costs
* **Dry cooling**: 8-15% capital cost increase + 3-8% efficiency penalty
* **Renewable energy**: Capital cost differential (though declining)
* **Efficiency improvements**: Upfront investment requirements

At high levels of climate change (RCP 8.5), adaptation costs can be substantial:

* 10-30% increase in water supply costs in water-stressed regions
* 5-15% increase in electricity generation costs
* Trade-offs with other investment priorities (development, mitigation)

There may also be **adaptation limits** where physical or economic constraints prevent full adaptation:

* Finite desalination capacity expansion rates
* Thermodynamic limits on dry cooling in extreme heat
* Competing uses for limited renewable energy
* Social and institutional barriers to demand reduction

Results and Insights
---------------------

Application of MESSAGEix-Nexus with climate impacts provides key insights (Awais et al., 2024 :cite:`awais_2024_nexus`):

Baseline Climate Impacts
^^^^^^^^^^^^^^^^^^^^^^^^^

In baseline (no climate policy) scenarios with climate change (RCP 4.5 or 8.5):

* **Water availability** declines in 40-50% of global basins by 2050-2100
* **Thermal power efficiency** reduced by 0.5-2% globally by 2100
* **Cooling water constraints** become binding in 20-30% of basins with thermal generation
* **Adaptation costs** reach 50-150 billion USD/year globally by 2050

Regional impacts vary dramatically:

* **MENA, South Asia**: Severe water scarcity, high adaptation costs
* **Europe, North America**: Moderate impacts, mostly manageable through adaptation
* **Sub-Saharan Africa**: Heterogeneous impacts, limited adaptation capacity

Climate Mitigation Co-Benefits
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Stringent climate mitigation (RCP 2.6) substantially reduces nexus stress:

* **Reduced temperature impacts**: Limits ambient temperature increases
* **Smaller hydrological changes**: Moderates precipitation and runoff changes
* **Lower thermal generation**: Rapid coal and gas phase-out reduces cooling water demand
* **Renewable expansion**: Wind and solar eliminate most cooling water requirements

Co-benefits of mitigation for water resources:

* 30-60% reduction in adaptation costs by 2050 (mitigation vs. baseline)
* Avoided water scarcity in 10-20% of basins
* Reduced compound event frequency by 40-70%

This demonstrates that climate mitigation provides substantial co-benefits for water-energy security.

SDG Interactions Under Climate Change
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Climate change exacerbates trade-offs between SDGs:

* **SDG6 vs. SDG7**: Water access competes with energy access in water-scarce regions
* **SDG13 (climate action) supports both**: Mitigation reduces nexus stress
* **Costs of achieving SDGs**: 20-50% higher under RCP 8.5 vs. RCP 2.6

Regional variation is critical:

* **MENA, South Asia**: Difficult to achieve both SDG6 and SDG7 under high climate change without substantial investment
* **Other regions**: Generally feasible but at higher cost

The integrated framework allows quantification of these trade-offs and identification of investment priorities.

Uncertainty and Robustness
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Key uncertainties in climate impact assessment:

* **Climate model spread**: ±30-50% uncertainty in regional precipitation changes
* **Hydrological model differences**: Different models show different sensitivities
* **Socioeconomic assumptions**: SSP pathway affects vulnerability and adaptive capacity
* **Technology development**: Uncertain costs and performance of adaptation options

Robust adaptation strategies that perform well across scenarios:

* **Renewable energy expansion**: Reduces cooling water needs across all scenarios
* **Water use efficiency**: Low-regret option with multiple benefits
* **Diversified supply portfolio**: Reduces vulnerability to single source failures
* **Flexible infrastructure**: Can adjust to different future conditions

Sensitivity analysis and scenario exploration help identify robust strategies.

Model Validation and Evaluation
--------------------------------

The climate impact representation in MESSAGEix-Nexus has been evaluated through:

Historical Validation
^^^^^^^^^^^^^^^^^^^^^

Comparison of historical simulations (1971-2000) with observations:

* **Water availability**: Hydrological models reproduce observed runoff patterns
* **Power plant performance**: Temperature-efficiency relationships match empirical data
* **Heat wave impacts**: Historical events (2003 Europe, 2012 USA) can be reproduced

Intercomparison with Other Models
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Comparison with other integrated assessment and water-energy models:

* **Qualitative agreement** on direction and magnitude of major impacts
* **Regional patterns** consistent across models
* **Quantitative differences** due to spatial resolution, representation details

The multi-model comparison builds confidence in key findings while highlighting uncertainties.

Stakeholder Engagement
^^^^^^^^^^^^^^^^^^^^^^

Results have been presented to and evaluated by:

* Water resource managers
* Energy system planners
* Climate adaptation practitioners
* Policy makers

Feedback has validated the relevance of modeled impacts and adaptation options while highlighting additional considerations (institutional barriers, equity, etc.) not fully represented in the model.

Future Development
------------------

Ongoing and planned enhancements to climate impact representation:

**Enhanced Hydrology**:

* More detailed reservoir and water management representation
* Groundwater-surface water interactions
* Water quality and temperature tracking

**Energy System Details**:

* Sub-daily electricity demand and generation
* Transmission constraints affected by temperature
* Renewable energy resource climate sensitivities (wind, solar)

**Extremes and Risks**:

* Explicit flood representation
* Cascading infrastructure failures
* Financial and economic risk metrics

**Socioeconomic Impacts**:

* Health impacts of compound heat-water stress
* Migration and displacement from water scarcity
* Inequality in climate impact distribution

These developments will further enhance the capability of MESSAGEix-Nexus to inform climate adaptation and resilience planning for water-energy systems.

.. footbibliography::

