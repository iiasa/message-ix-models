.. _water-cooling:

Power Plant Cooling Technologies
=================================

Power plant cooling technologies are a critical component of the water-energy nexus in MESSAGEix-Nexus. Thermal power plants (coal, gas, nuclear, concentrated solar power, geothermal) require cooling to dissipate waste heat from the thermodynamic cycle. The implementation of cooling technologies in MESSAGE explicitly represents the tradeoffs between water use, energy efficiency, and capital costs (Fricko et al., 2016 :cite:`fricko_2016`; Parkinson et al., 2019 :cite:`parkinson_2019`; Awais et al., 2024 :cite:`awais_2024_nexus`).

Thermodynamic Basis
-------------------

The water requirements and thermal pollution from power plant cooling are fundamentally linked to the plant's thermodynamic efficiency through the energy balance.

Energy Balance
^^^^^^^^^^^^^^

Looking at a simplified thermal energy balance at the power plant (:numref:`fig-ppl_energy_balance`), total combustion energy (:math:`E_{comb}`) is converted into:

* Electricity (:math:`E_{elec}`)
* Emissions and stack losses (:math:`E_{emis}`)
* Waste heat absorbed by cooling system (:math:`E_{cool}`)

:math:`E_{comb} = E_{elec} + E_{emis} + E_{cool}`

.. _fig-ppl_energy_balance:
.. figure:: /_static/ppl_energy_balance.png
   :width: 400px
   :align: center
   
   Simplified power plant energy balance.

Converting to per unit electricity generation, we can estimate the cooling requirement per unit of electricity (:math:`\phi_{cool}`) based on average heat rate (:math:`\phi_{comb}`) and heat lost to emissions (:math:`\phi_{emis}`):

:math:`\phi_{cool} = \phi_{comb} - \phi_{emis} - 1`

where all quantities are expressed per unit of electricity output (e.g., MJ thermal per MWh electric).

Time-Varying Heat Rates
^^^^^^^^^^^^^^^^^^^^^^^^

With time-varying heat rates (i.e., :math:`t = 0,1,2,...`) representing efficiency improvements, and assuming a constant share of energy to emissions and electricity:

:math:`\phi_{cool}[t] = \phi_{comb}[t] \cdot \left( 1 - \dfrac{\phi_{emis}}{\phi_{comb}[0]} \right) - 1`

This formulation enables heat rate improvements for power plants represented in MESSAGE to be automatically translated into improvements (reductions) in cooling water intensity. As plants become more efficient (lower heat rate), less waste heat must be dissipated per unit of electricity generated.

For example:

* **Coal plant**: Heat rate improvement from 10,000 MJ/MWh (36% efficient) to 8,500 MJ/MWh (42% efficient) reduces cooling requirement by ~15%
* **Gas combined cycle**: Heat rate improvement from 6,500 MJ/MWh (55% efficient) to 5,800 MJ/MWh (62% efficient) reduces cooling requirement by ~11%

Cooling Water Intensities
^^^^^^^^^^^^^^^^^^^^^^^^^^

Water withdrawal and consumption intensities for power plant cooling technologies are calibrated to ranges reported in the literature (Meldrum et al., 2013 :cite:`meldrum_2013`; Macknick et al., 2012 :cite:`macknick_2012`). The intensities account for:

* Waste heat to be dissipated (from heat rate)
* Cooling technology efficiency
* Ambient conditions (temperature, humidity)
* Water temperature limits for discharge

Representative water intensities are provided in :ref:`water-demand` and vary by plant type and cooling technology.

Cooling Technology Options
---------------------------

Three main cooling technology categories are represented in MESSAGEix-Nexus, each with distinct characteristics regarding water use, energy penalties, and costs.

Once-Through Cooling
^^^^^^^^^^^^^^^^^^^^

Once-through (open-loop) cooling draws water from a surface water body, passes it through the condenser to absorb waste heat, and returns the warmed water to the source.

**Characteristics**:

* **Very high water withdrawal**: 100-200 m³/MWh depending on plant type
* **Low water consumption**: 1-2 m³/MWh (only evaporation from source due to heating)
* **Minimal energy penalty**: Small pumping requirement (<0.3% of generation)
* **Low capital cost**: Simplest cooling system
* **Requires large water body**: River, lake, or ocean with adequate flow
* **Thermal pollution**: Discharged water is 8-15°C warmer than intake

**Advantages**:

* Lowest cost option
* Minimal parasitic energy loss
* Simple operation and maintenance

**Disadvantages**:

* Very high water withdrawal (though mostly returned)
* Thermal pollution impacts aquatic ecosystems
* Restricted by environmental regulations in many regions
* Requires proximity to large, reliable water source
* Vulnerable to water temperature constraints during heat waves

**Availability**:

* Primarily for coastal plants (seawater cooling)
* Large rivers with high minimum flows
* Great Lakes and similar large water bodies
* Increasingly restricted by environmental regulations (EU, USA)

Recirculating (Wet Tower) Cooling
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Recirculating cooling uses a closed-loop system where water circulates between the condenser and a cooling tower. Heat is dissipated by evaporation in the cooling tower.

**Characteristics**:

* **Low water withdrawal**: 2-4 m³/MWh (to replace evaporation and blowdown)
* **Moderate-high water consumption**: 2-4 m³/MWh (mostly evaporation)
* **Small energy penalty**: 1-2% of generation (pumps, fans)
* **Moderate capital cost**: Cooling tower construction
* **Independent of large water bodies**: Can be located anywhere with adequate water supply
* **Minimal thermal pollution**: Water recirculates; only blowdown is discharged

**Advantages**:

* Much lower withdrawal than once-through
* Can be sited inland without large water body
* Minimal thermal discharge to environment
* Widely accepted technology

**Disadvantages**:

* Moderate-high water consumption (comparable to or higher than once-through)
* Energy penalty reduces net generation
* Higher capital and operating costs than once-through
* Visible water vapor plumes
* Still vulnerable to water scarcity during droughts

**Availability**:

* Standard technology for inland plants
* Can be retrofitted to existing once-through plants
* Suitable for most locations with adequate water supply

Dry (Air) Cooling
^^^^^^^^^^^^^^^^^

Dry cooling uses air instead of water to dissipate heat, eliminating water consumption. Heat is transferred via air-cooled condensers or air-cooled heat exchangers.

**Characteristics**:

* **Minimal water withdrawal**: 0.05-0.15 m³/MWh (only for auxiliary systems)
* **Minimal water consumption**: 0.05-0.15 m³/MWh (>95% reduction vs. wet cooling)
* **Significant energy penalty**: 3-8% of generation depending on climate
* **High capital cost**: Large air-cooled condenser surface area
* **Climate-dependent performance**: Efficiency loss greater in hot climates
* **No thermal water pollution**: All heat dissipated to atmosphere

**Advantages**:

* Eliminates water use for cooling (~95-99% reduction)
* Enables plant siting in water-scarce regions
* No thermal water pollution
* No water availability risk to plant operations

**Disadvantages**:

* Significant efficiency penalty (especially in hot weather)
* Much higher capital cost (2-3× cooling system cost)
* Larger physical footprint
* Performance degradation during heat waves (when power demand peaks)
* Higher operating costs due to energy penalty

**Availability**:

* Increasingly used in water-scarce regions
* Required in some jurisdictions with limited water
* Growing market share for new plants in arid regions

Implementation in MESSAGEix-Nexus
----------------------------------

The cooling technology representation in MESSAGEix-Nexus allows the model to endogenously select the optimal cooling technology for each power plant type in each region and time period (Parkinson et al., 2019 :cite:`parkinson_2019`; Awais et al., 2024 :cite:`awais_2024_nexus`).

Technology Structure
^^^^^^^^^^^^^^^^^^^^

Each thermal power plant type that requires cooling is connected to multiple cooling technology options (:numref:`fig-cooling_implement1`). The investment and operation of cooling technologies are explicit decision variables in the optimization.

.. _fig-cooling_implement1:
.. figure:: /_static/cooling_implement1.png
   :width: 800px
   :align: center

   Implementation of cooling technologies in the MESSAGE IAM (Fricko et al., 2016 :cite:`fricko_2016`).

For example, a coal power plant can be built with:

* Coal plant + once-through cooling
* Coal plant + recirculating cooling  
* Coal plant + dry cooling

Each combination has specific:

* **Capital costs**: Plant cost + cooling system cost
* **Efficiency**: Plant efficiency - cooling energy penalty
* **Water withdrawal/consumption**: Technology-specific intensities
* **Operational constraints**: Water availability, thermal limits

The model simultaneously optimizes:

* Which power plants to build
* Which cooling technology to pair with each plant
* Operational dispatch considering water and energy constraints

Cost Representation
^^^^^^^^^^^^^^^^^^^

Cooling technology costs enter the model as:

* **Capital cost differential**: the additional investment for the cooling system relative to once-through cooling, which is the cheapest option
* **Efficiency penalty**: the parasitic load of pumps and fans, which reduces net electricity output and is largest for dry cooling
* **Operating costs**: maintenance and the additional fuel implied by the efficiency penalty

Cost assumptions are derived from technology assessments (Zhai and Rubin, 2010 :cite:`zhai_2010`; Zhang et al., 2014 :cite:`zhang_2014`; Loew et al., 2016 :cite:`loew_2016`).

Initial Cooling Technology Distribution
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The base year (2020) distribution of cooling technologies for existing power plants is estimated using the dataset from Raptis and Pfister (2016) :cite:`Raptis_2016_powerplant_data`, which provides plant-level cooling technology data. 

Basin-scale shares of cooling technologies across all power plant types are shown in :numref:`fig-cooling_implement2`. The historical distribution shows:

* **Coastal regions**: Predominantly once-through cooling
* **Inland rivers**: Mix of once-through and recirculating
* **Arid inland regions**: Higher share of dry and recirculating cooling
* **Developed regions**: Shift toward recirculating due to environmental regulations

.. _fig-cooling_implement2:
.. figure:: /_static/cooling_implement2.png
   :width: 800px
   :align: center
   
   Average cooling technology shares across all power plant types at the river basin-scale (Fricko et al., 2016 :cite:`fricko_2016`).

Future cooling technology choices are endogenous based on:

* Water availability and scarcity
* Regulatory constraints (thermal pollution limits)
* Technology costs and performance
* Competition with other water demands

Water-Energy Tradeoffs
----------------------

The explicit cooling technology representation enables MESSAGEix-Nexus to capture key water-energy tradeoffs.

Water Scarcity Drives Technology Choice
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In water-scarce regions or time periods, the model faces a choice:

1. **Build thermal plants with water-intensive cooling**: Requires water allocation from other uses or new water supply
2. **Build thermal plants with dry cooling**: Higher cost and efficiency penalty
3. **Build alternative generation technologies**: Renewables (wind, solar PV) that don't require cooling water

The optimal choice depends on:

* Relative costs of water supply vs. efficiency penalty
* Availability and cost of alternative generation
* Value of water in competing uses

Example: In a water-scarce basin, if groundwater costs 0.20 USD/m³ and a gas combined cycle plant requires 2.5 m³/MWh with wet cooling, the water cost is 0.50 USD/MWh. Dry cooling eliminates this water cost but has a ~4% efficiency penalty. At gas prices of 5 USD/GJ and 6,000 MJ/MWh heat rate, the efficiency penalty costs ~1.20 USD/MWh. If capital cost differential is small, wet cooling remains attractive despite water costs.

Climate Change Amplification
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. note:: **Placeholder — to be completed.**
   Earlier versions of the module adjusted the cooling technology capacity
   factor by a climate- and region-dependent impact factor, so that warming
   degraded cooling performance directly. That representation has been removed
   from the build pending its replacement, and the cooling technology
   parameterisation is currently independent of the climate forcing scenario.

   Climate change still reaches cooling indirectly, through basin water
   availability: a drier basin raises the cost of the water that wet cooling
   needs, which shifts the technology choice. The direct temperature effect on
   plant and cooling system performance is not currently represented.

   This section should describe the replacement representation once it lands.

Regional Patterns
^^^^^^^^^^^^^^^^^

Cooling technology evolution varies by region:

**Water-Abundant Regions** (Northern Europe, Canada, parts of South America):

* Continued use of once-through cooling where environmentally acceptable
* Recirculating cooling as standard inland
* Limited dry cooling adoption

**Water-Stressed Regions** (Middle East, North Africa, Central Asia, Australia):

* Rapid shift to dry cooling for new thermal plants
* Reduced overall thermal generation share
* Increased solar PV and wind (no cooling water requirements)

**Developing Regions** (South Asia, Southeast Asia, Sub-Saharan Africa):

* Initial expansion with recirculating cooling (standard technology)
* Potential shift to dry cooling if water scarcity intensifies
* Competition between energy access and water access goals

**Transition Regions** (China, India, Western USA):

* Mix of technologies depending on local water availability
* Retrofits of once-through to recirculating
* New plants increasingly using dry cooling in water-scarce areas

Scenario Results
----------------

.. note:: **Placeholder — to be completed.**
   This section should summarise cooling technology results from the current
   model version: the evolution of the once-through, recirculating and dry
   cooling shares across SSPs and forcing scenarios, how mitigation changes
   cooling water demand as thermal generation declines, and how SDG6 water
   access constraints interact with cooling technology choice. The figures
   previously given here described an earlier model version and have been
   removed rather than carried forward unverified.

Key Insights
------------

The cooling technology representation in MESSAGEix-Nexus provides several key insights:

1. **Water availability is an important constraint** on energy system development in water-scarce regions, affecting technology choice and generation dispatch.

2. **Endogenous cooling technology choice** enables the model to find cost-effective adaptation strategies to water scarcity, including shifts to dry cooling and alternative generation technologies.

3. **Water availability links the water and energy systems**, so climate-driven changes in basin hydrology propagate into energy system technology choice.

4. **Mitigation reduces nexus stress**: Climate mitigation scenarios reduce cooling water demand by phasing out thermal generation, providing a co-benefit for water resources.

5. **SDG interactions are complex**: Achieving universal water access can constrain energy system choices in water-scarce regions, requiring careful planning and investment.

6. **Regional heterogeneity matters**: Global average trends obscure important regional dynamics where water-energy constraints are binding.

The implementation demonstrates the value of integrated water-energy modeling for understanding nexus interactions, identifying vulnerabilities, and evaluating policy and technology options.

.. footbibliography::

