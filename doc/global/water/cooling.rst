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

Cooling Technology Options
---------------------------

Three cooling technology categories are represented, plus a seawater variant for coastal plants. They differ in how much water they withdraw, how much they consume, and what they cost in lost generation.

.. list-table:: Cooling technology characteristics
   :widths: 22 20 20 20 18
   :header-rows: 1

   * - Technology
     - Water withdrawal
     - Water consumption
     - Efficiency penalty
     - Capital cost
   * - Once-through
     - Very high
     - Low
     - Negligible
     - Lowest
   * - Recirculating (wet tower)
     - Low
     - High
     - Small
     - Moderate
   * - Dry (air) cooling
     - Negligible
     - Negligible
     - Large, rises with ambient temperature
     - Highest
   * - Once-through, seawater
     - Very high (saline)
     - Low
     - Negligible
     - Low

Once-through cooling returns most of what it withdraws to the source, warmer; recirculating cooling withdraws far less but evaporates most of it; dry cooling removes the water constraint at the cost of generation. Seawater cooling draws on a separate saline supply and is available only to coastal plants, so it sidesteps freshwater scarcity entirely.

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

.. _cooling-climate-placeholder:

Climate Change Amplification
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. note:: **Placeholder.** Cooling technology performance is currently independent of the climate forcing scenario; the earlier climate impact factor on the capacity factor has been removed pending its replacement. Climate change still reaches cooling indirectly, through basin water availability.
