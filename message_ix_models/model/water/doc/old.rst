Water system implementation
==================================

Overview
--------

A representation of the global water sector is incorporated into the global MESSAGE model enabling
integrated assessment of clean water goals. The implementation is described below and
detailed explicitly in Parkinson et al. (2018) :cite:`parkinson_2018`.  The implementation
involves the following two steps:

1. 	``generate_water_constraints.r``

	Spatially-explicit analysis of gridded demands and socioeconomic indicators to develop
	pathways for sectoral water withdrawals, return flows and infrastructure penetration rates
	in each MESSAGE region. The pathways feature branching points reflecting a specific water
	sector development narrative (e.g., convergence towards achieving specific SDG targets).

2. 	``add_water_infrastructure.r``

	Processing input data and implemention into the MESSAGEix model using the ixmp utilities
	and solving the model for different policy cases to ensure the framework operates as anticipated.

The data processing results in a new set of baseline scenarios that feature embedded water infrastructure
accounting under different development narratives, and are specifically configured to examine issues
related to the Sustainable Development Goal for clean water and sanitation (SDG6).

The scripts associated with this workflow add all relevant parameters to an existing baseline SSP2 scenario from the ixDB.
Each scenario is solved and a solution is uploaded to the ixDB. Additionally, sensitivity scenarios featuring
a combination of increasingly stringent water efficiency and climate change mitigation targets are solved.
It takes multiple days to solve the scenarios sequentially.

Water for energy
---------------------

The water withdrawal and return flows from energy technologies are calculated in
MESSAGE following the approach described in Fricko et al., (2016) :cite:`fricko_2016`.
Each technology is prescribed a water withdrawal and consumption intensity (e.g., m3 per kWh)
that translates technology outputs optimized in MESSAGE into water requirements and return
flows.

.. figure:: _static/ppl_energy_balance.png
   :width: 320px
   :align: right

   Simplified power plant energy balance.

For power plant cooling technologies, the amount of water required and energy dissipated
to water bodies as heat is linked to the parameterized power plant fuel conversion efficiency (heat
rate). Looking at a simple thermal energy balance at the power plant (figure to the right), total combustion
energy (:math:`E_{comb}`) is conveterted into electricity (:math:`E_{elec}`), emissions (:math:`E_{emis}`)
and additional thermal energy that must be absorbed by the cooling system (:math:`E_{cool}`):

:math:`E_{comb} = E_{elec} + E_{emis} + E_{cool}`

Converting to per unit electricity, we can estimate the cooling required per unit of electricity generation
(:math:`\phi_{cool}`) based on average heat-rate (:math:`\phi_{comb}`) and heat lost to emissions
(:math:`\phi_{emis}`), and this data is identified from the literature :cite:`fricko_2016`.

:math:`\phi_{cool} = \phi_{comb} - \phi_{emis} - 1`

With time-varying heat-rates (i.e., :math:`t =0,1,2,...`) and a constant share of energy to emissions and electricity:

:math:`\phi_{cool}[t] = \phi_{comb}[t] \cdot \left( \, 1 - \dfrac{\phi_{emis}}{\phi_{comb}[0]} \, \right) - 1`

Increased fuel efficiency (lower heat-rate) reduces the per unit electricity cooling requirement.
This enables the fuel efficiency improvements included in MESSAGE to be translated into
improvements in water intensity. Water withdrawal and consumption intensities for power plant
cooling technologies are calibrated to the range
reported in Meldrum et al., (2013) :cite:`meldrum_2013`. Additional parasitic electricity demands from recirculating
and dry cooling technologies are accounted for explicitly in the electricity balance calculation. All
other technologies follow the data reported in Fricko et al.
(2016) :cite:`fricko_2016`.

A key feature of the implementation is the representation of power plant cooling
technology options for individual power plant types (figure below).
Each power plant type that requires cooling in MESSAGE
is connected to a corresponding cooling technology option (once-through, recirculating or
air cooling), with the investment into and operation of the cooling technologies included in the
optimization decision variables. This enables MESSAGE to choose the type of cooling technology
for each power plant type and track how the operation of the cooling technologies impact water
withdrawals, return flows, thermal pollution and parasitic electricity use.

.. figure:: _static/cooling_implement1.png
   :width: 820px
   :align: center

   Implementation of cooling technologies in the MESSAGE IAM :cite:`parkinson_2018`.

Costs and efficiency for
cooling technologies are estimated following previous technology assessments :cite:`zhai_2010,zhang_2014,loew_2016`.
The initial distribution of cooling technologies in each region
and for each technology is estimated with the dataset described in Raptis and Pfister (2016) (figure below) :cite:`Raptis_2016_powerplant_data`.

.. figure:: _static/cooling_implement2.png
   :width: 820px
   :align: center

   Average cooling technology shares across all power plant types at the river basin-scale :cite:`parkinson_2018`.

Water infrastructure
-----------------------

A reduced-form freshwater balance and water infrastructure investment module is incorporated into
MESSAGE to enable quantification of key interactions between water and
energy systems under sustainability transitions (figure below). Freshwater supply is constrained by renewable water availability
which is defined as a fraction of the base-year (2010) withdrawals to enable interactive
implementation of long-term conservation targets using existing water stress indicators. The conservation targets are
set based on the water sector development
narrative descrbed below combined with an assessment of future demands and degree of water stress at the river basin-scale.
The degree of water stress (i.e., low, medium, and high) is defined relative to the calculated ratio between historical
withdrawals and renewable water availability, and is estimated previously for global basin ecological regions with
modeled runoff data from the WaterGAP global hydrological model :cite:`alcamo_2003,hoekstra_2010`.  The implementation and
data sources are detailed in Parkinson et al. (2018) :cite:`parkinson_2018`.

.. figure:: _static/msg_water.png
   :width: 820px
   :align: center

   Simplified water infrastructure representation in the global MESSAGEix implementation :cite:`parkinson_2018`.

We applied a stylized approach to include expected water conservation costs and demand response at the regional-scale. We assume a general form
for the conservation curve that enables consistent linearization across regions (figure below). A maximum conservation potential in
each sector (municipal, manufacturing and irrigation) representing 30 % of the baseline withdrawals is assumed,
and is a somewhat conservative interpretation of previous assessments that focus specifically on water conservation potentials for specific sectors.

.. figure:: _static/cons_curve.png
   :width: 650px
   :align: center

   Stylized conservation curve and linear representation :cite:`parkinson_2018`.

Scenarios
----------------------------

The figure below outlines the parameterized water sector development scenarios in terms of branching points,
each defining a set of scenario indicators reflecting a specific realization the SDG6 targets.
It is important to emphasize the analysis does not cover all of the targets associated with SDG6, including
targets for flood management and transboundary cooperation. Two unique pathways consistent with the SDG6 narrative bridge
uncertainties driven by future end-use behavior and technological development. A supply-oriented pathway (SDG6-Supply) combines
the SDG6 policy implementation with business-as-usual (baseline) water use projections. The scenario primarily features expansion
of supply-side technologies in response to mitigating future demand growth. An efficiency-oriented pathway (SDG6-Efficiency)
features a transition towards a future where significant progress is made on the demand-side in terms of reaching sustainable
water consumption behavior. A detailed description of the indicator mapping onto the MESSAGE variables is described in
Parkinson et al. (2018) :cite:`parkinson_2018`.

.. figure:: _static/table_sdg_implement.png
   :width: 820px
   :align: center

   Water sector development scenarios and branching points.

The following scenarios can be imported as the default versions using the ``ixmp`` utilities:

1.	Baseline

	``model = "MESSAGE-GLOBIOM CD-LINKS R2.3.1"``

	``scenario = "baseline_globiom_base_watbaseline_w0_c0"``

2.	SDG6-Supply scenario

	``model = "MESSAGE-GLOBIOM CD-LINKS R2.3.1"``

	``scenario = "baseline_globiom_SDG_sdg6supp_w20_c0"``

3.	SDG6-Efficiency

	``model = "MESSAGE-GLOBIOM CD-LINKS R2.3.1"``

	``scenario = "baseline_globiom_SDG_sdg6eff_w30_c0"``

[Important Note] To solve the scenarios you must have implemented a full commodity
balance in the MESSAGE core model.

Data
-------------

Additional data used to parameterize the implementation can be found in the common data folder
on the `IIASA-ENE Sharepoint drive`_.

.. _`IIASA-ENE Sharepoint drive` : https://sp.ene.iiasa.ac.at:10443/Shared%20Documents/MESSAGE_data_sources/water

The implementation relies on harmonized gridded data for socioeconomic and hydro-climatic inputs, as well as a number of
other data files containing parameter mappings and technical performance data.
The specific data locations are indicated within the workflow scripts.

