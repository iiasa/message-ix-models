Water
============

The water withdrawal and return flows from energy technologies are calculated in 
MESSAGE following the approach described in Fricko et al., (2016) :cite:`fricko_2016`.
Each technology is prescribed a water withdrawal and consumption intensity (e.g., m3 per kWh)
that translates technology outputs optimized in MESSAGE into water requirements and return
flows. 

.. figure:: /_static/ppl_energy_balance.png
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
technology options for individual power plant types (figure below) :cite:`parkinson_2019`.
Each power plant type that requires cooling in MESSAGE 
is connected to a corresponding cooling technology option (once-through, recirculating or
air cooling), with the investment into and operation of the cooling technologies included in the
optimization decision variables. This enables MESSAGE to choose the type of cooling technology
for each power plant type and track how the operation of the cooling technologies impact water
withdrawals, return flows, thermal pollution and parasitic electricity use. 

.. figure:: /_static/cooling_implement1.png
   :width: 820px
   :align: center
   
   Implementation of cooling technologies in the MESSAGE IAM.

Costs and efficiency for
cooling technologies are estimated following previous technology assessments :cite:`zhai_2010,zhang_2014,loew_2016`. 
The initial distribution of cooling technologies in each region
and for each technology is estimated with the dataset described in Raptis and Pfister (2016) (figure below) :cite:`Raptis_2016_powerplant_data`.    
   
.. figure:: /_static/cooling_implement2.png
   :width: 820px
   :align: center
   
   Average cooling technology shares across all power plant types at the river basin-scale. 