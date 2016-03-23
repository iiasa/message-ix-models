Energy
==========
The Reference Energy System (RES) defines the total set of available energy conversion technologies. In MESSAGE terms, energy conversion technology refers to all types of energy technologies from resource extraction to transformation, transport, distribution of energy carriers, and end-use technologies.

Because few conversion technologies convert resources directly into useful energy, the energy system in MESSAGE is divided into 5 energy levels:

* Resource (r), like coal, oil, natural gas in the ground or biomass on the field
* Primary (a) energy, like crude oil at the refinery
* Secondary (x) energy, like gasoline or diesel fuel at the refinery, or wind- or solar power at the powerplant
* Final (f) energy, like diesel fuel in the tank of a car or electricity at the socket
* Useful (u) energy that satisfies some demand for energy services, like heating, lighting or moving people

Technologies can take in from one level and put out at another level or on the same level. The energy forms defined in each level can be envisioned as a transfer hub, that the various technologies feed into or pump away from. The useful energy demand is given as a time series. Technologies can also vary per time period.

The mathematical formulation of MESSAGE ensures that the flows are consistent: demand is met, inflows equal outflows and constraints are not exceeded.

.. toctree::
   :maxdepth: 1

   resource
   conversion/index
   transport
   resid_commerc
   industrial
