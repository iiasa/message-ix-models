.. _message:

Energy (MESSAGE)
==========
MESSAGE (Model for Energy Supply Strategy Alternatives and their General Environmental Impact) is a linear programming (LP) energy engineering model with global coverage. 
As a systems engineering optimization model, MESSAGE is used for medium- to long-term energy system planning, energy policy analysis, and scenario development 
(Messner and Strubegger, 1995 :cite:`messner_users_1995`). The model provides a framework for representing an energy system with all its interdependencies from resource 
extraction, imports and exports, conversion, transport, and distribution, to the provision of energy end-use services such as light, space conditioning, industrial 
production processes, and transportation. In addition, MESSAGE links to GLOBIOM (GLObal BIOsphere Model, cf. Section :ref:`globiom`) to consistently assess the implications 
of utilizing bioenergy of different types and to integrate the GHG emissions from energy and land use and to the aggregated macro-economic model MACRO (cf. Section :ref:`macro`)
to assess econmic implications and to capture economic feedbacks.

MESSAGE covers all greenhouse gas (GHG)-emitting sectors, including energy, industrial processes as well as - through its linkage to GLOBIOM - agriculture and forestry, 
for a full basket of greenhouse gases and other radiatively active gases, including CO2, CH4 ,N2O, NOx, volatile organic compounds (VOCs), CO, SO2, BC/OC, CF4, C2F6, 
HFC125, HFC134a, HFC143a, HFC227ea, HFC245ca and SF6. MESSAGE is used in conjunction with MAGICC (Model for Greenhouse gas Induced Climate Change) version 6.8 (cf. Section 
:ref:`magicc`) for calculating internally consistent scenarios for atmospheric concentrations, radiative forcing, annual-mean global surface air temperature and 
global-mean sea level implications.

The model is designed to formulate and evaluate alternative energy supply strategies consonant with the user-defined constraints such as limits on new investment, fuel 
availability and trade, environmental regulations and market penetration rates for new technologies. Environmental aspects can be analysed by accounting, and if necessary 
limiting, the amounts of pollutants emitted by various technologies at various steps in energy supplies. This helps to evaluate the impact of environmental regulations 
on energy system development.

It's principal results comprise, among others, estimates of technology-specific multi-sector response strategies for specific climate stabilization targets. By doing so, 
the model identifies the least-cost portfolio of mitigation technologies. The choice of the individual mitigation options across gases and sectors is driven by the relative 
economics of the abatement measures, assuming full temporal and spatial flexibility (i.e., emissions-reduction measures are assumed to occur when and where they are 
cheapest to implement).

The Reference Energy System (RES) defines the total set of available energy conversion technologies. In MESSAGE terms, energy conversion technology refers to all types 
of energy technologies from resource extraction to transformation, transport, distribution of energy carriers, and end-use technologies.

Because few conversion technologies convert resources directly into useful energy, the energy system in MESSAGE is divided into 5 energy levels:

* Resource (r), like coal, oil, natural gas in the ground or biomass on the field
* Primary (a) energy, like crude oil at the refinery
* Secondary (x) energy, like gasoline or diesel fuel at the refinery, or wind- or solar power at the powerplant
* Final (f) energy, like diesel fuel in the tank of a car or electricity at the socket
* Useful (u) energy that satisfies useful energy demand for providing energy services, like heating, lighting or moving people

Technologies can take in from one level and put out at another level or on the same level. The energy forms defined in each level can be envisioned as a transfer hub, 
that the various technologies feed into or pump away from. The useful energy demand is given as a time series. Technologies can also vary per time period.

The mathematical formulation of MESSAGE ensures that the flows are consistent: demand is met, inflows equal outflows and constraints are not exceeded.

.. toctree::
   :maxdepth: 1

   resource/index
   conversion/index
   transport
   resid_commerc
   industrial
