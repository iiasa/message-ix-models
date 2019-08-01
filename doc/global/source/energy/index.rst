.. _message:

Energy (MESSAGE)
==========
The `MESSAGEix <https://message.iiasa.ac.at>`_ modeling framework, briefly known as MESSAGE (Model for Energy Supply Strategy Alternatives and their General Environmental Impact), is a linear programming (LP) energy engineering model with global coverage. 
As a systems engineering optimization model, MESSAGE is primarily used for medium- to long-term energy system planning, energy policy analysis, and scenario development 
(Huppmann et al., 2019 :cite:`huppmann_message_2019`; Messner and Strubegger, 1995 :cite:`messner_users_1995`). The model provides a framework for representing an energy system with all its interdependencies from resource 
extraction, imports and exports, conversion, transport, and distribution, to the provision of energy end-use services such as light, space conditioning, industrial 
production processes, and transportation. In addition, MESSAGE links to GLOBIOM (GLObal BIOsphere Model, cf. Section :ref:`globiom`) to consistently assess the implications 
of utilizing bioenergy of different types and to integrate the GHG emissions from energy and land use and to the aggregated macro-economic model MACRO (cf. Section :ref:`macro`)
to assess economic implications and to capture economic feedbacks.

MESSAGE covers all greenhouse gas (GHG)-emitting sectors, including energy, industrial processes as well as - through its linkage to GLOBIOM - agriculture and forestry. 
The emissions of the full basket of greenhouse gases including CO2, CH4, N2O and F-gases (CF4, C2F6, HFC125, HFC134a, HFC143a, HFC227ea, HFC245ca and SF6) as well as other radiatively 
active gases, such as NOx, volatile organic compounds (VOCs), CO, SO2, and BC/OC is represented in hte model. MESSAGE is used in conjunction with MAGICC (Model for Greenhouse gas 
Induced Climate Change) version 6.8 (cf. Section :ref:`magicc`) for calculating atmospheric concentrations, radiative forcing, and annual-mean global surface air temperature increase.

The model is designed to formulate and evaluate alternative energy supply strategies consonant with the user-defined constraints such as limits on new investment, fuel 
availability and trade, environmental regulations and policies as well as diffusion rates of new technologies. Environmental aspects can be analysed by accounting, and if necessary 
limiting, the amounts of pollutants emitted by various technologies at various steps in energy supplies. This helps to evaluate the impact of environmental regulations 
on energy system development.

It's principal results comprise, among others, estimates of technology-specific multi-sector response strategies for specific climate stabilization targets. By doing so, 
the model identifies the least-cost portfolio of mitigation technologies. The choice of the individual mitigation options across gases and sectors is driven by the relative 
economics of the abatement measures, assuming full temporal and spatial flexibility (i.e., emissions-reduction measures are assumed to occur when and where they are 
cheapest to implement).

The Reference Energy System (RES) defines the full set of available energy conversion technologies. In MESSAGE terms, energy conversion technology refers to all types 
of energy technologies from resource extraction to transformation, transport, distribution of energy carriers, and end-use technologies.

Because few conversion technologies convert resources directly into useful energy, the energy system in MESSAGE is divided into 5 energy levels:

* Resources: raw resources (e.g., coal, oil, natural gas in the ground or biomass on the field)
* Primary energy: raw product at a generation site (e.g., crude oil input to the refinery)
* Secondary energy: finalized product at a generation site (e.g., gasoline or diesel fuel output from the refinery)
* Final energy: finalized product at its consumption point (e.g., gasoline in the tank of a car or electricity leaving a socket)
* Useful energy: finalized product satisfying demand for services (e.g., heating, lighting or moving people)

Technologies can take in energy commodities from one level and put out at another level (e.g., refineries produce refinded oil products at secondary level from crude oil at the primary level) 
or at the same level (e.g., hydrogen electrolyzers produce hydrogen at the secondary energy level from electricity at the secondary level). The energy forms defined in each level can be 
envisioned as a transfer hub, that the various technologies feed into or pump away from. The useful energy demand is given as a time series. Technology characteristics generally vary over time period.

The mathematical formulation of MESSAGE ensures that the flows are consistent: demand is met, inflows equal outflows and constraints are not exceeded. In other words, MESSAGE itself is a partial
equilibrium model. However, through its linkage to MACRO general equilibrium effects are taken into account (cf. Section :ref:`macro`).

.. toctree::
   :maxdepth: 1

   resource/index
   conversion/index
   enduse/index
   tech
   tech_addon
   demand

