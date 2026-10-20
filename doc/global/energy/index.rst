.. _message:

Energy (|MESSAGEix|)
********************

MESSAGE (short for Model for Energy Supply Strategy Alternatives and their General Environmental impact) is a generalized optimization
(specifically, linear programming, or LP)
energy systems engineering model
(Messner and Strubegger, 1995 :cite:`messner_users_1995`).
The `MESSAGEix <https://docs.messageix.org>`_ modeling framework
combines a concrete implementation of MESSAGE (in GAMS) with programming tools (in Python) for applications including medium- to long-term energy system planning,
energy policy analysis, and scenario development
(Huppmann et al., 2019 :cite:`huppmann_2019_messageix`).
The |MESSAGEix| framework allows to represent an energy system
with all its interdependencies,
from resource extraction,
imports and exports,
conversion,
transport,
and distribution,
to the provision of energy end-use services such as light,
space conditioning,
industrial production processes,
and transportation.


|name| links |MESSAGEix| to three further models.

* GLOBIOM (GLObal BIOsphere Management Model, cf. Section :ref:`globiom`)
  to consistently assess the implications of utilizing bioenergy of different types
  and to integrate the GHG emissions from energy and land use.
* GAINS (Greenhouse gas-Air pollution INteractions and Synergies, cf. Section :ref:`gains`)
  to represent non-CO2 greenhouse gases and air pollutants
  at the resolution at which their sources, control options and legislation are assessed.
* MACRO (cf. Section :ref:`macro`),
  an aggregated macro-economic model,
  to assess economic implications
  and to capture the feedbacks between the energy system and the demand for energy services.

|name| covers all greenhouse gas (GHG)-emitting sectors,
including energy,
industrial processes,
and, through its linkage to GLOBIOM, agriculture and forestry.
The full basket of greenhouse gases is represented,
with each gas entering the model through the component that resolves its sources.
CO2 and the F-gases
(CF4, C2F6, HFC125, HFC134a, HFC143a, HFC227ea, HFC245ca and SF6)
are accounted directly in |MESSAGEix|
from the activity of its technologies.
CH4 and N2O from the energy system, industrial processes, product use, waste and wastewater
are accounted in |MESSAGEix| using implied emission factors derived from GAINS,
together with the abatement options GAINS identifies for each source,
which enter the optimization as explicit technologies with their own costs
(cf. Section :ref:`gains`).
CO2, CH4 and N2O from agriculture, forestry and land-use change
come from GLOBIOM (cf. Section :ref:`globiom`).
Other radiatively active gases and air pollutants,
SO2, NOx, NH3, volatile organic compounds (VOCs), CO, and BC/OC,
are calculated in GAINS once the energy and land-use solution is complete,
from the scenario activity translated to the resolution of GAINS,
and aggregated back to the regions of |name|.
|name| is used in conjunction with MAGICC
(Model for Greenhouse gas Induced Climate Change)
version 7.6.0a3
(cf. Section :ref:`magicc`)
to calculate atmospheric concentrations,
radiative forcing,
and annual-mean global surface air temperature increase.

The model is designed to formulate and evaluate alternative energy supply strategies
subject to user-defined constraints
such as limits on capacity additions and activity,
fuel availability and trade,
environmental regulations and policies,
and diffusion rates of new technologies.
Environmental aspects can be analysed by accounting for the pollutants
each technology emits at any step of the energy supply chain,
and by limiting these emissions where required.
This helps to evaluate the impact of environmental regulations
on the development of the energy system.

Its principal results comprise, among others, estimates of technology-specific multi-sector response strategies for specific climate stabilization targets. By doing so,
the model identifies the least-cost portfolio of mitigation technologies. The choice of the individual mitigation options across gases and sectors is driven by the relative
economics of the abatement measures, assuming full temporal and spatial flexibility (i.e., emissions-reduction measures are assumed to occur when and where they are
cheapest to implement).

The Reference Energy System (RES) defines the full set of available energy conversion technologies
and the energy commodities that connect them.
:numref:`fig-res` gives an illustrative overview of its structure,
with technologies grouped by function at each step of the energy chain.
Within |MESSAGEix|, the term “energy conversion technology” refers to
all types of energy technologies,
from resource extraction to transformation,
transportation,
distribution of energy carriers,
and end-use technologies.

.. _fig-res:
.. figure:: /_static/RES_overview.png

   Illustrative overview of the Reference Energy System (RES) of |name|.
   Columns follow the energy chain from resources to demand.
   Blue boxes are energy commodities at the respective level,
   green boxes are groups of technologies,
   with the number of technologies in each group in parentheses.
   Grey lines connect each group to the commodities
   its technologies take as input or deliver as output.
   Individual technologies and their connections are not shown.

Because few conversion technologies convert resources directly into useful energy,
the energy system in |name| is divided into 5 energy levels:

* Resources: raw resources (e.g., coal, oil, natural gas in the ground or biomass on the field)
* Primary energy: raw product at a generation site (e.g., crude oil input to the refinery)
* Secondary energy: finalized product at a generation site (e.g., gasoline or diesel fuel output from the refinery)
* Final energy: finalized product at its consumption point (e.g., gasoline in the tank of a car or electricity leaving a socket)
* Useful energy (Demand): finalized product satisfying demand for services (e.g., heating, lighting or moving people)

Technologies can take in energy commodities from one level and put out at another level (e.g., refineries produce refined oil products at secondary level from crude oil at the primary level)
or at the same level (e.g., hydrogen electrolyzers produce hydrogen at the secondary energy level from electricity at the secondary level). The energy forms defined in each level can be
envisioned as a transfer hub, that the various technologies feed into or pump away from. The useful energy demand is given as a time series. Technology characteristics generally vary over time period.

The |MESSAGEix| mathematical formulation ensures that the flows are consistent:
demand is met,
inflows equal outflows,
and constraints are not exceeded.
In other words, |MESSAGEix| *on its own* is a partial equilibrium model.
However, through its linkage to MACRO,
general equilibrium effects are taken into account
(cf. Section :ref:`macro`).

.. toctree::
   :maxdepth: 1

   resource/index
   conversion/index
   enduse/index
   tech
   tech_addon
   fuel_blending
   demand
   policy
