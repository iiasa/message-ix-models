Overview
==============
MESSAGE (Model for Energy Supply Strategy Alternatives and their General Environmental Impact) is a hybrid energy-economic model that combines a linear programming (LP) energy engineering model with an aggregated macro-economic model (MACRO) which is solved by using a non-linear program (NLP). At its core is a global systems engineering optimization model used for medium- to long-term energy system planning, energy policy analysis, and scenario development (Messner and Strubegger, 1995 :cite:`messner_users_1995`). The model provides a framework for representing an energy system with all its interdependencies from resource extraction, imports and exports, conversion, transport, and distribution, to the provision of energy end-use services such as light, space conditioning, industrial production processes, and transportation. In addition, MESSAGE links to GLOBIOM (GLObal BIOsphere Model) to consistently assess the implications of utilizing bioenergy of different types and to integrate the GHG emissions from energy and land use.

MESSAGE covers all greenhouse gas (GHG)-emitting sectors, including energy, industrial processes as well as - through its linkage to GLOBIOM - agriculture and forestry, for a full basket of greenhouse gases and other radiatively active gases, including CO2, CH4 ,N2O, NOx, volatile organic compounds (VOCs), CO, SO2, BC/OC, CF4, C2F6, HFC125, HFC134a, HFC143a, HFC227ea, HFC245ca and SF6. MESSAGE is used in conjunction with MAGICC (Model for Greenhouse gas Induced Climate Change) version 6.8 for calculating internally consistent scenarios for atmospheric concentrations, radiative forcing, annual-mean global surface air temperature and global-mean sea level implications.

The model is designed to formulate and evaluate alternative energy supply strategies consonant with the user-defined constraints such as limits on new investment, fuel availability and trade, environmental regulations and market penetration rates for new technologies. Environmental aspects can be analysed by accounting, and if necessary limiting, the amounts of pollutants emitted by various technologies at various steps in energy supplies. This helps to evaluate the impact of environmental regulations on energy system development.

It's principal results comprise, among others, estimates of technology-specific multi-sector response strategies for specific climate stabilization targets. By doing so, the model identifies the least-cost portfolio of mitigation technologies. The choice of the individual mitigation options across gases and sectors is driven by the relative economics of the abatement measures, assuming full temporal and spatial flexibility (i.e., emissions-reduction measures are assumed to occur when and where they are cheapest to implement).

MESSAGE represents the core of the IIASA IAM framework (:numref:`fig-iiasaiam`) and its main task is to optimize the energy system so that it can satisfy specified energy demands at the lowest costs. MESSAGE carries out this optimization in an iterative setup with MACRO, which provides estimates of the macro-economic demand response that results of energy system and services costs computed by MESSAGE. For the six commercial end-use demand categories depicted in MESSAGE (see :ref:`demand`), MACRO will adjust useful energy demands, until the two models have reached equilibrium (see :ref:`macro`). This iteration reflects price-induced energy efficiency improvements that can occur when energy prices increase.

GLOBIOM provides MESSAGE with information on land use and its implications, like the availability and cost of bio-energy, and availability and cost of emission mitigation in the AFOLU (Agriculture, Forestry and Land Use) sector (see :ref:`globiom`). To reduce computational costs, MESSAGE iteratively queries a GLOBIOM emulator which can provide possible land-use outcomes during the optimization process instead of requiring the GLOBIOM model to be rerun continuously. Only once the iteration between MESSAGE and MACRO has converged, the resulting bioenergy demands along with corresponding carbon prices are used for a concluding online analysis with the full-fledged GLOBIOM model. This ensures full consistency in the modelled results from MESSAGE and GLOBIOM, and also allows the production of a more extensive set of reporting variables.

Air pollution implications of the energy system are computed in MESSAGE by applying technology-specific pollution coefficients from GAINS (see :ref:`gains`).

In general, cumulative global GHG emissions from all sectors are constrained at different levels to reach the forcing levels (cf. right-hand side :numref:`fig-iiasaiam`). The climate constraints are thus taken up in the coupled MESSAGE-GLOBIOM optimization, and the resulting carbon price is fed back to the full-fledged GLOBIOM model for full consistency. Finally, the combined results for land use, energy, and industrial emissions from MESSAGE and GLOBIOM are merged and fed into MAGICC (see :ref:`magicc`), a global carbon-cycle and climate model, which then provides estimates of the climate implications in terms of atmospheric concentrations, radiative forcing, and global-mean temperature increase. Importantly, climate impacts and impacts of the carbon cycle are currently not accounted for in the IIASA IAM framework. The entire framework is linked to an online database infrastructure which allows straightforward visualisation, analysis, comparison and dissemination of results. (Fricko et al., 2016 :cite:`fricko_marker_2016`)

.. _fig-iiasaiam:

.. figure:: /_static/iiasaiam.png
   :width: 900px

   Overview of the IIASA IAM framework. Coloured boxes represent respective specialized disciplinary models which are integrated for generating internally consistent scenarios (`Riahi et al., 2016 <http://pure.iiasa.ac.at/13280/>`_ :cite:`riahi_shared_2016`). 

.. toctree::
   :maxdepth: 1

   spatial
   temporal
