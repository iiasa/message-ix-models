.. _overview:

Overview
==============
The IIASA IAM framework consists of a combination of five different models or modules - the energy model MESSAGE, the land use model GLOBIOM, the air pollution and GHG  model GAINS, the aggregated macro-economic model MACRO and the simple climate model MAGICC - which complement each other and are specialized in different areas. All models and modules together build the IIASA IAM framework, also referred to as MESSAGE-GLOBIOM owing to the fact that the energy model MESSAGE and the land use model GLOBIOM are its central components. The five models provide input to and iterate between each other during a typical scenario development cycle. Below is a brief overview of how the models interact with each other, specifically in the context of developing the SSP scenarios.

MESSAGE (Huppmann et al., 2019 :cite:`huppmann_2019_messageix`) represents the core of the IIASA IAM framework (:numref:`fig-iiasaiam`) and its main task is to optimize the energy system so that it can satisfy specified energy demands at the lowest costs. MESSAGE carries out this optimization in an iterative setup with MACRO, a single sector macro-economic model, which provides estimates of the macro-economic demand response that results from energy system and services costs computed by MESSAGE. For the six commercial end-use demand categories depicted in MESSAGE (see :ref:`demand`), based on demand prices MACRO will adjust useful energy demands, until the two models have reached equilibrium (see :ref:`macro`). This iteration reflects price-induced energy efficiency adjustements that can occur when energy prices change.

GLOBIOM provides MESSAGE with information on land use and its implications, including the availability and cost of bioenergy, and availability and cost of emission mitigation in the AFOLU (Agriculture, Forestry and Other Land Use) sector (see :ref:`globiom`). To reduce computational costs, MESSAGE iteratively queries a GLOBIOM emulator which provides an approximation of land-use outcomes during the optimization process instead of requiring the GLOBIOM model to be rerun iteratively. Only once the iteration between MESSAGE and MACRO has converged, the resulting bioenergy demands along with corresponding carbon prices are used for a concluding analysis with the full-fledged GLOBIOM model. This ensures full consistency of the results from MESSAGE and GLOBIOM, and also allows producing a more extensive set of land-use related indicators, including spatially explicit information on land use.

Air pollution implications of the energy system are accounted for in MESSAGE by applying technology-specific air pollution coefficients derived from the GAINS model (see :ref:`gains`). This approach has been applied the SSP process (Rao et al., 2017 :cite:`rao_2017_SSP_airpollution`). Alternatively, GAINS can be run ex-post based on MESSAGEix-GLOBIOM scenarios to estimate air pollution emissions, concentrations and the related health impacts. This approach allows analyzing different air pollution policy packages (e.g., current legislation, maximum feasible reduction), including the estimation of costs for air pollution control measures. Examples for applying this way of linking MESSAGEix-GLOBIOM and GAINS can be found in McCollum et al. (2018 :cite:`mccollum_2018_investment`) and Grubler et al. (2018 :cite:`grubler_2018_led`).

In general, cumulative global carbon emissions from all sectors are constrained at different levels, with equivalent pricing applied to other GHGs, to reach the desired radiative forcing levels (cf. right-hand side :numref:`fig-iiasaiam`). The climate constraints are thus taken up in the coupled MESSAGE-GLOBIOM optimization, and the resulting carbon price is fed back to the full-fledged GLOBIOM model for full consistency. Finally, the combined results for land use, energy, and industrial emissions from MESSAGE and GLOBIOM are merged and fed into MAGICC (see :ref:`magicc`), a global carbon-cycle and climate model, which then provides estimates of the climate implications in terms of atmospheric concentrations, radiative forcing, and global-mean temperature increase. Importantly, climate impacts and impacts of the carbon cycle are -- depending on the specific application -- currently only partly accounted for in the IIASA IAM framework. The entire framework is linked to an online database infrastructure which allows straightforward visualisation, analysis, comparison and dissemination of results (Riahi et al., 2017 :cite:`riahi_shared_2017`).

The scientific software underlying the global MESSAGE-GLOBIOM model is called the |MESSAGEix| framework, an open-source, versatile implementation of a linear optimization problem, with the option of coupling to the computable general equilibrium (CGE) model MACRO to incorporate the effect of price changes on economic activity and demand for commodities and resources. |MESSAGEix| is integrated with the *ix modeling platform* (ixmp), a "data warehouse" for version control of reference timeseries, input data and model results. ixmp provides interfaces to the scientific programming languages Python and R for efficient, scripted workflows for data processing and visualisation of results (Huppmann et al., 2019 :cite:`huppmann_2019_messageix`).

.. _fig-iiasaiam:

.. figure:: /_static/iiasaiam.png
   :width: 900px

   Overview of the IIASA IAM framework. Coloured boxes represent respective specialized disciplinary models which are integrated for generating internally consistent scenarios (Fricko et al., 2017 :cite:`fricko_marker_2017`). 

.. toctree::
   :maxdepth: 1

   spatial
   temporal

.. |MESSAGEix| replace:: MESSAGE\ :emphasis:`ix`
