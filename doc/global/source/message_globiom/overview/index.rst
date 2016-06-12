.. _overview:

Overview
==============
The IIASA IAM framework consists of a combination of five different models or modules - the energy model MESSAGE, the land use model GLOBIOM, the air pollution and GHG 
model GAINS, the aggregated macro-economic model MACRO and the simple climate model MAGICC - which complement each other and are specialized in different areas. 
All models and modules together build the IIASA IAM framework, also referred to as MESSAGE-GLOBIOM owing to the fact that the energy model MESSAGE and the land use model 
GLOBIOM are its most important components. The five models provide input to and iterate between each other during a typical SSP scenario development cycle. 
Below we provide a brief overview of how the models interact and describe which further steps are taken within the IIASA IAM framework to develop an SSP scenario.  

MESSAGE represents the core of the IIASA IAM framework (:numref:`fig-iiasaiam`) and its main task is to optimize the energy system so that it can satisfy specified 
energy demands at the lowest costs. MESSAGE carries out this optimization in an iterative setup with MACRO, which provides estimates of the macro-economic demand response 
that results of energy system and services costs computed by MESSAGE. For the six commercial end-use demand categories depicted in MESSAGE (see :ref:`demand`), MACRO 
will adjust useful energy demands, until the two models have reached equilibrium (see :ref:`macro`). This iteration reflects price-induced energy efficiency improvements 
that can occur when energy prices increase.

GLOBIOM provides MESSAGE with information on land use and its implications, like the availability and cost of bio-energy, and availability and cost of emission mitigation 
in the AFOLU (Agriculture, Forestry and Land Use) sector (see :ref:`globiom`). To reduce computational costs, MESSAGE iteratively queries a GLOBIOM emulator which can 
provide possible land-use outcomes during the optimization process instead of requiring the GLOBIOM model to be rerun continuously. Only once the iteration between 
MESSAGE and MACRO has converged, the resulting bioenergy demands along with corresponding carbon prices are used for a concluding online analysis with the full-fledged 
GLOBIOM model. This ensures full consistency in the modelled results from MESSAGE and GLOBIOM, and also allows the production of a more extensive set of reporting 
variables.

Air pollution implications of the energy system are computed in MESSAGE by applying technology-specific pollution coefficients from GAINS (see :ref:`gains`).

In general, cumulative global GHG emissions from all sectors are constrained at different levels to reach the forcing levels (cf. right-hand side :numref:`fig-iiasaiam`). 
The climate constraints are thus taken up in the coupled MESSAGE-GLOBIOM optimization, and the resulting carbon price is fed back to the full-fledged GLOBIOM model for 
full consistency. Finally, the combined results for land use, energy, and industrial emissions from MESSAGE and GLOBIOM are merged and fed into MAGICC (see :ref:`magicc`), 
a global carbon-cycle and climate model, which then provides estimates of the climate implications in terms of atmospheric concentrations, radiative forcing, and 
global-mean temperature increase. Importantly, climate impacts and impacts of the carbon cycle are currently not accounted for in the IIASA IAM framework. The entire 
framework is linked to an online database infrastructure which allows straightforward visualisation, analysis, comparison and dissemination of results
(Fricko et al., 2016 :cite:`fricko_marker_2016`).

.. _fig-iiasaiam:

.. figure:: /_static/iiasaiam.png
   :width: 900px

   Overview of the IIASA IAM framework. Coloured boxes represent respective specialized disciplinary models which are integrated for generating internally consistent scenarios (`Riahi et al., 2016 <http://pure.iiasa.ac.at/13280/>`_ :cite:`riahi_shared_2016`). 

.. toctree::
   :maxdepth: 1

   spatial
   temporal
