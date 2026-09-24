.. _globiom:

Land use (GLOBIOM-G4M)
======================
Global land cover,
land-use change,
and the production activities of the agriculture, forestry and other land use (AFOLU) sector
are represented in |name| by the coupled GLOBIOM-G4M land-use modelling framework
(Frank et al., 2021 :cite:`frank_land_2021`).
The framework is spatially explicit,
links land cover and land-based production endogenously,
and is simulated dynamically.
It also provides the emission abatement potential analysis
in the 2025 edition of the OECD-FAO Agricultural Outlook
(OECD and FAO, 2025 :cite:`oecd_fao_agricultural_2025`).

GLOBIOM (Global Biosphere Management Model)
(IBF-IIASA, 2023 :cite:`ibf_globiom_2023`;
Havlik et al., 2011 :cite:`havlik_global_2011`;
Havlik et al., 2014 :cite:`havlik_climate_2014`)
is a global, spatially explicit, partial equilibrium model of the AFOLU sector.
It represents the competition between land-based activities,
including crop production, livestock systems, forestry and bioenergy
(see :numref:`fig-landuse_product_structure`),
and combines biophysical constraints and technological costs,
resolved on the spatial units described in :ref:`globiom-spatial`,
with a rich set of environmental parameters,
including comprehensive AFOLU greenhouse gas emission accounts, irrigation water use,
and indicators of biodiversity, nitrogen flows and food security
(see :ref:`globiom-indicators`).
The model simulates the market equilibrium for agricultural and forest products
by allocating land and production activities
so as to maximize the total economic surplus of producers and consumers,
subject to resource, technological and policy constraints
(McCarl and Spreen, 1980 :cite:`mccarl_surplus_1980`).
Combining economic parameters with biophysical conditions,
the model captures how economic incentives determine production patterns,
demand levels,
land use and land-use change (see :ref:`landuse`),
and international trade flows.

Production adjusts to meet demand (see :ref:`globiom-demand`)
at the level of the 59 native GLOBIOM regions,
which are aggregated to the 12 |name| regions in the linkage.
Both resolutions are set out in :ref:`spatial`.
International trade is represented with the spatial equilibrium modelling approach,
where individual regions trade with each other purely on cost competitiveness
because goods are assumed to be homogeneous
(Takayama and Judge, 1971 :cite:`takayama_spatial_1971`;
Schneider, McCarl et al., 2007 :cite:`schneider_agricultural_2007`).
As in other partial equilibrium models, prices are endogenous.
The model is run recursively dynamic with a 10 year time step, going from 2000 to 2100.
It is solved using a linear programming solver
and can be run on a personal computer with the GAMS software.

.. _fig-landuse_product_structure:
.. figure:: /_static/GLOBIOM_chart_hires.jpg
   :width: 800px

   GLOBIOM land use and product structure.

GLOBIOM results are complemented by G4M (Global Forest Model)
(Gusti, 2010 :cite:`gusti_algorithm_2010`;
Gusti and Kindermann, 2011 :cite:`gusti_approach_2011`;
Kindermann et al., 2006 :cite:`kindermann_predicting_2006`;
Kindermann et al., 2008 :cite:`kindermann_global_2008`),
a spatially explicit intertemporal optimization model of forest management and land-use change
that operates on a 0.5 by 0.5 degree grid globally.
G4M integrates four thematic components,
namely the representation of environmental conditions and forest parameters,
the local economic assessment of wood and agricultural land values,
the decision on forest management and land-use change,
and the estimation of CO2 emissions and removals from forestry activities.
For each location it compares the income from forest,
the difference between wood price and harvesting costs
together with the income from storing carbon in forests,
with the income from alternative land use at the same location,
and decides on afforestation, deforestation or alternative management options.

The two models are linked through a downscaling module,
which brings GLOBIOM land cover results from the spatial units of the economic model to the G4M grid.
These,
together with regional results for wood prices, agricultural land rents, wood demand and carbon prices,
form the key scenario inputs to G4M.
G4M then solves a perfect foresight optimization of global net present value
while meeting the wood harvest demand
that follows from the GLOBIOM economic optimization and mass balance simulation.
Its output covers forest-related land-use change,
that is afforestation, deforestation, and conversion between managed and unmanaged forest,
detailed forest management options such as management type and rotation length,
forest-related carbon emissions and sinks,
and the supply of biomass for bioenergy and timber.
These complement the land-use dynamics and greenhouse gas results of GLOBIOM,
in particular the afforestation and forest related emissions that GLOBIOM does not model.

Within |name|, the land-use framework enters the energy system optimization as an emulator.
GLOBIOM and G4M are run beforehand over a grid of biomass prices and greenhouse gas prices,
and the resulting cases supply |MESSAGEix|
with bioenergy supply cost curves
and with marginal abatement cost curves for AFOLU emissions.
A full GLOBIOM-G4M run is carried out for every scenario in addition,
and its results replace the emulated land-use results in the scenario output.
The emulator is described in :doc:`emulator`.

.. toctree::
   :maxdepth: 1

   spatial
   crop
   livestock
   forest
   land
   food
   indicators
   emulator

