.. _globiom:

Land-use (GLOBIOM)
=========
Land-use dynamics are modelled with the GLOBIOM (GLobal BIOsphere Management) model, which is a recursive-dynamic partial-equilibrium model (Havlík et al., 2011 :cite:`havlik_global_2011`; Havlík et al., 2014 :cite:`havlik_climate_2014`). GLOBIOM represents the competition between different land-use based activities. It includes a bottom-up representation of the agricultural, forestry and bio-energy sector, which allows for the inclusion of detailed grid-cell information on biophysical constraints and technological costs, as well as a rich set of environmental parameters, incl. comprehensive AFOLU (agriculture, forestry and other land use) GHG emission accounts and irrigation water use. Its spatial equilibrium modelling approach represents bilateral trade based on cost competitiveness. For spatially explicit projections of the change in afforestation, deforestation, forest management, and their related CO2 emissions, GLOBIOM is coupled with the G4M (Global FORest Model) model (Kindermann et al., 2006 :cite:`kindermann_predicting_2006`; Kindermann et al., 2008 :cite:`kindermann_global_2008`; Gusti, 2010 :cite:`gusti_algorithm_2010`). The spatially explicit G4M model compares the income of managed forest (difference of wood price and harvesting costs, income by storing carbon in forests) with income by alternative land use on the same place, and decides on afforestation, deforestation or alternative management options. As outputs, G4M provides estimates of forest area change, carbon uptake and release by forests, and supply of biomass for bioenergy and timber. (Fricko et al., 2016 :cite:`fricko_marker_2016`)

The model structure
----
We conduct our analysis using the Global Biosphere Management Model (GLOBIOM)  (Havlík, Schneider et al. 2011). GLOBIOM is a partial equilibrium model representing land-	use based activities: agriculture, forestry and bioenergy sectors. The model is built following a bottom-up setting based on detailed gridcell information, providing the biophysical and technical cost information. Production adjusts to meet the demand at the level of 30 economic regions (see list of the regions in Table S2). International trade representation is based on the spatial equilibrium modelling approach, where individual regions trade with each other based purely on cost competitiveness because goods are assumed to be homogenous (Takayama and Judge 1971, Schneider, McCarl et al. 2007). Market equilibrium is determined through mathematical optimization which allocates land and other resources to maximize the sum of consumer and producer surplus (McCarl and Spreen 1980 :cite:`mccarl_surplus_1980`). As in other partial equilibrium models, prices are endogenous. The model is run recursively dynamic with a 10 year time step, along a baseline going from 2000 to 2030. The model is solved using a linear programming simplex solver and can be run on a personal computer with the GAMS software.


.. _fig-globiom_land:

.. figure:: /_static/GLOBIOM_land_cover.png
   :width: 900px

   Land cover representation in GLOBIOM and the matrix of endogenous land cover change possibilities (Havlík et al., 2014 :cite:`havlik_climate_2014`).
   
A grid cell structure
----
All supply-side data are implemented in the model at the level of gridcell-based Simulation Units (Skalsky, Tarasovicova et al. 2008). In total, 212,707 Simulation Units are delineated by clustering 5 x 5 minutes of arc pixels according to five criteria: altitude, slope, and soil class, 0.5 x 0.5 degrees grid, and the country boundaries. For the present study, in order to ease computation time with the livestock module, the input datasets and the model resolution were agregated to 2 x 2 degree cells disaggregated only by country boundaries and by three agro-ecological zones used in the livestock production system classification: arid, humid, temperate and tropical highlands. This led to a total of 10,894 different Supply Units.

Agricultural production
----
GLOBIOM explicitly covers production of each of the 18 world major crops representing more than 70% of the total harvested area and 85% of the vegetal calorie supply as reported by FAOSTAT. Each crop can be produced under different management systems depending on their relative profitability: subsistence, low input rainfed, high input rainfed, and high input irrigated, when water resources are available. For each of the four systems, crop yields are calculated at the Simulation Unit level on the basis of soil, slope, altitude and climate information, using the EPIC model (Williams and Singh 1995). Within each management system, input structure is fixed following a Leontieff production function. But crop yields can change in reaction to external socio-economic drivers through switch to another management system or reallocation of the production to a more or less productive Supply Unit. Besides the endogennous mechanisms, an exogenous component representing longterm technological change is also considered. The livestock sector - the key component of this paper - is presented separately in section 2.

Forestry
----
The forestry sector is represented in GLOBIOM with five categories of primary products (pulp logs, saw logs, biomass for energy, traditional fuel wood, and other industrial logs) which are consumed by industrial energy, cooking fuel demand, or processed and sold on the market as final products (wood pulp and sawnwood). These products are supplied from managed forests and short rotation plantations. Harvesting cost and mean annual increments are informed by the G4M global forestry model (Kindermann, Obersteiner et al. 2006) which in turn calculates them based on thinning strategies and length of the rotation period.
