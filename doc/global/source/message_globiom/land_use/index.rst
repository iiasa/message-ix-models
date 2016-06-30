.. _globiom:

Land-use (GLOBIOM)
=========
Land-use dynamics are modelled with the GLOBIOM (GLobal BIOsphere Management) model, which is a recursive-dynamic partial-equilibrium model (Havlík et al., 2011 :cite:`havlik_global_2011`; Havlík et al., 2014 :cite:`havlik_climate_2014`). GLOBIOM represents the competition between different land-use based activities. It includes a bottom-up representation of the agricultural, forestry and bio-energy sector, which allows for the inclusion of detailed grid-cell information on biophysical constraints and technological costs, as well as a rich set of environmental parameters, incl. comprehensive AFOLU (agriculture, forestry and other land use) GHG emission accounts and irrigation water use. Its spatial equilibrium modelling approach represents bilateral trade based on cost competitiveness. For spatially explicit projections of the change in afforestation, deforestation, forest management, and their related CO2 emissions, GLOBIOM is coupled with the G4M (Global FORest Model) model (Kindermann et al., 2006 :cite:`kindermann_predicting_2006`; Kindermann et al., 2008 :cite:`kindermann_global_2008`; Gusti, 2010 :cite:`gusti_algorithm_2010`). The spatially explicit G4M model compares the income of managed forest (difference of wood price and harvesting costs, income by storing carbon in forests) with income by alternative land use on the same place, and decides on afforestation, deforestation or alternative management options. As outputs, G4M provides estimates of forest area change, carbon uptake and release by forests, and supply of biomass for bioenergy and timber. (Fricko et al., 2016 :cite:`fricko_marker_2016`)

Model structure
----
GLOBIOM is a partial equilibrium model representing land-use based activities: agriculture, forestry and bioenergy sectors. The model is built following a bottom-up setting based on detailed gridcell information, providing the biophysical and technical cost information. Production adjusts to meet the demand at the level of 30 economic regions (see list of the regions in Table S2). International trade representation is based on the spatial equilibrium modelling approach, where individual regions trade with each other based purely on cost competitiveness because goods are assumed to be homogenous (Takayama and Judge 1971 :cite:`takayama_spatial_1971`; Schneider, McCarl et al. 2007 :cite:`schneider_agricultural_2007`). Market equilibrium is determined through mathematical optimization which allocates land and other resources to maximize the sum of consumer and producer surplus (McCarl and Spreen 1980 :cite:`mccarl_surplus_1980`). As in other partial equilibrium models, prices are endogenous. The model is run recursively dynamic with a 10 year time step, along a baseline going from 2000 to 2100. The model is solved using a linear programming simplex solver and can be run on a personal computer with the GAMS software.
   
A grid cell structure
----
All supply-side data are implemented in the model at the level of gridcell-based Simulation Units (Skalsky, Tarasovicova et al. 2008 :cite:`skalsky_geo-bene_2008`). In total, 212,707 Simulation Units are delineated by clustering 5 x 5 minutes of arc pixels according to five criteria: altitude, slope, and soil class, 0.5 x 0.5 degrees grid, and the country boundaries. For the present study, in order to ease computation time with the livestock module, the input datasets and the model resolution were agregated to 2 x 2 degree cells disaggregated only by country boundaries and by three agro-ecological zones used in the livestock production system classification: arid, humid, temperate and tropical highlands. This led to a total of 10,894 different Supply Units.

Model Details
-------------

.. toctree::
   :maxdepth: 1

   crop
   livestock
   forest
   land use

Land use change
----
The model optimizes over six land cover types: cropland, grassland, short rotation plantations, managed forests, unmanaged forests and other natural land. Economic activities are associated with the first four land cover types. There are other three land cover types represented in the model: other agricultural land, wetlands, and not relevant (bare areas, water bodies, snow and ice, and artificial surfaces). These three categories are currently kept constant. Each Simulation Unit can contain the nine land cover types. The base year spatial distribution of land cover is based on the Global Land Cover 2000 (GLC2000). However, as any other global dataset of this type, GLC2000 suffers from large uncertainty (Fritz, See et al. 2011 :cite:`fritz_highlighting_2011`). Therefore auxiliary datasets and procedures are used to transform this “raw” data into a consistent dataset corresponding to the model needs. An example of such a transformation is presented in section 2.4 concerning grasslands.

.. _fig-globiom_land:

.. figure:: /_static/GLOBIOM_land_cover.png
   :width: 900px

   Land cover representation in GLOBIOM and the matrix of endogenous land cover change possibilities (Havlík et al., 2014 :cite:`havlik_climate_2014`).

Land conversion over the simulation period is endogenously determined for each Supply Unit within the available land resources. Such conversion implies a conversion cost – increasing with the area of land converted - that is taken into account in the producer optimization behavior. Land conversion possibilities are further restricted through biophysical land suitability and production potentials, and through a matrix of potential land cover transitions (Fig. 1). 

Food demand
----
Food demand is in GLOBIOM endogenous and depends on population, gross domestic product (GDP) and own produt price. Population and GDP are exogenous variables while prices are endogenous. The simple demand system is presented in Eq. 1. First, for each product i in region r and period t,  the prior demand quantity Q ̅ is calculated as a function of population POP, GDP per capita 〖GDP〗^cap adjusted by the income elasticity ε^GDP, and the base year consumption level as reported in the Food Balance Sheets of FAOSTAT. If the prior demand quantity could be satisfied at the base year price P ̅, this would be also the optimal demand quantity Q. However, usually the optimal quantity will be different from the prior quantity, and will depend on the optimal price P and the price elasticity ε^price, the latter calculated from USDA (Seale, Regmi et al. 2003, :cite:`seale_international_2003`), updated in (Muhammad, Seale et al. 2011, :cite:`muhammad_international_2011`) for the base year 2000. Because food demand in developed countries is more inelastic than in developing ones, the value of this elasticity is assumed to decrease with the level of GDP per capita. The rule we apply is that the price elasticity of developing countries converges to the price elasticity of the USA in 2000 at the same pace as their GDP per capita reach the USA GDP per capita value of 2000. This allows us to capture the effect of change in relative prices on food consumption taking into account heterogeneity of responses across regions, products and over time.

Our demand function has the virtue of being easy to linearize which allows us to solve GLOBIOM as a linear program. This is currently necessary because of the size of the model and the current performance of non-linear solvers. However, this demand function has although some limitations which need to be kept in mind when considering the results obtained with respect to climate change mitigation and food availability. One of them is that we do not consider direct substitution effects on the consumer side which could be captured through cross price demand elasticities. Such a demand representation could lead to increased consumption of some products like legumes or cereals when prices of GHG intensive products like rice or beef would go up as a consequence of a carbon price targeting emissions for the agricultural sector. Neglecting the direct substitution effects may lead to an overestimation of the negative impact of such mitigation policies on total food consumption. However, the effect on emissions would be only of second order, because consumption would increase for commodities the least affected by the carbon price, and hence the least emission intensive. Although we do not represent the direct substitution effects on the demand side, substitution can still occur due to changes in prices on the supply side and can in some cases lead to a partial compensation of the decreased demand for commodities affected the most by a mitigation policy. This phenomenon can be observed in our results for mitigation policies targeting the livestock sector only (Fig. 4. In the main text). 

