.. _globiom:

Land-use (GLOBIOM)
=========
Land-use dynamics are modelled with the GLOBIOM (GLobal BIOsphere Management) model, which is a recursive-dynamic partial-equilibrium model (Havlík et al., 2011 :cite:`havlik_global_2011`; Havlík et al., 2014 :cite:`havlik_climate_2014`). GLOBIOM represents the competition between different land-use based activities. It includes a bottom-up representation of the agricultural, forestry and bio-energy sector, which allows for the inclusion of detailed grid-cell information on biophysical constraints and technological costs, as well as a rich set of environmental parameters, incl. comprehensive AFOLU (agriculture, forestry and other land use) GHG emission accounts and irrigation water use. Its spatial equilibrium modelling approach represents bilateral trade based on cost competitiveness. For spatially explicit projections of the change in afforestation, deforestation, forest management, and their related CO2 emissions, GLOBIOM is coupled with the G4M (Global FORest Model) model (Kindermann et al., 2006 :cite:`kindermann_predicting_2006`; Kindermann et al., 2008 :cite:`kindermann_global_2008`; Gusti, 2010 :cite:`gusti_algorithm_2010`). The spatially explicit G4M model compares the income of managed forest (difference of wood price and harvesting costs, income by storing carbon in forests) with income by alternative land use on the same place, and decides on afforestation, deforestation or alternative management options. As outputs, G4M provides estimates of forest area change, carbon uptake and release by forests, and supply of biomass for bioenergy and timber. (Fricko et al., 2016 :cite:`fricko_marker_2016`)

A grid cell structure
----
All supply-side data are implemented in the model at the level of gridcell-based Simulation Units (SimU) (Skalsky, Tarasovicova et al. 2008 :cite:`skalsky_geo-bene_2008`). In total, 212,707 SimU are delineated by clustering 5 x 5 minutes of arc pixels according to five criteria: altitude, slope, and soil class, 0.5 x 0.5 degrees grid, and the country boundaries. For the present study, in order to ease computation time with the livestock module, the input datasets and the model resolution were agregated to 2 x 2 degree cells disaggregated only by country boundaries and by three agro-ecological zones used in the livestock production system classification: arid, humid, temperate and tropical highlands. This led to a total of 10,894 different Supply Units.

Land resources and their characteristics are the fundamental elements of our modelling approach. In order to enable global bio-physical process modelling of agricultural and forest production, a comprehensive database has been built (Skalský et al., 2008), which contains geo-spatial data on soil, climate/weather, topography, land cover/use, and crop management (e.g. fertilization, irrigation). The data were compiled from various sources (FAO, ISRIC, USGS, NASA, CRU UEA, JRC, IFRPI, IFA, WISE, etc.) and significantly vary with respect to spatial, temporal, and attribute resolutions, thematic relevance, accuracy, and reliability. Therefore, data were harmonized into several common spatial resolution layers including 5 and 30 Arcmin as well as country layers. Subsequently, Homogeneous Response Units (HRU) have been delineated by geographically clustering according to only those parameters of the landscape, which are generally not changing over time and are thus invariant with respect to land use and management or climate change. At the global scale, we have included five altitude classes, seven slope classes, and five soil classes. In a second step, the HRU layer is intersected with a 0.5° × 0.5° grid and country boundaries to delineate SimUs which contain other relevant information such as global climate data, land category/use data, irrigation data, etc. For each SimU a number of land management options are simulated using the bio-physical process model EPIC (Environmental Policy Integrated Climate Model; Izaurralde et al., 2006 :cite:`izaurralde_simulating_2006`; Williams, 1995). And the SimUs are the basis for estimation of land use/management parameters in all other supporting models as well. 
The HRU concept assures consistent aggregation of geo-spatially explicit bio-physical impacts in the economic land use assessment. In GLOBIOM, we can choose at which level of resolution the model is run, and aggregate the inputs consistently. As shown in the Appendix, each land related activity and all land resources are currently indexed by country, altitude, slope, and soil class. The information relevant to the 0.5° × 0.5° grid layer has been averaged to keep the model size and computational time within reasonable limits.

Model structure
----
GLOBIOM is a partial equilibrium model representing land-use based activities: agriculture, forestry and bioenergy sectors. The model is built following a bottom-up setting based on detailed gridcell information, providing the biophysical and technical cost information. Production adjusts to meet the demand at the level of 30 economic regions (see list of the regions in Table S2). International trade representation is based on the spatial equilibrium modelling approach, where individual regions trade with each other based purely on cost competitiveness because goods are assumed to be homogenous (Takayama and Judge 1971 :cite:`takayama_spatial_1971`; Schneider, McCarl et al. 2007 :cite:`schneider_agricultural_2007`). Market equilibrium is determined through mathematical optimization which allocates land and other resources to maximize the sum of consumer and producer surplus (McCarl and Spreen 1980 :cite:`mccarl_surplus_1980`). As in other partial equilibrium models, prices are endogenous. The model is run recursively dynamic with a 10 year time step, along a baseline going from 2000 to 2100. The model is solved using a linear programming simplex solver and can be run on a personal computer with the GAMS software.

.. _fig-landuse_product_structure:
.. figure:: /_static/GLOBIOM_land_use_product_structure.png
   :width: 800px
   
   GLOBIOM land use and product structure. 

Model Details
-------------

.. toctree::
   :maxdepth: 1

   crop
   livestock
   forest
   land 
   food 




