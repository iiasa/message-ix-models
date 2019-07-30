.. _crop:

Crop production
----

GLOBIOM directly represents production from three major land cover types: cropland, managed forest, and areas suitable for short rotation tree plantations. Crop production accounts for more than 30 of the 
globally most important crops. The average yield level for each crop in each country is taken from FAOSTAT. Management related yield coefficients according to fertilizer and irrigation rates are explicitly 
simulated with the EPIC model (Williams and Singh, 1995 :cite:`williams_computer_1995`) for 17 crops (barley, dry beans, cassava, chickpea, corn, cotton, ground nuts, millet, potatoes, rapeseed, rice, soybeans, 
sorghum, sugarcane, sunflower, sweet potatoes, and wheat). These 17 crops together represent nearly 80 % of the 2007 harvested area and 85% of the vegetal calorie supply as reported by FAOSTAT. Four management 
systems are considered (irrigated, high input - rainfed, low input - rainfed and subsistence management systems) corresponding to the International Food and Policy Research Institute (IFPRI) crop distribution data 
classification (You and Wood, 2006 :cite:`you_entropy_2006`). Within each management system, input structure is fixed following a Leontieff production function. But crop yields can change in reaction to external 
socio-economic drivers through switch to another management system or reallocation of the production to a more or less productive Supply Unit. 

Besides the endogennous mechanisms, an exogenous component representing 
long-term technological change is also considered. Only two management systems are differentiated for the remaining crops (bananas, other dry beans, coconuts, coffee, lentils, mustard seed, olives, oil palm, plantains, 
peas, other pulses, sesame seed, sugar beet, and yams) – rainfed and irrigated. Rainfed and irrigated crop yield coefficients, and crop specific irrigation water requirements for crops not simulated with EPIC, 
and costs for four irrigation systems for all crops, are derived from a variety of sources as described in Sauer et al. (2008) :cite:`sauer_agriculture_2008`. Crop supply can enter one of three processing/demand 
channels: consumption, livestock production and biofuel production (see :numref:`fig-landuse_product_structure`). 
