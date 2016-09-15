.. _landuse:

Land use change
---------------
The model optimizes over six land cover types: cropland, grassland, short rotation plantations, managed forests, unmanaged forests and other natural land. Economic activities are associated with the first four land cover types. There are other three land cover types represented in the model: other agricultural land, wetlands, and not relevant (bare areas, water bodies, snow and ice, and artificial surfaces). These three categories are currently kept constant. Each Simulation Unit can contain the nine land cover types. The base year spatial distribution of land cover is based on the Global Land Cover 2000 (GLC2000). However, as any other global dataset of this type, GLC2000 suffers from large uncertainty (Fritz, See et al. 2011 :cite:`fritz_highlighting_2011`). Therefore auxiliary datasets and procedures are used to transform this “raw” data into a consistent dataset corresponding to the model needs.

.. _fig-globiom_land:

.. figure:: /_static/GLOBIOM_land_cover.png
   :width: 900px

   Land cover representation in GLOBIOM and the matrix of endogenous land cover change possibilities (Havlik et al., 2014 :cite:`havlik_climate_2014`).

Land conversion over the simulation period is endogenously determined for each Supply Unit within the available land resources. Such conversion implies a conversion cost – increasing with the area of land converted - that is taken into account in the producer optimization behavior. Land conversion possibilities are further restricted through biophysical land suitability and production potentials, and through a matrix of potential land cover transitions (:numref:`fig-globiom_land`). 
