.. _globiom-spatial:

Spatial resolution
******************

Land resources and their characteristics are the fundamental elements of the GLOBIOM modelling approach. In order to enable global bio-physical process modelling of agricultural and forest production, a
comprehensive database has been built (Skalsky et al., 2008 :cite:`skalsky_geo-bene_2008`), which contains geo-spatial data on soil, climate/weather, topography, land cover/use, and crop management
(e.g. fertilization, irrigation). The data were compiled from various sources (FAO, ISRIC, USGS, NASA, CRU UEA, JRC, IFRPI, IFA, WISE, etc.) and significantly vary with respect to spatial, temporal,
and attribute resolutions, thematic relevance, accuracy, and reliability. Therefore, data were harmonized into several common spatial resolution layers including 5 and 30 Arcmin as well as country layers.
Subsequently, Homogeneous Response Units (HRU) have been delineated by geographically clustering according to only those parameters of the landscape, which are generally not changing over time and are thus
invariant with respect to land use and management or climate change. At the global scale, five altitude classes, seven slope classes, and five soil classes have been included.

In a second step, the HRU layer is intersected with a 0.5 x 0.5 degree grid and country boundaries to delineate Simulation Units (SimUs) which contain other relevant information such as global climate data,
land category/use data, irrigation data, etc. In total, 212,707 SimUs are delineated by clustering 5 x 5 minutes of arc pixels according to five criteria: altitude, slope, and soil class, 0.5 x 0.5
degrees grid, and the country boundaries. The SimUs are the basis for estimation of land use/management parameters in all other supporting models as well. For each SimU a number of land management
options are simulated using the bio-physical process model EPIC (Environmental Policy Integrated Climate) (Izaurralde et al., 2006 :cite:`izaurralde_simulating_2006`; Williams and Singh, 1995 :cite:`williams_computer_1995`).
Within each simulation unit,
alternative production systems are represented with distinct productivity levels and input requirements
(see :ref:`crop`, :ref:`livestock` and :ref:`forestry`),
and land allocation decisions are taken at the same unit.
This representation captures the spatial heterogeneity of environmental conditions and production potential in detail.
To increase computation efficiency,
the input data sets and the model resolution are aggregated to 2 x 2 degree cells,
disaggregated only by country boundaries
and by the three agro-ecological zones used in the livestock production system classification,
that is arid, humid, and temperate or tropical highlands.
This gives a total of 10,894 Supply Units.
Results are downscaled from these units back to 0.5 x 0.5 degree resolution through a downscaling module,
so that the gain in computation efficiency does not cost spatial detail in the output.
