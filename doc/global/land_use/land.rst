.. _landuse:

Land use change
---------------
The model optimizes over six land cover types,
cropland, grassland, short rotation plantations, managed forests, unmanaged forests and other natural land.
Cropland is the area growing the 18 crops that GLOBIOM represents endogenously (see :ref:`crop`).
Grassland is the managed pasture used for grazing (see :ref:`livestock`).
Short rotation plantations are the areas under dedicated second generation biomass tree plantations.
Managed forests are all forest areas where harvesting operations take place,
while unmanaged forests are undisturbed secondary or primary forests (see :ref:`forestry`).
Other natural land covers the remaining natural vegetation.
Economic activities are associated with the first four land cover types.
There are other three land cover types represented in the model,
other agricultural land, which is mainly land for fruits and vegetables,
wetlands,
and not relevant areas, that is bare areas, water bodies, snow and ice, and artificial surfaces including urban areas.
These three categories close the land balance
but are currently kept constant at their initial levels and are not modelled.
Each Simulation Unit can contain the nine land cover types. The base year spatial distribution of land cover is based on the Global Land Cover 2000 (GLC2000). 
However, as any other global dataset of this type, GLC2000 suffers from large uncertainty (Fritz et al., 2011 :cite:`fritz_highlighting_2011`). Therefore auxiliary datasets and procedures are used to transform this “raw” data into a consistent dataset corresponding to the model needs.

.. _fig-globiom_land:

.. figure:: /_static/GLOBIOM_land_cover.png
   :width: 900px

   Land cover representation in GLOBIOM and the matrix of endogenous land cover change possibilities (Havlik et al., 2014 :cite:`havlik_climate_2014`).

Land conversion over the simulation period is endogenously determined for each Supply Unit within the available land resources,
and reflects the competition between the land-based activities the model represents.
Such conversion implies a conversion cost that increases with the area of land converted
and that is taken into account in the producer optimization behavior,
capturing the biophysical and economic limits on how fast land can be transformed.
Conversions also depend on the relative profitability of the competing activities.
Land conversion possibilities are further restricted through biophysical land suitability and production potentials,
and through a matrix of potential land cover transitions that governs which conversions are possible in each region
(:numref:`fig-globiom_land`).

Apart from the land-use change dynamics,
GLOBIOM also represents land-use management decisions and intensification processes endogenously.
Productivity and input costs are estimated at the grid cell level
for each crop type under each management system,
including for land that is not currently used as cropland.
Conversion of other land categories to cropland,
and switches between levels of management intensity,
can therefore be evaluated against the productivity and input costs expected
in the new location or under the new management system.
The same approach is applied to grassland, bioenergy plantations and managed forests.

