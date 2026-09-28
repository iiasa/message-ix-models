.. _emission_land:

Emissions from land (GLOBIOM/G4M)
---------------------------------

Emissions from agriculture, forestry and other land use are simulated by GLOBIOM,
together with the forest model G4M for forests (see :ref:`globiom`).
The emissions enter |MESSAGEix| through the land-use emulator (see :ref:`emulator`).

Crop sector emissions
~~~~~~~~~~~~~~~~~~~~~

Crop emissions in GLOBIOM comprise CH4 from rice cultivation
and N2O from the application of synthetic and organic fertilizers.
Emission factors for synthetic fertilizers are derived from EPIC model outputs combined with IPCC default factors.
EPIC provides the fertilizer use for each management system at the level of the simulation units,
and the IPCC AFOLU guidelines provide the emission factors.
Synthetic fertilizer use is thus built bottom-up,
but is scaled up to the International Fertilizer Association statistics on total fertilizer use per crop at the national level
where the calculated use is too low at the aggregated level.
This correction ensures consistency with observed fertilizer purchases.
Emissions from organic fertilizers are linked to the livestock systems
and derived from outputs of the RUMINANT model.
Rice emissions follow a Tier 1 approach,
with emissions proportional to the area of rice cultivated
and an emission factor from EPA (2012) :cite:`environmental_protection_agency_epa_US_2012`.

Livestock emissions
~~~~~~~~~~~~~~~~~~~

The emission accounts assigned directly to livestock are
CH4 from enteric fermentation, CH4 and N2O from manure management,
and N2O from manure deposited on pastures.
N2O from manure applied to cropland is reported in a separate account linked to crop production.
CH4 from enteric fermentation is a simultaneous output of the feed and yield calculations of the RUMINANT model,
as are the nitrogen content of excreta and the amount of volatile solids.
Combined with literature-based parameters,
this differentiates emissions across species, production systems and feeding practices.
The assumptions on the proportions of manure management systems, manure uses and emission coefficients
are based on a detailed literature review,
described in Herrero et al. (2013) :cite:`herrero_global_2013`.

Burning and other sources
~~~~~~~~~~~~~~~~~~~~~~~~~

Emissions from crop residue burning and savannah burning
follow a land-use driver from GLOBIOM,
the cropland area for crop residue burning and the pasture area for savannah burning.
The implied emission factor per unit of land is held constant,
so that the emissions follow the land-use pathway of each scenario.
Peat fires are held at their last historical value,
and emissions from organic soils are not represented.
The historical burning emissions these drivers are applied to,
and how they are used in the emulator and in the GLOBIOM feedback runs,
are described with the land-use emulator (see :ref:`emulator`).

Land use change emissions
~~~~~~~~~~~~~~~~~~~~~~~~~

CO2 emissions and removals from land-use change and forestry
are modelled consistently with the IPCC accounting guidelines.
CO2 fluxes from changes in above- and below-ground biomass, litter and soil carbon in forests
are estimated endogenously by G4M
from grid-level land-use change and forest management decisions (see :ref:`forestry`).
They cover emissions from deforestation, removals from afforestation,
and the carbon dynamics of forest management.
Above- and below-ground living biomass carbon in forests is sourced from Kindermann et al. (2008) :cite:`kindermann_global_2008`,
which provides a geographically explicit allocation of the carbon stocks.
These carbon stocks are consistent with the 2010 Forest Resources Assessment
(FAO, 2010 :cite:`food_and_agricultural_organization_fao_global_2010`),
so that the emission factors for deforestation are in line with those of FAO.
Carbon stocks in grassland and other natural vegetation
are taken from the above- and below-ground biomass carbon map of Ruesch and Gibbs (2008) :cite:`ruesch_new_ipcc_2008`.
When forest or other natural vegetation is converted to agricultural use,
all above- and below-ground biomass carbon is assumed to be released to the atmosphere.
Overall, the model combines Tier 1, Tier 2 and Tier 3 methods,
with most sources represented at Tier 2 or Tier 3
using region-specific data and biophysical modelling.

Comparison with FAOSTAT
~~~~~~~~~~~~~~~~~~~~~~~

:numref:`tab-ag-emissions-globiom-fao` compares the agricultural emissions of GLOBIOM
with FAOSTAT (Tubiello et al., 2013 :cite:`tubiello_faostat_2013`),
which uses a simple and transparent approach
based largely on FAOSTAT activity data and IPCC Tier 1 emission coefficients.
The FAOSTAT categories are aggregated to the categories GLOBIOM reports.
In 2000,
GLOBIOM emissions from rice cultivation, enteric fermentation and manure are close to FAOSTAT,
while emissions from managed soils are higher.
Between 2000 and 2010,
GLOBIOM emissions grow somewhat more slowly than FAOSTAT,
mainly because of managed soils,
so that total agricultural emissions in 2010 are slightly below FAOSTAT.

.. _tab-ag-emissions-globiom-fao:
.. list-table:: Agricultural GHG emissions from GLOBIOM and from FAOSTAT (Tubiello et al., 2013 :cite:`tubiello_faostat_2013`) for 2000 and 2010, in Mt CO2-equiv/yr, converted with IPCC Second Assessment Report 100-year GWPs. The change ratio divides the relative change from 2000 to 2010 in GLOBIOM by that in FAOSTAT, so that 1.00 means both change at the same rate. Managed soils cover synthetic fertilizer, manure applied to soils and crop residues, manure covers manure management and manure left on pasture.
   :widths: 30 14 14 14 14 14
   :header-rows: 1

   * - Source
     - GLOBIOM 2000
     - GLOBIOM 2010
     - FAOSTAT 2000
     - FAOSTAT 2010
     - Change ratio
   * - Rice cultivation (CH4)
     - 484
     - 489
     - 490
     - 499
     - 0.99
   * - Managed soils (N2O)
     - 862
     - 984
     - 753
     - 950
     - 0.90
   * - Enteric fermentation (CH4)
     - 1,849
     - 1,928
     - 1,863
     - 2,018
     - 0.96
   * - Manure (CH4 and N2O)
     - 1,026
     - 1,084
     - 1,030
     - 1,117
     - 0.97
   * - Total agriculture
     - 4,221
     - 4,485
     - 4,136
     - 4,586
     - 0.96
