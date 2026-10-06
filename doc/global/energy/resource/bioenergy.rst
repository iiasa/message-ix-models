Biomass Resources
======================
Biomass energy is another potentially important renewable energy resource in the |MESSAGEix| model. This includes both commercial and non-commercial use. 
Commercial refers to the use of bioenergy in, for example, power plants or biofuel refineries, while non-commercial refers to the use of bioenergy for 
residential heating and cooking, primarily in rural households of today’s developing countries.
Bioenergy potentials are derived from the GLOBIOM model
and enter |MESSAGEix| through the land-use emulator (see :ref:`emulator`).
For the emulator, GLOBIOM is run at seven biomass price levels, ranging up to 68 $2005/GJ,
chosen so that the range covers the full biomass potential in every region.
Each run yields a complete land-use pathway,
with land cover, agricultural and forestry production and land-use emissions
alongside the biomass available to the energy system at that price,
and the emulator carries all of these into |MESSAGEix|.
The biomass component of these pathways gives the regional biomass supply curves for each narrative,
shown at the global aggregate level for the year 2100 in :numref:`fig-beavail` (a)
across the five SSP narratives and the LED variant.
SSP2 and SSP3 have the largest biomass potential, around 305 EJ/yr,
while the remaining narratives are limited globally to between 214 EJ/yr and 227 EJ/yr in 2100.
The differences result from different levels of competition over land for food, fibre and energy.
In part they are driven by assumptions about future forest-related developments,
that is the degree of assumed afforestation and deforestation,
by dietary trends and the associated land requirements for food provision,
and by environmental concerns.
The drivers underlying this competition are the land-use developments of the individual narratives,
which are determined by agricultural productivity and global food demand
(Fricko et al., 2026 :cite:`fricko_wu_2026`)
and are described in the land-use section (see :ref:`globiom`).

Non-commercial biomass, that is fuel wood, is typically not traded or sold,
and in |MESSAGEix| it is represented as a demand category rather than as part of this resource potential
(see :ref:`demand`).
Where a market exists, prices range from 0.1 to 1.5 $/GJ
(Pachauri et al., 2013 :cite:`pachauri_pathways_2013`) ($ equals 2005 USD).

.. _fig-beavail:
.. figure:: /_static/biomass_resource_cost_curves.png
   :width: 750px

   Biomass resource cost curve and source composition.
   Biomass resource cost-availability curve by narrative (LED and SSP1 to SSP5) for 2100 (a)
   and biomass potential in 2100 by narrative for the Global North and South, by source (b).
   Panel (a) plots cumulative biomass potential (EJ/yr) against price ($2005/GJ),
   one curve per narrative.
   Panel (b) shows stacked bars of biomass potential (EJ/yr) grouped by narrative,
   split into Global North and South within each group and coloured by source
   (Fricko et al., 2026 :cite:`fricko_wu_2026`).

SSP2 and SSP3 offer the greatest quantities of biomass for energy sector usage by the end of the century,
with the majority of the potential located in the Global South (:numref:`fig-beavail` (b)).
In SSP3, higher fuelwood requirements, that is biomass used for cooking and heating,
follow from low per capita incomes,
which slow the transition away from traditional fuels and raise deforestation rates, especially in the South.
This accounts for many of the differences in where biomass is sourced.
SSP3 also uses the most land for dedicated fast rotation bioenergy crops,
yet produces less biomass from them than SSP2,
reflecting the lower yield rates assumed in the narrative.
The remaining narratives rely to a large degree, in relative terms, on high yield fast rotation energy crops.
They harvest less roundwood from forests,
so natural forests stay more intact and require less re- and afforestation,
which is also reflected in the larger total land area under protection (:numref:`fig-landcover`).

.. _fig-landcover:
.. figure:: /_static/land_cover_by_narrative.png
   :width: 750px

   Land cover by narrative, Global North and South, by type.
   Stacked land cover in 2100 (Mha), with bars grouped by narrative (LED and SSP1 to SSP5)
   and split into Global North and South within each group.
   Colours denote land type and the hatched overlay marks the total protected area
   (Fricko et al., 2026 :cite:`fricko_wu_2026`).
